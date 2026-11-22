package dev.manhattan.fleet.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.manhattan.fleet.server.FleetModels.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.LongSupplier;

/**
 * Pure-Java fleet coordinator managing concurrent jobs, assignment policies, chunk leases,
 * 60-second rejoin grace, 1-chunk spatial separation, bed-pool assignments, and snapshots.
 */
public final class FleetManager {
    private final FleetConfig config;
    private final LongSupplier clockMs;
    private final SleepCoordinator sleepCoordinator;
    private final ChestCoordinator chestCoordinator;

    private final Map<String, FleetJob> jobs = new LinkedHashMap<>();
    private final Map<String, BotRecord> bots = new LinkedHashMap<>();
    private final Map<String, Map<String, TaskMeta>> taskMetaCache = new HashMap<>();
    private int jobCounter = 1;
    private boolean dirty = false;

    private record TaskMeta(String id, String kind, Boolean waitForPrevious) {}

    public FleetManager(FleetConfig config, LongSupplier clockMs) {
        this.config = Objects.requireNonNull(config, "config");
        this.config.ensureInitialized();
        this.clockMs = Objects.requireNonNull(clockMs, "clockMs");
        this.sleepCoordinator = new SleepCoordinator(config.sleepTimeoutSeconds * 1000L);
        this.chestCoordinator = new ChestCoordinator();
    }

    public SleepCoordinator sleepCoordinator() {
        return sleepCoordinator;
    }

    public ChestCoordinator chestCoordinator() {
        return chestCoordinator;
    }

    public boolean consumeDirty() {
        boolean was = dirty;
        dirty = false;
        return was;
    }

    public Map<String, FleetJob> jobsView() {
        return Collections.unmodifiableMap(jobs);
    }

    public Collection<BotRecord> botsView() {
        return Collections.unmodifiableCollection(bots.values());
    }

    public void loadPersistedJobs(List<FleetJob> persisted) {
        jobs.clear();
        int maxNum = 0;
        if (persisted != null) {
            for (FleetJob job : persisted) {
                if (job == null || job.jobId == null || job.jobId.isBlank()) continue;
                job.ensureInitialized();
                // On server restart, convert any stale CLAIMED leases into DISCONNECTED_GRACE so
                // auto-reconnecting bots can still inherit their chunk during the grace window.
                long now = clockMs.getAsLong();
                long graceMs = config.rejoinGraceSeconds * 1000L;
                for (ChunkEntry ce : job.chunks.values()) {
                    if (ce.state == ChunkState.CLAIMED) {
                        if (graceMs > 0 && ce.ownerUuid != null) {
                            ce.state = ChunkState.DISCONNECTED_GRACE;
                            ce.graceExpiresAtMs = now + graceMs;
                        } else {
                            clearChunkOwner(ce, ChunkState.PENDING);
                        }
                    } else if (ce.state == ChunkState.DEFERRED && ce.tries >= config.maxChunkRetries) {
                        clearChunkOwner(ce, ChunkState.FAILED);
                        if (ce.reason == null || ce.reason.isBlank()) {
                            ce.reason = "max retries " + config.maxChunkRetries;
                        }
                        dirty = true;
                    }
                }
                checkJobCompletion(job);
                jobs.put(job.jobId, job);
                if (job.jobId.startsWith("job_")) {
                    try {
                        maxNum = Math.max(maxNum, Integer.parseInt(job.jobId.substring(4)));
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }
        jobCounter = Math.max(jobCounter, maxNum + 1);
    }

    // ------------------------------------------------------------------ Bot Lifecycle

    public BotRecord registerBot(String uuid, String name, ClientMode mode, List<String> tags, int[] localBed,
                                 String dimension, int x, int y, int z) {
        long now = clockMs.getAsLong();
        BotRecord rec = bots.computeIfAbsent(uuid, k -> {
            BotRecord b = new BotRecord();
            b.uuid = uuid;
            return b;
        });
        rec.name = name;
        rec.mode = mode == null ? ClientMode.LOCAL : mode;
        rec.tags = tags != null ? new ArrayList<>(tags) : new ArrayList<>();
        rec.online = true;
        rec.dimension = dimension != null ? dimension : "minecraft:overworld";
        rec.x = x;
        rec.y = y;
        rec.z = z;
        rec.localBed = (localBed != null && localBed.length == 3) ? localBed.clone() : null;
        rec.lastHeartbeatMs = now;
        rec.disconnectedAtMs = 0L;
        return rec;
    }

    public SleepCoordinator.SleepOrder disconnectBot(String uuid) {
        long now = clockMs.getAsLong();
        BotRecord bot = bots.get(uuid);
        if (bot != null) {
            bot.online = false;
            bot.disconnectedAtMs = now;
            bot.phase = "OFFLINE";
        }

        // Put any active CLAIMED chunk into DISCONNECTED_GRACE so a rejoining bot inherits it.
        long graceMs = config.rejoinGraceSeconds * 1000L;
        for (FleetJob job : jobs.values()) {
            for (ChunkEntry ce : job.chunks.values()) {
                if (ce.state == ChunkState.CLAIMED && uuid.equals(ce.ownerUuid)) {
                    if (graceMs > 0) {
                        ce.state = ChunkState.DISCONNECTED_GRACE;
                        ce.graceExpiresAtMs = now + graceMs;
                    } else {
                        clearChunkOwner(ce, ChunkState.PENDING);
                    }
                    dirty = true;
                }
            }
        }

        chestCoordinator.releaseAllForBot(uuid);
        return sleepCoordinator.onBotUnavailable(uuid, now, bots.values());
    }

    public SleepCoordinator.SleepOrder onBotDeath(String uuid) {
        long now = clockMs.getAsLong();
        BotRecord bot = bots.get(uuid);
        if (bot != null) {
            bot.inDanger = true;
            bot.activeChunkKey = null;
            bot.phase = "DEAD";
            bot.alertSummary = "Died; leases released";
        }
        for (FleetJob job : jobs.values()) {
            for (ChunkEntry ce : job.chunks.values()) {
                if ((ce.state == ChunkState.CLAIMED || ce.state == ChunkState.DISCONNECTED_GRACE)
                        && uuid.equals(ce.ownerUuid)) {
                    clearChunkOwner(ce, ChunkState.PENDING);
                    dirty = true;
                }
            }
        }
        chestCoordinator.releaseAllForBot(uuid);
        return sleepCoordinator.onBotUnavailable(uuid, now, bots.values());
    }

    public void updateHeartbeat(String uuid, String dimension, int x, int y, int z,
                                double health, int food, int freeSlots, boolean inDanger,
                                String activeJobId, String activeChunkKey, String phase,
                                String alertSummary, int[] localBed) {
        long now = clockMs.getAsLong();
        BotRecord bot = bots.get(uuid);
        if (bot == null) return;
        bot.online = true;
        if (dimension != null && !dimension.isBlank()) bot.dimension = dimension;
        bot.x = x;
        bot.y = y;
        bot.z = z;
        bot.health = health;
        bot.food = food;
        bot.freeSlots = freeSlots;
        bot.inDanger = inDanger;
        if (phase != null) bot.phase = phase;
        if (alertSummary != null) bot.alertSummary = alertSummary;
        if (localBed != null && localBed.length == 3) bot.localBed = localBed.clone();
        bot.lastHeartbeatMs = now;

        // Renew active lease if matching.
        if (activeJobId != null && activeChunkKey != null) {
            FleetJob job = jobs.get(activeJobId);
            if (job != null) {
                ChunkEntry ce = job.chunks.get(activeChunkKey);
                if (ce != null && ce.state == ChunkState.CLAIMED && uuid.equals(ce.ownerUuid)) {
                    ce.leaseExpiresAtMs = now + config.leaseTtlSeconds * 1000L;
                }
            }
        }
    }

    // ------------------------------------------------------------------ Job Management

    public FleetJob submitJob(String projectId, String projectName, String projectJson, String dimension,
                              AssignmentPolicy assignment, List<ChunkSpec> chunks, List<int[]> bedPool,
                              String createdBy) {
        long now = clockMs.getAsLong();
        FleetJob job = new FleetJob();
        job.jobId = String.format(Locale.ROOT, "job_%03d", jobCounter++);
        job.projectId = projectId != null ? projectId : job.jobId;
        job.projectName = (projectName != null && !projectName.isBlank()) ? projectName : job.projectId;
        job.projectJson = projectJson != null ? projectJson : "{}";
        job.projectHash = sha256Short(job.projectJson);
        job.dimension = (dimension != null && !dimension.isBlank()) ? dimension : "minecraft:overworld";
        job.status = JobStatus.ACTIVE;
        job.assignment = assignment != null ? assignment : new AssignmentPolicy();
        job.assignment.ensureInitialized();
        job.createdBy = createdBy != null ? createdBy : "";
        job.createdAtMs = now;
        if (bedPool != null) {
            for (int[] b : bedPool) {
                if (b != null && b.length == 3) {
                    job.bedPool.add(new int[]{b[0], b[1], b[2]});
                }
            }
        }
        if (chunks != null) {
            for (ChunkSpec spec : chunks) {
                if (spec == null || spec.chunkKey == null || spec.chunkKey.isBlank()) continue;
                job.chunks.put(spec.chunkKey, new ChunkEntry(spec));
            }
        }
        if (job.chunks.isEmpty()) {
            job.status = JobStatus.DONE;
        }
        jobs.put(job.jobId, job);
        dirty = true;
        return job;
    }

    public boolean pauseJob(String jobId) {
        FleetJob job = jobs.get(jobId);
        if (job == null || job.status != JobStatus.ACTIVE) return false;
        job.status = JobStatus.PAUSED;
        dirty = true;
        return true;
    }

    public boolean resumeJob(String jobId) {
        FleetJob job = jobs.get(jobId);
        if (job == null || job.status != JobStatus.PAUSED) return false;
        job.status = JobStatus.ACTIVE;
        dirty = true;
        return true;
    }

    public boolean cancelJob(String jobId) {
        FleetJob job = jobs.get(jobId);
        if (job == null || job.status == JobStatus.CANCELLED) return false;
        job.status = JobStatus.CANCELLED;
        for (ChunkEntry ce : job.chunks.values()) {
            if (ce.state == ChunkState.CLAIMED || ce.state == ChunkState.DISCONNECTED_GRACE) {
                clearChunkOwner(ce, ChunkState.PENDING);
            }
        }
        for (BotRecord bot : bots.values()) {
            if (jobId.equals(bot.activeJobId)) {
                bot.activeJobId = null;
                bot.activeChunkKey = null;
            }
        }
        dirty = true;
        return true;
    }

    public boolean updateAssignment(String jobId, AssignmentPolicy policy) {
        FleetJob job = jobs.get(jobId);
        if (job == null || policy == null) return false;
        policy.ensureInitialized();
        job.assignment = policy;
        dirty = true;
        return true;
    }

    // ------------------------------------------------------------------ Chunk Claiming & Reporting

    public WorkGrant claimWork(String botUuid, String dimension, int x, int y, int z, String knownProjectHash) {
        long now = clockMs.getAsLong();
        tick(now);

        BotRecord bot = bots.get(botUuid);
        if (bot == null || !bot.online) return WorkGrant.idle();
        if (bot.mode != ClientMode.WORKER && bot.mode != ClientMode.BOTH) return WorkGrant.idle();

        bot.dimension = dimension != null ? dimension : bot.dimension;
        bot.x = x;
        bot.y = y;
        bot.z = z;
        bot.lastHeartbeatMs = now;

        // 1. Rejoin grace inheritance: if this bot owns an unexpired DISCONNECTED_GRACE chunk on an ACTIVE job
        //    in the same dimension, hand back the exact same chunk immediately.
        for (FleetJob job : jobs.values()) {
            if (job.status != JobStatus.ACTIVE) continue;
            if (!job.dimension.equals(bot.dimension)) continue;
            for (ChunkEntry ce : job.chunks.values()) {
                if ((ce.state == ChunkState.DISCONNECTED_GRACE || ce.state == ChunkState.CLAIMED)
                        && botUuid.equals(ce.ownerUuid)) {
                    boolean wasGrace = ce.state == ChunkState.DISCONNECTED_GRACE;
                    return grantChunk(bot, job, ce, knownProjectHash, now, wasGrace);
                }
            }
        }

        // 2. Build ordered list of active jobs in this dimension: sticky job first, then by priority desc / createdAt asc.
        List<FleetJob> candidates = new ArrayList<>();
        for (FleetJob job : jobs.values()) {
            if (job.status == JobStatus.ACTIVE && job.dimension.equals(bot.dimension)) {
                candidates.add(job);
            }
        }
        String stickyJobId = bot.activeJobId;
        candidates.sort((a, b) -> {
            boolean aSticky = a.jobId.equals(stickyJobId);
            boolean bSticky = b.jobId.equals(stickyJobId);
            if (aSticky != bSticky) return aSticky ? -1 : 1;
            int cmp = Integer.compare(b.assignment.priority, a.assignment.priority);
            if (cmp != 0) return cmp;
            return Long.compare(a.createdAtMs, b.createdAtMs);
        });

        for (FleetJob job : candidates) {
            if (!isBotEligibleForJob(bot, job)) continue;
            ChunkEntry chosen = pickChunkForBot(bot, job, now);
            if (chosen != null) {
                return grantChunk(bot, job, chosen, knownProjectHash, now, false);
            }
        }

        bot.activeChunkKey = null;
        bot.phase = "IDLE";
        return WorkGrant.idle();
    }

    public boolean reportChunk(String botUuid, String jobId, String chunkKey, ChunkState outcome, String reason) {
        FleetJob job = jobs.get(jobId);
        if (job == null) return false;
        ChunkEntry ce = job.chunks.get(chunkKey);
        if (ce == null) return false;
        if (ce.ownerUuid != null && !ce.ownerUuid.equals(botUuid)) return false;

        ce.tries++;
        ce.reason = reason;
        if (outcome == ChunkState.DONE || outcome == ChunkState.FAILED) {
            clearChunkOwner(ce, outcome);
        } else if (ce.tries >= config.maxChunkRetries) {
            clearChunkOwner(ce, ChunkState.FAILED);
            ce.reason = (reason != null && !reason.isBlank())
                    ? reason + " (max retries " + config.maxChunkRetries + ")"
                    : "max retries " + config.maxChunkRetries;
        } else {
            clearChunkOwner(ce, ChunkState.DEFERRED);
            ce.nextRetryAtMs = clockMs.getAsLong() + config.deferCooldownSeconds * 1000L;
        }

        BotRecord bot = bots.get(botUuid);
        if (bot != null && chunkKey.equals(bot.activeChunkKey)) {
            bot.activeChunkKey = null;
        }

        checkJobCompletion(job);
        dirty = true;
        return true;
    }

    // ------------------------------------------------------------------ Maintenance Tick

    public void tick(long nowMs) {
        chestCoordinator.expireLocks(nowMs);

        for (FleetJob job : jobs.values()) {
            if (job.status != JobStatus.ACTIVE) continue;
            for (ChunkEntry ce : job.chunks.values()) {
                if (ce.state == ChunkState.CLAIMED && ce.leaseExpiresAtMs > 0 && nowMs >= ce.leaseExpiresAtMs) {
                    clearChunkOwner(ce, ChunkState.PENDING);
                    dirty = true;
                } else if (ce.state == ChunkState.DISCONNECTED_GRACE && ce.graceExpiresAtMs > 0 && nowMs >= ce.graceExpiresAtMs) {
                    clearChunkOwner(ce, ChunkState.PENDING);
                    dirty = true;
                }
            }
            checkJobCompletion(job);
        }
    }

    public FleetSnapshot snapshot() {
        FleetSnapshot snap = new FleetSnapshot();
        snap.serverTimeMs = clockMs.getAsLong();
        snap.designatedSleeperUuid = sleepCoordinator.designatedSleeperUuid();
        snap.designatedSleeperName = sleepCoordinator.designatedSleeperName();
        snap.bots.addAll(bots.values());

        for (FleetJob job : jobs.values()) {
            JobSummary js = new JobSummary();
            js.jobId = job.jobId;
            js.projectId = job.projectId;
            js.projectName = job.projectName;
            js.dimension = job.dimension;
            js.status = job.status;
            js.assignment = job.assignment;
            js.totalChunks = job.chunks.size();

            Set<String> workerNames = new LinkedHashSet<>();
            for (ChunkEntry ce : job.chunks.values()) {
                switch (ce.state) {
                    case DONE -> js.doneChunks++;
                    case FAILED -> js.failedChunks++;
                    case CLAIMED, DISCONNECTED_GRACE -> {
                        js.claimedChunks++;
                        if (ce.ownerName != null) workerNames.add(ce.ownerName);
                        PeerClaim pc = new PeerClaim();
                        pc.jobId = job.jobId;
                        pc.chunkKey = ce.chunkKey;
                        pc.cx = ce.cx;
                        pc.cz = ce.cz;
                        pc.centerX = ce.centerX;
                        pc.centerZ = ce.centerZ;
                        pc.ownerName = ce.ownerName;
                        pc.state = ce.state;
                        snap.activeClaims.add(pc);
                    }
                    default -> {}
                }
            }
            js.activeWorkers.addAll(workerNames);
            snap.jobs.add(js);
        }
        return snap;
    }

    // ------------------------------------------------------------------ Internals

    private boolean isBotEligibleForJob(BotRecord bot, FleetJob job) {
        AssignmentPolicy p = job.assignment;
        if (p.mode == AssignmentMode.EXPLICIT) {
            for (String target : p.explicitBots) {
                if (target == null) continue;
                if (target.equalsIgnoreCase(bot.name) || target.equalsIgnoreCase(bot.uuid)) {
                    return true;
                }
            }
            return false;
        }

        // AUTO_POOL mode
        if (p.poolTag != null && !p.poolTag.isBlank()) {
            boolean hasTag = false;
            for (String tag : bot.tags) {
                if (p.poolTag.equalsIgnoreCase(tag)) {
                    hasTag = true;
                    break;
                }
            }
            if (!hasTag) return false;
        }

        // Enforce maxBots (count distinct active/grace owners excluding this bot).
        Set<String> activeOwners = new HashSet<>();
        for (ChunkEntry ce : job.chunks.values()) {
            if ((ce.state == ChunkState.CLAIMED || ce.state == ChunkState.DISCONNECTED_GRACE)
                    && ce.ownerUuid != null && !ce.ownerUuid.equals(bot.uuid)) {
                activeOwners.add(ce.ownerUuid);
            }
        }
        return activeOwners.size() < p.maxBots;
    }

    private ChunkEntry pickChunkForBot(BotRecord bot, FleetJob job, long nowMs) {
        List<String> eligibleTasks = eligibleTaskIds(job);
        for (String taskId : eligibleTasks) {
            ChunkEntry picked = pickChunkInTask(bot, job, taskId, nowMs);
            if (picked != null) return picked;
        }
        return null;
    }

    /**
     * Returns ordered unfinished taskIds eligible for leasing right now:
     * always includes the earliest unfinished task, plus subsequent unfinished tasks until a barrier is hit
     * ({@code waitForPrevious == true}, or {@code waitForPrevious == null} when {@code kind} changes).
     */
    private List<String> eligibleTaskIds(FleetJob job) {
        List<String> unfinished = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (ChunkEntry ce : job.chunks.values()) {
            if (ce.state != ChunkState.DONE && ce.state != ChunkState.FAILED) {
                String tid = taskIdOf(ce.chunkKey);
                if (seen.add(tid)) unfinished.add(tid);
            }
        }
        if (unfinished.isEmpty()) return List.of();

        List<String> eligible = new ArrayList<>();
        String firstId = unfinished.get(0);
        eligible.add(firstId);
        if (unfinished.size() == 1) return eligible;

        Map<String, TaskMeta> metaById = taskMetaFor(job);
        if (metaById.isEmpty()) return eligible;
        TaskMeta firstMeta = metaById.get(firstId);
        String activeKind = firstMeta != null ? firstMeta.kind() : null;

        for (int i = 1; i < unfinished.size(); i++) {
            String tid = unfinished.get(i);
            TaskMeta m = metaById.get(tid);
            if (m == null) break;
            if (Boolean.TRUE.equals(m.waitForPrevious())) break;
            if (m.waitForPrevious() == null) {
                if (activeKind == null || m.kind() == null || !activeKind.equalsIgnoreCase(m.kind())) {
                    break;
                }
            }
            eligible.add(tid);
        }
        return eligible;
    }

    private Map<String, TaskMeta> taskMetaFor(FleetJob job) {
        if (job == null || job.projectJson == null || job.projectJson.isBlank()) return Map.of();
        String cacheKey = (job.projectHash != null && !job.projectHash.isBlank()) ? job.projectHash : job.jobId;
        Map<String, TaskMeta> cached = taskMetaCache.get(cacheKey);
        if (cached != null) return cached;
        Map<String, TaskMeta> parsed = new LinkedHashMap<>();
        try {
            JsonObject root = JsonParser.parseString(job.projectJson).getAsJsonObject();
            if (root.has("tasks") && root.get("tasks").isJsonArray()) {
                JsonArray arr = root.getAsJsonArray("tasks");
                for (JsonElement el : arr) {
                    if (!el.isJsonObject()) continue;
                    JsonObject obj = el.getAsJsonObject();
                    String id = obj.has("id") && !obj.get("id").isJsonNull() ? obj.get("id").getAsString() : null;
                    if (id == null || id.isBlank()) continue;
                    String kind = obj.has("kind") && !obj.get("kind").isJsonNull() ? obj.get("kind").getAsString() : null;
                    Boolean waitPrev = null;
                    if (obj.has("waitForPrevious") && !obj.get("waitForPrevious").isJsonNull()) {
                        waitPrev = obj.get("waitForPrevious").getAsBoolean();
                    }
                    parsed.put(id, new TaskMeta(id, kind, waitPrev));
                }
            }
        } catch (Exception ignored) {
        }
        taskMetaCache.put(cacheKey, parsed);
        return parsed;
    }

    private ChunkEntry pickChunkInTask(BotRecord bot, FleetJob job, String activeTaskId, long nowMs) {
        int maxDist = job.assignment.mode == AssignmentMode.AUTO_POOL ? job.assignment.maxDistance : 0;
        double maxDistSq = maxDist > 0 ? (double) maxDist * maxDist : Double.MAX_VALUE;

        // 2. Global top-down Y-band gating with 1-layer lookahead:
        //    Clear the highest unfinished Y-band (topBandY) across the entire task first.
        //    If no claimable chunks remain on topBandY (e.g. peers hold the last chunk(s)), allow an idle
        //    bot to start at most 1 layer down (nextBandY) on columns whose topBandY chunk is already finished.
        Integer topBandY = null;
        Integer nextBandY = null;
        Map<String, Integer> highestUnfinishedByCol = new HashMap<>();
        for (ChunkEntry ce : job.chunks.values()) {
            if (ce.state == ChunkState.DONE || ce.state == ChunkState.FAILED) continue;
            if (!activeTaskId.equals(taskIdOf(ce.chunkKey))) continue;
            Integer y = parseBandY(ce.chunkKey);
            if (y != null) {
                highestUnfinishedByCol.merge(columnKeyOf(ce.chunkKey), y, Math::max);
                if (topBandY == null || y > topBandY) {
                    if (topBandY != null) nextBandY = topBandY;
                    topBandY = y;
                } else if (y < topBandY && (nextBandY == null || y > nextBandY)) {
                    nextBandY = y;
                }
            }
        }

        List<ChunkEntry> activeOthers = new ArrayList<>();
        List<ChunkEntry> availablePending = new ArrayList<>();
        List<ChunkEntry> availableDeferred = new ArrayList<>();
        List<ChunkEntry> gracePreemptable = new ArrayList<>();
        List<ChunkEntry> lookaheadPending = new ArrayList<>();
        List<ChunkEntry> lookaheadDeferred = new ArrayList<>();

        for (ChunkEntry ce : job.chunks.values()) {
            if (!activeTaskId.equals(taskIdOf(ce.chunkKey))) continue;
            if (ce.state == ChunkState.CLAIMED && !bot.uuid.equals(ce.ownerUuid)) {
                activeOthers.add(ce);
            } else if (ce.state == ChunkState.PENDING) {
                if (distSq2D(bot.x, bot.z, ce.centerX, ce.centerZ) > maxDistSq) continue;
                if (isTopUnfinishedBand(ce.chunkKey, topBandY)) {
                    availablePending.add(ce);
                } else if (isEligibleLookaheadBand(ce.chunkKey, nextBandY, highestUnfinishedByCol)) {
                    lookaheadPending.add(ce);
                }
            } else if (ce.state == ChunkState.DEFERRED) {
                if (ce.tries >= config.maxChunkRetries || nowMs < ce.nextRetryAtMs) continue;
                if (distSq2D(bot.x, bot.z, ce.centerX, ce.centerZ) > maxDistSq) continue;
                if (isTopUnfinishedBand(ce.chunkKey, topBandY)) {
                    availableDeferred.add(ce);
                } else if (isEligibleLookaheadBand(ce.chunkKey, nextBandY, highestUnfinishedByCol)) {
                    lookaheadDeferred.add(ce);
                }
            } else if (ce.state == ChunkState.DISCONNECTED_GRACE && !bot.uuid.equals(ce.ownerUuid)) {
                if (!isTopUnfinishedBand(ce.chunkKey, topBandY)) continue;
                if (distSq2D(bot.x, bot.z, ce.centerX, ce.centerZ) <= maxDistSq) {
                    gracePreemptable.add(ce);
                }
            }
        }

        int buffer = config.spatialBufferChunks;

        // Pass 1 & 2: Fresh PENDING chunks on topBandY always take priority.
        ChunkEntry picked = pickNearest(bot, availablePending, activeOthers, buffer);
        if (picked != null) return picked;

        // Pass 3 & 4: Retry DEFERRED chunks on topBandY.
        picked = pickNearest(bot, availableDeferred, activeOthers, buffer);
        if (picked != null) return picked;

        // Pass 5: Preempt a DISCONNECTED_GRACE chunk on topBandY before looking ahead.
        ChunkEntry bestGrace = null;
        double bestGraceDist = Double.MAX_VALUE;
        for (ChunkEntry ce : gracePreemptable) {
            double d = distSq2D(bot.x, bot.z, ce.centerX, ce.centerZ);
            if (d < bestGraceDist) {
                bestGraceDist = d;
                bestGrace = ce;
            }
        }
        if (bestGrace != null) return bestGrace;

        // Pass 6 & 7: 1-layer lookahead on nextBandY when topBandY has no claimable chunks left.
        picked = pickNearest(bot, lookaheadPending, activeOthers, buffer);
        if (picked != null) return picked;
        return pickNearest(bot, lookaheadDeferred, activeOthers, buffer);
    }

    private static ChunkEntry pickNearest(BotRecord bot, List<ChunkEntry> candidates,
                                          List<ChunkEntry> activeOthers, int buffer) {
        if (candidates.isEmpty()) return null;
        if (buffer > 0 && !activeOthers.isEmpty()) {
            ChunkEntry bestBuffered = null;
            double bestBufferedDist = Double.MAX_VALUE;
            for (ChunkEntry ce : candidates) {
                if (isWithinSpatialBuffer(ce, activeOthers, buffer)) continue;
                double d = distSq2D(bot.x, bot.z, ce.centerX, ce.centerZ);
                if (d < bestBufferedDist) {
                    bestBufferedDist = d;
                    bestBuffered = ce;
                }
            }
            if (bestBuffered != null) return bestBuffered;
        }
        ChunkEntry bestAny = null;
        double bestAnyDist = Double.MAX_VALUE;
        for (ChunkEntry ce : candidates) {
            double d = distSq2D(bot.x, bot.z, ce.centerX, ce.centerZ);
            if (d < bestAnyDist) {
                bestAnyDist = d;
                bestAny = ce;
            }
        }
        return bestAny;
    }

    private WorkGrant grantChunk(BotRecord bot, FleetJob job, ChunkEntry ce, String knownProjectHash,
                                 long nowMs, boolean inheritedFromGrace) {
        ce.state = ChunkState.CLAIMED;
        ce.ownerUuid = bot.uuid;
        ce.ownerName = bot.name;
        ce.leaseExpiresAtMs = nowMs + config.leaseTtlSeconds * 1000L;
        ce.graceExpiresAtMs = 0L;
        ce.nextRetryAtMs = 0L;

        bot.activeJobId = job.jobId;
        bot.activeChunkKey = ce.chunkKey;
        bot.phase = "WORKING";

        int[] bed = assignBedIfNeeded(bot, job);

        WorkGrant grant = new WorkGrant();
        grant.idle = false;
        grant.jobId = job.jobId;
        grant.projectId = job.projectId;
        grant.projectHash = job.projectHash;
        grant.projectJson = !Objects.equals(knownProjectHash, job.projectHash) ? job.projectJson : null;
        grant.chunkKey = ce.chunkKey;
        grant.cx = ce.cx;
        grant.cz = ce.cz;
        grant.assignedBed = bed;
        grant.leaseExpiresAtMs = ce.leaseExpiresAtMs;
        grant.inheritedFromGrace = inheritedFromGrace;

        dirty = true;
        return grant;
    }

    private int[] assignBedIfNeeded(BotRecord bot, FleetJob job) {
        if (bot.localBed != null && bot.localBed.length == 3) {
            return bot.localBed.clone();
        }
        int[] existing = job.botBeds.get(bot.uuid);
        if (existing != null && existing.length == 3) {
            bot.assignedBed = existing.clone();
            return existing.clone();
        }
        if (job.bedPool == null || job.bedPool.isEmpty()) {
            return null;
        }
        for (int[] candidate : job.bedPool) {
            if (candidate == null || candidate.length != 3) continue;
            boolean taken = false;
            for (Map.Entry<String, int[]> e : job.botBeds.entrySet()) {
                if (!e.getKey().equals(bot.uuid) && Arrays.equals(e.getValue(), candidate)) {
                    taken = true;
                    break;
                }
            }
            if (!taken) {
                int[] copy = candidate.clone();
                job.botBeds.put(bot.uuid, copy);
                bot.assignedBed = copy.clone();
                return copy;
            }
        }
        return null;
    }

    private void checkJobCompletion(FleetJob job) {
        if (job.status != JobStatus.ACTIVE) return;
        for (ChunkEntry ce : job.chunks.values()) {
            if (ce.state != ChunkState.DONE && ce.state != ChunkState.FAILED) {
                return;
            }
        }
        job.status = JobStatus.DONE;
        dirty = true;
    }

    private static String taskIdOf(String chunkKey) {
        if (chunkKey == null) return "";
        int slash = chunkKey.indexOf('/');
        return slash >= 0 ? chunkKey.substring(0, slash) : "";
    }

    private static Integer parseBandY(String chunkKey) {
        if (chunkKey == null) return null;
        int slash = chunkKey.indexOf('/');
        String coords = slash >= 0 ? chunkKey.substring(slash + 1) : chunkKey;
        int c1 = coords.indexOf(',');
        if (c1 < 0) return null;
        int c2 = coords.indexOf(',', c1 + 1);
        if (c2 < 0) return null;
        try {
            return Integer.parseInt(coords.substring(c2 + 1).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String columnKeyOf(String chunkKey) {
        if (chunkKey == null) return "";
        int slash = chunkKey.indexOf('/');
        String prefix = slash >= 0 ? chunkKey.substring(0, slash + 1) : "";
        String coords = slash >= 0 ? chunkKey.substring(slash + 1) : chunkKey;
        int c1 = coords.indexOf(',');
        if (c1 < 0) return chunkKey;
        int c2 = coords.indexOf(',', c1 + 1);
        return c2 < 0 ? chunkKey : prefix + coords.substring(0, c2);
    }

    private static boolean isTopUnfinishedBand(String chunkKey, Integer highestUnfinishedY) {
        if (highestUnfinishedY == null) return true;
        Integer y = parseBandY(chunkKey);
        return y == null || y >= highestUnfinishedY;
    }

    private static boolean isEligibleLookaheadBand(String chunkKey, Integer nextBandY,
                                                   Map<String, Integer> highestUnfinishedByCol) {
        if (nextBandY == null) return false;
        Integer y = parseBandY(chunkKey);
        if (y == null || !y.equals(nextBandY)) return false;
        Integer colTop = highestUnfinishedByCol.get(columnKeyOf(chunkKey));
        return colTop == null || y >= colTop;
    }

    private static boolean isWithinSpatialBuffer(ChunkEntry candidate, List<ChunkEntry> activeOthers, int buffer) {
        for (ChunkEntry other : activeOthers) {
            int dx = Math.abs(candidate.cx - other.cx);
            int dz = Math.abs(candidate.cz - other.cz);
            if (Math.max(dx, dz) <= buffer) {
                return true;
            }
        }
        return false;
    }

    private static double distSq2D(int x1, int z1, int x2, int z2) {
        double dx = x1 - x2;
        double dz = z1 - z2;
        return dx * dx + dz * dz;
    }

    private static void clearChunkOwner(ChunkEntry ce, ChunkState newState) {
        ce.state = newState;
        ce.ownerUuid = null;
        ce.ownerName = null;
        ce.leaseExpiresAtMs = 0L;
        ce.graceExpiresAtMs = 0L;
        ce.nextRetryAtMs = 0L;
    }

    static String sha256Short(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                sb.append(String.format(Locale.ROOT, "%02x", hash[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            return Integer.toHexString(input.hashCode());
        }
    }
}
