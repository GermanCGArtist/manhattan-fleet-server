package dev.manhattan.fleet.server;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.manhattan.fleet.server.FleetModels.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Persistence for the fleet ({@code <world>/data/manhattan-fleet.json}).
 *
 * <ul>
 *   <li>The state file holds jobs (without their project JSON), bots, hazards and chest stock. It is small, so
 *       it is serialized on the server thread and written by a background thread (latest write wins).</li>
 *   <li>Each project is written once to {@code manhattan-fleet-projects/<hash>.json}; projects never change.</li>
 *   <li>Before a write replaces the state file, the previous good file is kept as {@code .bak}.</li>
 *   <li>A file that cannot be read is moved aside to {@code .corrupt-<time>} and never overwritten; the
 *       {@code .bak} is tried next. Every failure is logged.</li>
 * </ul>
 */
public final class FleetStore {
    private static final Logger LOG = LoggerFactory.getLogger("manhattan-fleet");
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    public static final int STATE_VERSION = 2;

    /** Everything that survives a restart. */
    public static final class PersistedState {
        public int version = STATE_VERSION;
        public int jobCounter = 1;
        public List<FleetJob> jobs = new ArrayList<>();
        public List<PersistedBot> bots = new ArrayList<>();
        public List<HazardRecord> hazards = new ArrayList<>();
        public List<ChestRecord> chests = new ArrayList<>();
    }

    /** The parts of a bot record that matter after a restart (all bots come back offline). */
    public static final class PersistedBot {
        public String uuid;
        public String name;
        public ClientMode mode;
        public List<String> tags = new ArrayList<>();
        public boolean held;
        public String activeJobId;
        public int[] assignedBed;
    }

    /** What {@link #load} found. {@code ok} is false when a file existed but nothing could be read from it. */
    public record LoadResult(PersistedState state, boolean ok, String message) {}

    private final Path statePath;
    private final Path projectDir;
    private final ExecutorService writer;
    private final AtomicReference<String> pendingState = new AtomicReference<>();
    private volatile boolean writeFailedLogged;

    public FleetStore(Path statePath) {
        this.statePath = statePath;
        this.projectDir = statePath.resolveSibling("manhattan-fleet-projects");
        this.writer = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "manhattan-fleet-store");
            t.setDaemon(true);
            return t;
        });
    }

    public Path statePath() {
        return statePath;
    }

    // ------------------------------------------------------------------ loading

    /** Loads the state file, falling back to its backup; attaches each job's project JSON. */
    public LoadResult load() {
        if (!Files.exists(statePath) && !Files.exists(backupPath())) {
            return new LoadResult(new PersistedState(), true, "no state file yet");
        }
        String firstError = null;
        if (Files.exists(statePath)) {
            try {
                PersistedState st = parse(Files.readString(statePath, StandardCharsets.UTF_8));
                attachProjects(st);
                return new LoadResult(st, true, "loaded " + st.jobs.size() + " job(s)");
            } catch (Exception e) {
                firstError = e.toString();
                Path aside = moveAside(statePath);
                LOG.error("[Fleet] Could not read {} ({}); moved it to {} and trying the backup.",
                        statePath, e, aside);
            }
        }
        if (Files.exists(backupPath())) {
            try {
                PersistedState st = parse(Files.readString(backupPath(), StandardCharsets.UTF_8));
                attachProjects(st);
                LOG.warn("[Fleet] Restored fleet state from backup {} ({} job(s)).", backupPath(), st.jobs.size());
                return new LoadResult(st, true, "restored from backup");
            } catch (Exception e) {
                Path aside = moveAside(backupPath());
                LOG.error("[Fleet] The backup {} is unreadable too ({}); moved it to {}.", backupPath(), e, aside);
                if (firstError == null) firstError = e.toString();
            }
        }
        return new LoadResult(new PersistedState(), false,
                "fleet state unreadable (" + firstError + "); starting empty, the old files were kept");
    }

    /** Parses a version 1 or 2 state file. Throws when the text is not a fleet state. */
    static PersistedState parse(String json) {
        JsonElement rootEl = JsonParser.parseString(json);
        if (!rootEl.isJsonObject()) throw new IllegalStateException("not a JSON object");
        JsonObject root = rootEl.getAsJsonObject();
        if (!root.has("jobs")) throw new IllegalStateException("no 'jobs' list");
        PersistedState st = GSON.fromJson(root, PersistedState.class);
        if (st == null) throw new IllegalStateException("empty state");
        if (st.jobs == null) st.jobs = new ArrayList<>();
        if (st.bots == null) st.bots = new ArrayList<>();
        if (st.hazards == null) st.hazards = new ArrayList<>();
        if (st.chests == null) st.chests = new ArrayList<>();
        // Version 1 kept the project JSON inside each job (a transient field now): read it by hand.
        if (root.get("jobs").isJsonArray()) {
            var arr = root.getAsJsonArray("jobs");
            for (int i = 0; i < arr.size() && i < st.jobs.size(); i++) {
                JsonElement el = arr.get(i);
                if (el.isJsonObject() && el.getAsJsonObject().has("projectJson")
                        && !el.getAsJsonObject().get("projectJson").isJsonNull() && st.jobs.get(i) != null) {
                    st.jobs.get(i).projectJson = el.getAsJsonObject().get("projectJson").getAsString();
                }
            }
        }
        st.jobs.removeIf(j -> j == null || j.jobId == null || j.jobId.isBlank());
        for (FleetJob j : st.jobs) j.ensureInitialized();
        return st;
    }

    private void attachProjects(PersistedState st) {
        for (FleetJob job : st.jobs) {
            if (job.projectJson != null && !job.projectJson.isBlank()) {
                // a version 1 file: write the project out so the next save can drop it from the state file
                if (job.projectHash == null || job.projectHash.isBlank()) {
                    job.projectHash = FleetManager.sha256Short(job.projectJson);
                }
                saveProjectIfAbsent(job.projectHash, job.projectJson);
                continue;
            }
            String json = readProject(job.projectHash);
            if (json != null) {
                job.projectJson = json;
            } else {
                job.projectJson = null;
                if (job.status == JobStatus.ACTIVE || job.status == JobStatus.PAUSED) {
                    job.status = JobStatus.PAUSED;
                    job.statusReason = "project file " + job.projectHash + ".json is missing";
                    LOG.error("[Fleet] Job {}: project file {} is missing; the job is paused.",
                            job.jobId, job.projectHash == null ? "?" : projectPath(job.projectHash));
                }
            }
        }
    }

    private String readProject(String hash) {
        if (hash == null || hash.isBlank()) return null;
        Path p = projectPath(hash);
        if (!Files.exists(p)) return null;
        try {
            return Files.readString(p, StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("[Fleet] Could not read project file {}: {}", p, e.toString());
            return null;
        }
    }

    private Path backupPath() {
        return statePath.resolveSibling(statePath.getFileName() + ".bak");
    }

    Path projectPath(String hash) {
        String clean = hash.replaceAll("[^A-Za-z0-9_-]", "_");
        return projectDir.resolve(clean + ".json");
    }

    private static Path moveAside(Path p) {
        long stamp = System.currentTimeMillis();
        Path aside = p.resolveSibling(p.getFileName() + ".corrupt-" + stamp);
        for (int n = 1; Files.exists(aside); n++) {
            aside = p.resolveSibling(p.getFileName() + ".corrupt-" + stamp + "-" + n);
        }
        try {
            Files.move(p, aside);
            return aside;
        } catch (IOException e) {
            LOG.error("[Fleet] Could not move {} aside: {}", p, e.toString());
            return p;
        }
    }

    // ------------------------------------------------------------------ saving

    /** Serializes on the calling (server) thread; the file is written by the store thread. */
    public void saveAsync(PersistedState state) {
        String json = GSON.toJson(state);
        pendingState.set(json);
        writer.execute(this::drainPending);
    }

    /** Writes the project once; projects are immutable, so an existing file is left alone. */
    public void saveProjectIfAbsent(String hash, String projectJson) {
        if (hash == null || hash.isBlank() || projectJson == null) return;
        Path p = projectPath(hash);
        writer.execute(() -> {
            if (Files.exists(p)) return;
            try {
                Files.createDirectories(projectDir);
                writeAtomically(p, projectJson);
            } catch (IOException e) {
                LOG.error("[Fleet] Could not write project file {}: {}", p, e.toString());
            }
        });
    }

    /** Writes {@code state} now and waits for every queued write (server shutdown, tests). */
    public void saveNowAndFlush(PersistedState state) {
        pendingState.set(GSON.toJson(state));
        writer.execute(this::drainPending);
        flush();
    }

    /** Blocks until every queued write has finished. */
    public void flush() {
        try {
            writer.submit(() -> {}).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("[Fleet] Waiting for the fleet store to finish writing failed: {}", e.toString());
        }
    }

    public void close() {
        flush();
        writer.shutdown();
    }

    private void drainPending() {
        String json = pendingState.getAndSet(null);
        if (json == null) return;
        try {
            if (statePath.getParent() != null) Files.createDirectories(statePath.getParent());
            if (Files.exists(statePath)) {
                Files.copy(statePath, backupPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            writeAtomically(statePath, json);
            if (writeFailedLogged) {
                LOG.info("[Fleet] Writing {} works again.", statePath);
                writeFailedLogged = false;
            }
        } catch (IOException e) {
            if (!writeFailedLogged) {
                LOG.error("[Fleet] Could not write {}: {} (will keep trying)", statePath, e.toString());
                writeFailedLogged = true;
            }
        }
    }

    private static void writeAtomically(Path path, String text) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(tmp, text, StandardCharsets.UTF_8);
        try {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicUnsupported) {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
