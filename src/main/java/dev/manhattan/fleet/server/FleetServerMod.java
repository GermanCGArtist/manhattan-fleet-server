package dev.manhattan.fleet.server;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.mojang.brigadier.arguments.StringArgumentType;
import dev.manhattan.fleet.server.FleetModels.*;
import dev.manhattan.fleet.wire.FleetWire;
import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.fabricmc.fabric.api.entity.event.v1.ServerLivingEntityEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
import net.fabricmc.fabric.api.networking.v1.PayloadTypeRegistry;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayNetworking;
import net.fabricmc.loader.api.FabricLoader;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.storage.LevelResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Fabric server entrypoint for Manhattan Fleet Server.
 */
public final class FleetServerMod implements ModInitializer {
    private static final Logger LOG = LoggerFactory.getLogger("manhattan-fleet");
    private static final Gson GSON = new Gson();
    private static final Type STRING_LIST = new TypeToken<List<String>>() {}.getType();
    private static final Type CHUNK_SPEC_LIST = new TypeToken<List<ChunkSpec>>() {}.getType();
    private static final Type INT_ARRAY_LIST = new TypeToken<List<int[]>>() {}.getType();
    private static final Type ITEM_COUNTS = new TypeToken<Map<String, Integer>>() {}.getType();

    private static volatile FleetServerMod instance;

    private FleetConfig config;
    private FleetManager manager;
    private Path configPath;
    private FleetStore store;
    private int tickCounter;
    private boolean snapshotDirty;
    private int ticksSinceSnapshot;
    /** botUuid -> [window start ms, intents in window] for the path-intent rate limit. */
    private final Map<String, long[]> intentRate = new HashMap<>();
    private final FleetWire.PacketReassembler packetReassembler = new FleetWire.PacketReassembler();

    /** The running instance (for the server-side benchmark bridge and tools); null before initialization. */
    public static FleetServerMod instance() {
        return instance;
    }

    public FleetManager manager() {
        return manager;
    }

    @Override
    public void onInitialize() {
        instance = this;
        configPath = FabricLoader.getInstance().getConfigDir().resolve("manhattan-fleet.json");
        config = FleetConfig.loadOrCreate(configPath);
        manager = new FleetManager(config, System::currentTimeMillis);

        PayloadTypeRegistry.serverboundPlay().register(FleetPayload.TYPE, FleetPayload.CODEC);
        PayloadTypeRegistry.clientboundPlay().register(FleetPayload.TYPE, FleetPayload.CODEC);

        ServerPlayNetworking.registerGlobalReceiver(FleetPayload.TYPE, (payload, context) -> {
            ServerPlayer player = context.player();
            MinecraftServer server = context.server();
            server.execute(() -> {
                try {
                    handlePacket(server, player, payload.json());
                } catch (RuntimeException e) {
                    LOG.error("[Fleet] Message from {} failed: {}", player.getGameProfile().name(), e.toString(), e);
                    sendError(player, "Fleet server could not handle a message: " + e);
                }
            });
        });

        ServerLifecycleEvents.SERVER_STARTED.register(server -> {
            Path statePath = server.getWorldPath(LevelResource.ROOT).resolve("data").resolve("manhattan-fleet.json");
            store = new FleetStore(statePath);
            FleetStore.LoadResult res = store.load();
            if (res.ok()) {
                LOG.info("[Fleet] {} ({}).", res.message(), FleetStoreInfo.describe(res.state()));
            } else {
                LOG.error("[Fleet] {}", res.message());
            }
            manager.importState(res.state());
        });

        ServerLifecycleEvents.SERVER_STOPPING.register(server -> {
            packetReassembler.clearAll();
            if (store != null) {
                saveProjects();
                store.saveNowAndFlush(manager.exportState());
                store.close();
            }
        });

        ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
            ServerPlayer player = handler.getPlayer();
            String uuid = player.getUUID().toString();
            intentRate.remove(uuid);
            packetReassembler.clearSender(uuid);
            SleepCoordinator.SleepOrder nextSleeper = manager.disconnectBot(uuid);
            if (nextSleeper != null) {
                sendSleepOrder(server, nextSleeper);
            }
            snapshotDirty = true;
        });

        ServerLivingEntityEvents.AFTER_DEATH.register((entity, damageSource) -> {
            if (entity instanceof ServerPlayer player) {
                MinecraftServer server = player.level().getServer();
                if (server != null) {
                    SleepCoordinator.SleepOrder nextSleeper = manager.onBotDeath(player.getUUID().toString());
                    if (nextSleeper != null) {
                        sendSleepOrder(server, nextSleeper);
                    }
                    snapshotDirty = true;
                }
            }
        });

        ServerTickEvents.END_SERVER_TICK.register(this::onServerTick);
        CommandRegistrationCallback.EVENT.register((dispatcher, registryAccess, env) -> registerCommands(dispatcher));
    }

    private void onServerTick(MinecraftServer server) {
        tickCounter++;
        ticksSinceSnapshot++;
        long now = System.currentTimeMillis();

        // Check lease expiry & coordinated 1-player sleep every second (20 ticks).
        if (tickCounter % 20 == 0) {
            manager.tick(now);

            ServerLevel overworld = server.overworld();
            if (overworld != null) {
                boolean isNightOrThunder = overworld.isDarkOutside() || overworld.isThundering();
                SleepCoordinator.SleepOrder order = manager.sleepCoordinator()
                        .tick(now, isNightOrThunder, manager.botsView());
                if (order != null) {
                    sendSleepOrder(server, order);
                }
            }

            if (store != null) {
                saveProjects();
                if (manager.consumeDirty()) {
                    store.saveAsync(manager.exportState());
                }
            }
        }

        // Snapshots: soon after a change (at most every half second), and every 2 s regardless.
        if ((snapshotDirty && ticksSinceSnapshot >= 10) || ticksSinceSnapshot >= 40) {
            broadcastSnapshot(server);
        }
    }

    private void saveProjects() {
        for (FleetJob job : manager.drainUnsavedProjects()) {
            store.saveProjectIfAbsent(job.projectHash, job.projectJson);
        }
    }

    private void handlePacket(MinecraftServer server, ServerPlayer player, String rawJson) {
        if (rawJson == null || rawJson.isBlank()) return;
        JsonObject root;
        try {
            root = JsonParser.parseString(rawJson).getAsJsonObject();
        } catch (Exception e) {
            LOG.warn("[Fleet] Dropped an unreadable message from {}: {}", player.getGameProfile().name(), e.toString());
            return;
        }

        int version = root.has("v") ? root.get("v").getAsInt() : 0;
        String type = root.has("type") ? root.get("type").getAsString() : "";
        JsonObject body = root.has("body") && root.get("body").isJsonObject()
                ? root.getAsJsonObject("body")
                : new JsonObject();

        if (!FleetWire.isCompatible(version)) {
            sendError(player, FleetWire.incompatibleMessage(version));
            return;
        }

        String uuid = player.getUUID().toString();
        String name = player.getGameProfile().name();
        String dim = player.level().dimension().identifier().toString();
        long now = System.currentTimeMillis();

        switch (type) {
            case FleetWire.PACKET_CHUNK -> {
                String id = getStr(body, "id", "");
                int idx = body.has("i") ? body.get("i").getAsInt() : -1;
                int total = body.has("n") ? body.get("n").getAsInt() : 0;
                boolean gz = !body.has("gz") || body.get("gz").getAsBoolean();
                String data = getStr(body, "d", null);
                try {
                    String full = packetReassembler.accept(uuid, id, idx, total, gz, data);
                    if (full != null) handlePacket(server, player, full);
                } catch (java.io.IOException e) {
                    LOG.warn("[Fleet] Failed to decompress chunked packet {} from {}: {}", id, name, e.toString());
                    sendError(player, "Failed to decompress chunked message: " + e.getMessage());
                }
            }

            case FleetWire.HELLO -> {
                ClientMode mode = parseEnum(ClientMode.class, getStr(body, "mode", "LOCAL"), ClientMode.LOCAL);
                List<String> tags = body.has("tags") ? GSON.fromJson(body.get("tags"), STRING_LIST) : List.of();
                int[] localBed = body.has("localBed") && !body.get("localBed").isJsonNull()
                        ? GSON.fromJson(body.get("localBed"), int[].class) : null;
                String schema = getStr(body, "schema", "");
                if (!FleetWire.SCHEMA.equals(schema)) {
                    LOG.warn("[Fleet] {} speaks protocol v{} with a different message set; update both mods.", name, version);
                }

                manager.registerBot(uuid, name, mode, tags, localBed, dim,
                        player.getBlockX(), player.getBlockY(), player.getBlockZ());

                JsonObject welcome = new JsonObject();
                welcome.addProperty("protocolVersion", FleetWire.PROTOCOL_VERSION);
                welcome.addProperty("schema", FleetWire.SCHEMA);
                welcome.addProperty("editorAllowed", isEditorAllowed(player));
                welcome.addProperty("worldId", server.getWorldData().getLevelName());
                welcome.add("snapshot", GSON.toJsonTree(manager.snapshot()));
                welcome.add("hazards", GSON.toJsonTree(manager.hazards()));
                welcome.add("chests", GSON.toJsonTree(manager.chestCoordinator().snapshot()));
                send(player, FleetWire.WELCOME, welcome);
                snapshotDirty = true;
            }

            case FleetWire.SUBMIT_JOB -> {
                if (!isEditorAllowed(player)) {
                    sendError(player, "Not authorized as Fleet Editor.");
                    return;
                }
                String projectId = getStr(body, "projectId", "project");
                String projectName = getStr(body, "projectName", projectId);
                String projectJson = getStr(body, "projectJson", "{}");
                String jobDim = getStr(body, "dimension", dim);
                AssignmentPolicy assignment = body.has("assignment")
                        ? GSON.fromJson(body.get("assignment"), AssignmentPolicy.class)
                        : new AssignmentPolicy();
                List<ChunkSpec> chunks = body.has("chunks")
                        ? GSON.fromJson(body.get("chunks"), CHUNK_SPEC_LIST)
                        : List.of();
                List<int[]> bedPool = body.has("bedPool")
                        ? GSON.fromJson(body.get("bedPool"), INT_ARRAY_LIST)
                        : List.of();

                FleetJob job = manager.submitJob(projectId, projectName, projectJson, jobDim, assignment, chunks, bedPool, name);
                LOG.info("[Fleet] {} submitted {} '{}' ({} chunks).", name, job.jobId, job.projectName, job.chunks.size());
                JsonObject ack = new JsonObject();
                ack.addProperty("jobId", job.jobId);
                ack.addProperty("chunkCount", job.chunks.size());
                ack.addProperty("submitId", getStr(body, "submitId", ""));
                send(player, FleetWire.JOB_ACK, ack);
                snapshotDirty = true;
            }

            case FleetWire.JOB_CONTROL -> {
                if (!isEditorAllowed(player)) {
                    sendError(player, "Not authorized as Fleet Editor.");
                    return;
                }
                String jobId = getStr(body, "jobId", "");
                String action = getStr(body, "action", "");
                switch (action.toLowerCase()) {
                    case "pause" -> manager.pauseJob(jobId);
                    case "resume" -> manager.resumeJob(jobId);
                    case "cancel" -> {
                        List<String> affected = manager.cancelJobAndListWorkers(jobId);
                        for (String botUuid : affected) {
                            sendWorkerStop(server, botUuid, jobId, "Job " + jobId + " cancelled by " + name);
                        }
                    }
                    case "cancel_all" -> {
                        List<String> affected = manager.cancelAllJobs();
                        for (String botUuid : affected) {
                            sendWorkerStop(server, botUuid, "", "All fleet jobs cancelled by " + name);
                        }
                    }
                    case "clear_finished" -> manager.clearFinishedJobs();
                    case "reassign" -> {
                        if (body.has("assignment")) {
                            AssignmentPolicy p = GSON.fromJson(body.get("assignment"), AssignmentPolicy.class);
                            manager.updateAssignment(jobId, p);
                        }
                    }
                    default -> {}
                }
                snapshotDirty = true;
            }

            case FleetWire.WORKER_CONTROL -> {
                if (!isEditorAllowed(player)) {
                    sendError(player, "Not authorized as Fleet Editor.");
                    return;
                }
                String botUuid = getStr(body, "botUuid", "");
                String action = getStr(body, "action", "");
                String targetJobId = getStr(body, "targetJobId", "");
                switch (action.toLowerCase()) {
                    case "stop" -> {
                        if (manager.stopWorker(botUuid, true)) {
                            sendWorkerStop(server, botUuid, "", "Stopped & held by " + name);
                        }
                    }
                    case "release" -> {
                        if (manager.stopWorker(botUuid, false)) {
                            sendWorkerStop(server, botUuid, "", "Chunk released by " + name);
                        }
                    }
                    case "resume" -> manager.resumeWorker(botUuid);
                    case "reassign" -> {
                        if (manager.reassignWorker(botUuid, targetJobId)) {
                            sendWorkerStop(server, botUuid, "", "Reassigned to " + targetJobId + " by " + name);
                        }
                    }
                    default -> {}
                }
                snapshotDirty = true;
            }

            case FleetWire.CLAIM_WORK -> {
                String knownHash = getStr(body, "knownProjectHash", null);
                WorkGrant grant = manager.claimWork(uuid, dim,
                        player.getBlockX(), player.getBlockY(), player.getBlockZ(), knownHash);
                grant.claimId = getStr(body, "claimId", null);
                send(player, FleetWire.WORK_LEASE, GSON.toJsonTree(grant));
                if (!grant.idle) snapshotDirty = true;
            }

            case FleetWire.HEARTBEAT -> {
                double health = body.has("health") ? body.get("health").getAsDouble() : player.getHealth();
                int food = body.has("food") ? body.get("food").getAsInt() : player.getFoodData().getFoodLevel();
                int freeSlots = body.has("freeSlots") ? body.get("freeSlots").getAsInt() : 0;
                boolean inDanger = body.has("inDanger") && body.get("inDanger").getAsBoolean();
                String activeJobId = getStr(body, "activeJobId", null);
                String activeChunkKey = getStr(body, "activeChunkKey", null);
                String phase = getStr(body, "phase", "IDLE");
                String alertSummary = getStr(body, "alertSummary", "");
                long epoch = body.has("leaseEpoch") ? body.get("leaseEpoch").getAsLong() : 0L;
                int[] localBed = body.has("localBed") && !body.get("localBed").isJsonNull()
                        ? GSON.fromJson(body.get("localBed"), int[].class) : null;

                manager.updateHeartbeat(uuid, dim,
                        player.getBlockX(), player.getBlockY(), player.getBlockZ(),
                        health, food, freeSlots, inDanger, activeJobId, activeChunkKey, phase, alertSummary, localBed,
                        epoch);
            }

            case FleetWire.CHUNK_REPORT -> {
                String jobId = getStr(body, "jobId", "");
                String chunkKey = getStr(body, "chunkKey", "");
                ChunkState outcome = parseEnum(ChunkState.class, getStr(body, "outcome", "DEFERRED"), ChunkState.DEFERRED);
                String reason = getStr(body, "reason", "");
                long epoch = body.has("leaseEpoch") ? body.get("leaseEpoch").getAsLong() : 0L;
                String msgId = getStr(body, "msgId", null);
                boolean wantNext = body.has("wantNext") && body.get("wantNext").getAsBoolean();

                ReportResult res = manager.reportChunk(uuid, jobId, chunkKey, outcome, reason, epoch, msgId);
                if (!res.accepted()) {
                    LOG.info("[Fleet] Report {} {} from {} not applied: {}", chunkKey, outcome, name, res.reason());
                }
                JsonObject ack = new JsonObject();
                ack.addProperty("msgId", msgId == null ? "" : msgId);
                ack.addProperty("jobId", jobId);
                ack.addProperty("chunkKey", chunkKey);
                ack.addProperty("accepted", res.accepted());
                ack.addProperty("duplicate", res.duplicate());
                ack.addProperty("reason", res.reason());
                send(player, FleetWire.REPORT_ACK, ack);

                // Hand-off: answer a finished chunk with the next lease at once, so the worker never idles.
                if (wantNext && res.accepted() && !res.duplicate()) {
                    WorkGrant next = manager.claimWork(uuid, dim, player.getBlockX(), player.getBlockY(),
                            player.getBlockZ(), getStr(body, "knownProjectHash", null));
                    next.claimId = getStr(body, "claimId", null);
                    send(player, FleetWire.WORK_LEASE, GSON.toJsonTree(next));
                }
                snapshotDirty = true;
            }

            case FleetWire.CHEST_REPORT -> {
                int cx = body.has("x") ? body.get("x").getAsInt() : 0;
                int cy = body.has("y") ? body.get("y").getAsInt() : 0;
                int cz = body.has("z") ? body.get("z").getAsInt() : 0;
                ChestState st = parseEnum(ChestState.class, getStr(body, "state", "AVAILABLE"), ChestState.AVAILABLE);
                ChestRecord rec = manager.chestCoordinator().reportState(dim, cx, cy, cz, st, name, now);
                manager.markDirty();
                broadcastToConnected(server, FleetWire.CHEST_SYNC, GSON.toJsonTree(rec));
            }

            case FleetWire.STOCK_REPORT -> {
                int cx = body.has("x") ? body.get("x").getAsInt() : 0;
                int cy = body.has("y") ? body.get("y").getAsInt() : 0;
                int cz = body.has("z") ? body.get("z").getAsInt() : 0;
                Map<String, Integer> items = body.has("items") && body.get("items").isJsonObject()
                        ? GSON.fromJson(body.get("items"), ITEM_COUNTS) : Map.of();
                ChestRecord rec = manager.chestCoordinator().reportStock(dim, cx, cy, cz, items, uuid, name, now);
                manager.markDirty();
                broadcastToConnected(server, FleetWire.STOCK_SYNC, GSON.toJsonTree(rec));
            }

            case FleetWire.RESERVE_ITEMS -> {
                int cx = body.has("x") ? body.get("x").getAsInt() : 0;
                int cy = body.has("y") ? body.get("y").getAsInt() : 0;
                int cz = body.has("z") ? body.get("z").getAsInt() : 0;
                String item = getStr(body, "item", "");
                int count = body.has("count") ? body.get("count").getAsInt() : 0;
                ChestCoordinator.ReserveResult r = manager.chestCoordinator().reserve(dim, cx, cy, cz, item,
                        Math.max(0, count), uuid, name, FleetWire.RESERVATION_TTL_MS, now);
                JsonObject reply = new JsonObject();
                reply.addProperty("x", cx);
                reply.addProperty("y", cy);
                reply.addProperty("z", cz);
                reply.addProperty("item", item);
                reply.addProperty("granted", r.granted());
                reply.addProperty("available", r.available());
                reply.addProperty("expiresAtMs", r.expiresAtMs());
                reply.addProperty("requestId", getStr(body, "requestId", ""));
                send(player, FleetWire.RESERVE_REPLY, reply);
                if (r.granted() > 0) {
                    broadcastToConnected(server, FleetWire.STOCK_SYNC,
                            GSON.toJsonTree(manager.chestCoordinator().get(dim, cx, cy, cz)));
                }
            }

            case FleetWire.CHEST_LOCK -> {
                int cx = body.has("x") ? body.get("x").getAsInt() : 0;
                int cy = body.has("y") ? body.get("y").getAsInt() : 0;
                int cz = body.has("z") ? body.get("z").getAsInt() : 0;
                boolean unlock = body.has("unlock") && body.get("unlock").getAsBoolean();
                if (unlock) {
                    manager.chestCoordinator().unlock(dim, cx, cy, cz, uuid);
                } else {
                    ChestCoordinator.LockResult res = manager.chestCoordinator().tryLock(
                            dim, cx, cy, cz, uuid, name, manager.config().chestLockTtlSeconds * 1000L, now);
                    JsonObject reply = new JsonObject();
                    reply.addProperty("x", cx);
                    reply.addProperty("y", cy);
                    reply.addProperty("z", cz);
                    reply.addProperty("granted", res.granted());
                    reply.addProperty("lockedBy", res.lockedByName() == null ? "" : res.lockedByName());
                    reply.addProperty("expiresAtMs", res.expiresAtMs());
                    send(player, FleetWire.CHEST_LOCK_REPLY, reply);
                }
            }

            case FleetWire.SLEEP_STATUS -> {
                String status = getStr(body, "status", "");
                SleepCoordinator.SleepOrder next = manager.sleepCoordinator()
                        .onSleepStatus(uuid, status, now, manager.botsView());
                if (next != null) {
                    sendSleepOrder(server, next);
                }
                snapshotDirty = true;
            }

            case FleetWire.PATH_INTENT -> relayPathIntent(server, player, uuid, name, dim, body, now);

            case FleetWire.HAZARD_REPORT -> {
                int hx = body.has("x") ? body.get("x").getAsInt() : player.getBlockX();
                int hy = body.has("y") ? body.get("y").getAsInt() : player.getBlockY();
                int hz = body.has("z") ? body.get("z").getAsInt() : player.getBlockZ();
                String hdim = getStr(body, "dimension", dim);
                HazardRecord h = manager.reportHazard(name, hdim, hx, hy, hz, getStr(body, "kind", "death"));
                if (h != null) {
                    JsonObject sync = new JsonObject();
                    JsonArray arr = new JsonArray();
                    arr.add(GSON.toJsonTree(h));
                    sync.add("hazards", arr);
                    broadcastToConnected(server, FleetWire.HAZARD_SYNC, sync);
                }
            }

            default -> {}
        }
    }

    /** Relays the next steps of a bot's path to the other bots nearby (rate-limited, never stored). */
    private void relayPathIntent(MinecraftServer server, ServerPlayer from, String uuid, String name, String dim,
                                 JsonObject body, long now) {
        long[] rate = intentRate.computeIfAbsent(uuid, k -> new long[]{now, 0});
        if (now - rate[0] >= 1000L) {
            rate[0] = now;
            rate[1] = 0;
        }
        if (++rate[1] > FleetWire.MAX_INTENTS_PER_SECOND) return;

        JsonArray points = body.has("points") && body.get("points").isJsonArray()
                ? body.getAsJsonArray("points") : new JsonArray();
        JsonArray trimmed = new JsonArray();
        for (int i = 0; i < points.size() && i < FleetWire.MAX_INTENT_POINTS; i++) trimmed.add(points.get(i));

        JsonObject out = new JsonObject();
        out.addProperty("uuid", uuid);
        out.addProperty("name", name);
        out.addProperty("dimension", dim);
        out.addProperty("x", from.getX());
        out.addProperty("y", from.getY());
        out.addProperty("z", from.getZ());
        out.addProperty("atMs", now);
        out.add("points", trimmed);
        FleetPayload payload = new FleetPayload(encodeEnvelope(FleetWire.PEER_PATHS, out));
        double r2 = (double) FleetWire.INTENT_RELAY_RADIUS * FleetWire.INTENT_RELAY_RADIUS;
        for (ServerPlayer p : server.getPlayerList().getPlayers()) {
            if (p == from || !ServerPlayNetworking.canSend(p, FleetPayload.TYPE)) continue;
            if (!p.level().dimension().identifier().toString().equals(dim)) continue;
            if (p.distanceToSqr(from) > r2) continue;
            ServerPlayNetworking.send(p, payload);
        }
    }

    private boolean isEditorAllowed(ServerPlayer player) {
        if (manager.config().isEditorWhitelisted(player.getUUID(), player.getGameProfile().name())) {
            return true;
        }
        return player.permissions().hasPermission(new net.minecraft.server.permissions.Permission.HasCommandLevel(
                net.minecraft.server.permissions.PermissionLevel.GAMEMASTERS));
    }

    private void sendWorkerStop(MinecraftServer server, String botUuid, String jobId, String reason) {
        if (botUuid == null || botUuid.isBlank()) return;
        UUID id;
        try {
            id = UUID.fromString(botUuid);
        } catch (IllegalArgumentException e) {
            LOG.warn("[Fleet] Cannot stop worker '{}': not a UUID.", botUuid);
            return;
        }
        ServerPlayer target = server.getPlayerList().getPlayer(id);
        if (target == null) return;
        JsonObject body = new JsonObject();
        body.addProperty("action", "stop");
        body.addProperty("jobId", jobId != null ? jobId : "");
        body.addProperty("reason", reason != null ? reason : "Stopped by Fleet Editor");
        send(target, FleetWire.WORKER_COMMAND, body);
    }

    private void sendSleepOrder(MinecraftServer server, SleepCoordinator.SleepOrder order) {
        ServerPlayer target;
        try {
            target = server.getPlayerList().getPlayer(UUID.fromString(order.botUuid()));
        } catch (IllegalArgumentException e) {
            return;
        }
        if (target != null && ServerPlayNetworking.canSend(target, FleetPayload.TYPE)) {
            JsonObject body = new JsonObject();
            body.addProperty("sleepNow", true);
            body.add("bedPos", GSON.toJsonTree(order.bedPos()));
            body.addProperty("expiresAtMs", order.expiresAtMs());
            send(target, FleetWire.SLEEP_ORDER, body);
        }
    }

    private void broadcastSnapshot(MinecraftServer server) {
        snapshotDirty = false;
        ticksSinceSnapshot = 0;
        JsonElement snap = GSON.toJsonTree(manager.snapshot());
        broadcastToConnected(server, FleetWire.FLEET_SNAPSHOT, snap);
    }

    private void broadcastToConnected(MinecraftServer server, String type, JsonElement body) {
        List<String> frames = FleetWire.frameForWire(encodeEnvelope(type, body));
        List<FleetPayload> payloads = new java.util.ArrayList<>(frames.size());
        for (String f : frames) payloads.add(new FleetPayload(f));
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            if (ServerPlayNetworking.canSend(player, FleetPayload.TYPE)) {
                for (FleetPayload p : payloads) {
                    ServerPlayNetworking.send(player, p);
                }
            }
        }
    }

    private void send(ServerPlayer player, String type, JsonElement body) {
        if (ServerPlayNetworking.canSend(player, FleetPayload.TYPE)) {
            for (String frame : FleetWire.frameForWire(encodeEnvelope(type, body))) {
                ServerPlayNetworking.send(player, new FleetPayload(frame));
            }
        }
    }

    private void sendError(ServerPlayer player, String message) {
        JsonObject err = new JsonObject();
        err.addProperty("error", message);
        send(player, FleetWire.ERROR, err);
    }

    private static String encodeEnvelope(String type, JsonElement body) {
        JsonObject env = new JsonObject();
        env.addProperty("v", FleetWire.PROTOCOL_VERSION);
        env.addProperty("type", type);
        env.add("body", body == null ? new JsonObject() : body);
        return GSON.toJson(env);
    }

    private void registerCommands(com.mojang.brigadier.CommandDispatcher<net.minecraft.commands.CommandSourceStack> dispatcher) {
        dispatcher.register(Commands.literal("fleet")
                .requires(src -> src.permissions().hasPermission(
                        new net.minecraft.server.permissions.Permission.HasCommandLevel(
                                net.minecraft.server.permissions.PermissionLevel.GAMEMASTERS)))
                .then(Commands.literal("status").executes(ctx -> {
                    FleetSnapshot snap = manager.snapshot();
                    long onlineWorkers = snap.bots.stream().filter(b -> b.online).count();
                    ctx.getSource().sendSuccess(() -> Component.literal(
                            "[Fleet] Online clients: " + onlineWorkers + ", Jobs: " + snap.jobs.size()
                                    + ", Active claims: " + snap.activeClaims.size()
                                    + ", Hazards: " + manager.hazards().size()
                                    + (snap.designatedSleeperName != null ? ", Sleeper: " + snap.designatedSleeperName : "")),
                            false);
                    return 1;
                }))
                .then(Commands.literal("pause")
                        .then(Commands.argument("jobId", StringArgumentType.word()).executes(ctx -> {
                            String id = StringArgumentType.getString(ctx, "jobId");
                            boolean ok = manager.pauseJob(id);
                            ctx.getSource().sendSuccess(() -> Component.literal(ok ? "[Fleet] Paused " + id : "[Fleet] Could not pause " + id), false);
                            return ok ? 1 : 0;
                        })))
                .then(Commands.literal("resume")
                        .then(Commands.argument("jobId", StringArgumentType.word()).executes(ctx -> {
                            String id = StringArgumentType.getString(ctx, "jobId");
                            boolean ok = manager.resumeJob(id);
                            ctx.getSource().sendSuccess(() -> Component.literal(ok ? "[Fleet] Resumed " + id : "[Fleet] Could not resume " + id), false);
                            return ok ? 1 : 0;
                        })))
                .then(Commands.literal("cancel")
                        .then(Commands.argument("jobId", StringArgumentType.word()).executes(ctx -> {
                            String id = StringArgumentType.getString(ctx, "jobId");
                            boolean ok = manager.cancelJob(id);
                            ctx.getSource().sendSuccess(() -> Component.literal(ok ? "[Fleet] Cancelled " + id : "[Fleet] Could not cancel " + id), false);
                            return ok ? 1 : 0;
                        })))
                .then(Commands.literal("reload").executes(ctx -> {
                    config = FleetConfig.loadOrCreate(configPath);
                    manager.updateConfig(config);
                    ctx.getSource().sendSuccess(() -> Component.literal("[Fleet] Reloaded config/manhattan-fleet.json"), false);
                    return 1;
                }))
        );
    }

    private static String getStr(JsonObject obj, String key, String def) {
        if (obj == null || !obj.has(key) || obj.get(key).isJsonNull()) return def;
        return obj.get(key).getAsString();
    }

    private static <E extends Enum<E>> E parseEnum(Class<E> cls, String val, E def) {
        if (val == null || val.isBlank()) return def;
        try {
            return Enum.valueOf(cls, val.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return def;
        }
    }

    /** Small helper so the log line names what was loaded. */
    private static final class FleetStoreInfo {
        static String describe(FleetStore.PersistedState st) {
            return st.jobs.size() + " job(s), " + st.bots.size() + " bot(s), " + st.hazards.size() + " hazard(s)";
        }
    }
}
