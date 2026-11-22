package dev.manhattan.fleet.server;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.mojang.brigadier.arguments.StringArgumentType;
import dev.manhattan.fleet.server.FleetModels.*;
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

import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/**
 * Fabric server entrypoint for Manhattan Fleet Server.
 */
public final class FleetServerMod implements ModInitializer {
    private static final Gson GSON = new Gson();
    private static final Type STRING_LIST = new TypeToken<List<String>>() {}.getType();
    private static final Type CHUNK_SPEC_LIST = new TypeToken<List<ChunkSpec>>() {}.getType();
    private static final Type INT_ARRAY_LIST = new TypeToken<List<int[]>>() {}.getType();

    private FleetConfig config;
    private FleetManager manager;
    private Path configPath;
    private Path storePath;
    private int tickCounter;

    @Override
    public void onInitialize() {
        configPath = FabricLoader.getInstance().getConfigDir().resolve("manhattan-fleet.json");
        config = FleetConfig.loadOrCreate(configPath);
        manager = new FleetManager(config, System::currentTimeMillis);

        PayloadTypeRegistry.serverboundPlay().register(FleetPayload.TYPE, FleetPayload.CODEC);
        PayloadTypeRegistry.clientboundPlay().register(FleetPayload.TYPE, FleetPayload.CODEC);

        ServerPlayNetworking.registerGlobalReceiver(FleetPayload.TYPE, (payload, context) -> {
            ServerPlayer player = context.player();
            MinecraftServer server = context.server();
            server.execute(() -> handlePacket(server, player, payload.json()));
        });

        ServerLifecycleEvents.SERVER_STARTED.register(server -> {
            storePath = server.getWorldPath(LevelResource.ROOT).resolve("data").resolve("manhattan-fleet.json");
            manager.loadPersistedJobs(FleetStore.load(storePath));
        });

        ServerLifecycleEvents.SERVER_STOPPING.register(server -> {
            if (storePath != null) {
                FleetStore.save(storePath, manager.jobsView().values());
            }
        });

        ServerPlayConnectionEvents.DISCONNECT.register((handler, server) -> {
            ServerPlayer player = handler.getPlayer();
            SleepCoordinator.SleepOrder nextSleeper = manager.disconnectBot(player.getUUID().toString());
            if (nextSleeper != null) {
                sendSleepOrder(server, nextSleeper);
            }
            broadcastSnapshot(server);
        });

        ServerLivingEntityEvents.AFTER_DEATH.register((entity, damageSource) -> {
            if (entity instanceof ServerPlayer player) {
                MinecraftServer server = player.level().getServer();
                if (server != null) {
                    SleepCoordinator.SleepOrder nextSleeper = manager.onBotDeath(player.getUUID().toString());
                    if (nextSleeper != null) {
                        sendSleepOrder(server, nextSleeper);
                    }
                    broadcastSnapshot(server);
                }
            }
        });

        ServerTickEvents.END_SERVER_TICK.register(this::onServerTick);
        CommandRegistrationCallback.EVENT.register((dispatcher, registryAccess, env) -> registerCommands(dispatcher));
    }

    private void onServerTick(MinecraftServer server) {
        tickCounter++;
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

            if (storePath != null && manager.consumeDirty()) {
                FleetStore.save(storePath, manager.jobsView().values());
            }
        }

        // Broadcast fleet snapshot every 2 seconds (40 ticks).
        if (tickCounter % 40 == 0) {
            broadcastSnapshot(server);
        }
    }

    private void handlePacket(MinecraftServer server, ServerPlayer player, String rawJson) {
        if (rawJson == null || rawJson.isBlank()) return;
        JsonObject root;
        try {
            root = JsonParser.parseString(rawJson).getAsJsonObject();
        } catch (Exception e) {
            return;
        }

        int version = root.has("v") ? root.get("v").getAsInt() : 0;
        String type = root.has("type") ? root.get("type").getAsString() : "";
        JsonObject body = root.has("body") && root.get("body").isJsonObject()
                ? root.getAsJsonObject("body")
                : new JsonObject();

        if (version != FleetModels.PROTOCOL_VERSION) {
            JsonObject err = new JsonObject();
            err.addProperty("error", "Protocol mismatch: server=" + FleetModels.PROTOCOL_VERSION + " client=" + version);
            send(player, "error", err);
            return;
        }

        String uuid = player.getUUID().toString();
        String name = player.getGameProfile().name();
        String dim = player.level().dimension().identifier().toString();

        switch (type) {
            case "hello" -> {
                ClientMode mode = parseEnum(ClientMode.class, getStr(body, "mode", "LOCAL"), ClientMode.LOCAL);
                List<String> tags = body.has("tags") ? GSON.fromJson(body.get("tags"), STRING_LIST) : List.of();
                int[] localBed = body.has("localBed") && !body.get("localBed").isJsonNull()
                        ? GSON.fromJson(body.get("localBed"), int[].class) : null;

                manager.registerBot(uuid, name, mode, tags, localBed, dim,
                        player.getBlockX(), player.getBlockY(), player.getBlockZ());

                JsonObject welcome = new JsonObject();
                welcome.addProperty("protocolVersion", FleetModels.PROTOCOL_VERSION);
                welcome.addProperty("editorAllowed", isEditorAllowed(player));
                welcome.addProperty("worldId", server.getWorldData().getLevelName());
                welcome.add("snapshot", GSON.toJsonTree(manager.snapshot()));
                send(player, "welcome", welcome);
                broadcastSnapshot(server);
            }

            case "submit_job" -> {
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
                JsonObject ack = new JsonObject();
                ack.addProperty("jobId", job.jobId);
                ack.addProperty("chunkCount", job.chunks.size());
                send(player, "job_ack", ack);
                broadcastSnapshot(server);
            }

            case "job_control" -> {
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
                broadcastSnapshot(server);
            }

            case "worker_control" -> {
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
                broadcastSnapshot(server);
            }

            case "claim_work" -> {
                String knownHash = getStr(body, "knownProjectHash", null);
                WorkGrant grant = manager.claimWork(uuid, dim,
                        player.getBlockX(), player.getBlockY(), player.getBlockZ(), knownHash);
                send(player, "work_lease", GSON.toJsonTree(grant));
                if (!grant.idle) {
                    broadcastSnapshot(server);
                }
            }

            case "heartbeat" -> {
                double health = body.has("health") ? body.get("health").getAsDouble() : player.getHealth();
                int food = body.has("food") ? body.get("food").getAsInt() : player.getFoodData().getFoodLevel();
                int freeSlots = body.has("freeSlots") ? body.get("freeSlots").getAsInt() : 0;
                boolean inDanger = body.has("inDanger") && body.get("inDanger").getAsBoolean();
                String activeJobId = getStr(body, "activeJobId", null);
                String activeChunkKey = getStr(body, "activeChunkKey", null);
                String phase = getStr(body, "phase", "IDLE");
                String alertSummary = getStr(body, "alertSummary", "");
                int[] localBed = body.has("localBed") && !body.get("localBed").isJsonNull()
                        ? GSON.fromJson(body.get("localBed"), int[].class) : null;

                manager.updateHeartbeat(uuid, dim,
                        player.getBlockX(), player.getBlockY(), player.getBlockZ(),
                        health, food, freeSlots, inDanger, activeJobId, activeChunkKey, phase, alertSummary, localBed);
            }

            case "chunk_report" -> {
                String jobId = getStr(body, "jobId", "");
                String chunkKey = getStr(body, "chunkKey", "");
                ChunkState outcome = parseEnum(ChunkState.class, getStr(body, "outcome", "DEFERRED"), ChunkState.DEFERRED);
                String reason = getStr(body, "reason", "");
                manager.reportChunk(uuid, jobId, chunkKey, outcome, reason);
                broadcastSnapshot(server);
            }

            case "chest_report" -> {
                int cx = body.has("x") ? body.get("x").getAsInt() : 0;
                int cy = body.has("y") ? body.get("y").getAsInt() : 0;
                int cz = body.has("z") ? body.get("z").getAsInt() : 0;
                ChestState st = parseEnum(ChestState.class, getStr(body, "state", "AVAILABLE"), ChestState.AVAILABLE);
                ChestRecord rec = manager.chestCoordinator().reportState(dim, cx, cy, cz, st, name, System.currentTimeMillis());
                broadcastToConnected(server, "chest_sync", GSON.toJsonTree(rec));
            }

            case "chest_lock" -> {
                int cx = body.has("x") ? body.get("x").getAsInt() : 0;
                int cy = body.has("y") ? body.get("y").getAsInt() : 0;
                int cz = body.has("z") ? body.get("z").getAsInt() : 0;
                boolean unlock = body.has("unlock") && body.get("unlock").getAsBoolean();
                if (unlock) {
                    manager.chestCoordinator().unlock(dim, cx, cy, cz, uuid);
                } else {
                    ChestCoordinator.LockResult res = manager.chestCoordinator().tryLock(
                            dim, cx, cy, cz, uuid, name, config.chestLockTtlSeconds * 1000L, System.currentTimeMillis());
                    JsonObject reply = new JsonObject();
                    reply.addProperty("x", cx);
                    reply.addProperty("y", cy);
                    reply.addProperty("z", cz);
                    reply.addProperty("granted", res.granted());
                    reply.addProperty("lockedBy", res.lockedByName() == null ? "" : res.lockedByName());
                    reply.addProperty("expiresAtMs", res.expiresAtMs());
                    send(player, "chest_lock_reply", reply);
                }
            }

            case "sleep_status" -> {
                String status = getStr(body, "status", "");
                SleepCoordinator.SleepOrder next = manager.sleepCoordinator()
                        .onSleepStatus(uuid, status, System.currentTimeMillis(), manager.botsView());
                if (next != null) {
                    sendSleepOrder(server, next);
                }
                broadcastSnapshot(server);
            }

            default -> {}
        }
    }

    private boolean isEditorAllowed(ServerPlayer player) {
        if (config.isEditorWhitelisted(player.getUUID(), player.getGameProfile().name())) {
            return true;
        }
        return player.permissions().hasPermission(new net.minecraft.server.permissions.Permission.HasCommandLevel(
                net.minecraft.server.permissions.PermissionLevel.GAMEMASTERS));
    }

    private void sendWorkerStop(MinecraftServer server, String botUuid, String jobId, String reason) {
        if (botUuid == null || botUuid.isBlank()) return;
        try {
            ServerPlayer target = server.getPlayerList().getPlayer(UUID.fromString(botUuid));
            if (target == null) return;
            JsonObject body = new JsonObject();
            body.addProperty("action", "stop");
            body.addProperty("jobId", jobId != null ? jobId : "");
            body.addProperty("reason", reason != null ? reason : "Stopped by Fleet Editor");
            send(target, "worker_command", body);
        } catch (IllegalArgumentException ignored) {
        }
    }

    private void sendSleepOrder(MinecraftServer server, SleepCoordinator.SleepOrder order) {
        ServerPlayer target = server.getPlayerList().getPlayer(UUID.fromString(order.botUuid()));
        if (target != null && ServerPlayNetworking.canSend(target, FleetPayload.TYPE)) {
            JsonObject body = new JsonObject();
            body.addProperty("sleepNow", true);
            body.add("bedPos", GSON.toJsonTree(order.bedPos()));
            body.addProperty("expiresAtMs", order.expiresAtMs());
            send(target, "sleep_order", body);
        }
    }

    private void broadcastSnapshot(MinecraftServer server) {
        JsonElement snap = GSON.toJsonTree(manager.snapshot());
        broadcastToConnected(server, "fleet_snapshot", snap);
    }

    private void broadcastToConnected(MinecraftServer server, String type, JsonElement body) {
        String encoded = encodeEnvelope(type, body);
        FleetPayload payload = new FleetPayload(encoded);
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            if (ServerPlayNetworking.canSend(player, FleetPayload.TYPE)) {
                ServerPlayNetworking.send(player, payload);
            }
        }
    }

    private void send(ServerPlayer player, String type, JsonElement body) {
        if (ServerPlayNetworking.canSend(player, FleetPayload.TYPE)) {
            ServerPlayNetworking.send(player, new FleetPayload(encodeEnvelope(type, body)));
        }
    }

    private void sendError(ServerPlayer player, String message) {
        JsonObject err = new JsonObject();
        err.addProperty("error", message);
        send(player, "error", err);
    }

    private static String encodeEnvelope(String type, JsonElement body) {
        JsonObject env = new JsonObject();
        env.addProperty("v", FleetModels.PROTOCOL_VERSION);
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
}
