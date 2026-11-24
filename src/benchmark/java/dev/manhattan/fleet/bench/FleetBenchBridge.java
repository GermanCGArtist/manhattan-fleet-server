package dev.manhattan.fleet.bench;

import dev.manhattan.fleet.server.FleetManager;
import dev.manhattan.fleet.server.FleetModels.*;
import dev.manhattan.fleet.server.FleetServerMod;
import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
import net.fabricmc.fabric.api.networking.v1.ServerPlayConnectionEvents;
import net.minecraft.core.BlockPos;
import net.minecraft.network.chat.Component;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.gamerules.GameRules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/**
 * Test-only server half of the Deming fleet scenarios. Inert unless {@code server.properties} names a
 * scenario in {@code manhattan-fleet-bench}.
 *
 * <ol>
 *   <li>Builds the terrain (a dirt box to clear, optionally a bedrock pen with a single 1-wide door), sets
 *       game rules (no drops, no mobs, keep inventory, noon), forceloads the area.</li>
 *   <li>Equips every joining player (Efficiency V tools) and makes {@code FBEditor} an operator (Fleet Editor).</li>
 *   <li>Once all expected bots are registered as fleet workers, says {@code [FleetBench] READY ... box=...};
 *       the editor client then submits the clear job through the normal editor path.</li>
 *   <li>Disturbs the run as the scenario says (kick or kill a worker 25 s after the job started).</li>
 *   <li>When the job ends (or times out) checks, on the server: every block of the box is air, every chunk is
 *       DONE exactly once, none FAILED. Says {@code [FleetBench] RESULT scenario=... pass=true|false ...}, which the
 *       clients turn into their {@code BENCHMARK_RESULT} line.</li>
 * </ol>
 */
public final class FleetBenchBridge implements ModInitializer {
    private static final Logger LOG = LoggerFactory.getLogger("manhattan-fleet-bench");
    private static final String EDITOR = "FBEditor";

    private record Scenario(String name, int bots, int[] box, String chaos, String chaosTarget, int timeoutSec, boolean pen) {}

    private static Scenario scenario(String name) {
        int[] clear9 = {8, -60, 8, 55, -58, 55};      // 48 x 48 x 3: 9 chunks of 16
        int[] clear16 = {8, -60, 8, 71, -59, 71};     // 64 x 64 x 2: 16 chunks
        int[] east4 = {24, -60, -16, 55, -59, 15};    // 32 x 32 x 2 east of the pen: 4 chunks
        return switch (name) {
            case "fleet_clear_4" -> new Scenario(name, 4, clear9, "", "", 900, false);
            case "fleet_clear_8" -> new Scenario(name, 8, clear16, "", "", 900, false);
            case "fleet_kick_4" -> new Scenario(name, 4, clear9, "kick", "FBWorker2", 1200, false);
            case "fleet_death_4" -> new Scenario(name, 4, clear9, "kill", "FBWorker1", 1200, false);
            case "fleet_doorway_4" -> new Scenario(name, 4, east4, "", "", 900, true);
            default -> null;
        };
    }

    private Scenario sc;
    private int ticks;
    private boolean terrainDone;
    private boolean ready;
    private long readySaidMs;
    private String jobId;
    private long jobStartMs;
    private boolean chaosDone;
    private String result;
    private long resultSaidMs;
    private int resultRepeats;
    private final Set<String> equipped = new HashSet<>();
    /** Players to set up, with the tick they joined (set up 2 s later, once they are fully in the world). */
    private final java.util.Map<String, Integer> pendingSetup = new java.util.LinkedHashMap<>();

    @Override
    public void onInitialize() {
        String name = readScenarioName();
        if (name == null || name.isBlank()) return;
        sc = scenario(name.trim());
        if (sc == null) {
            LOG.error("[FleetBench] Unknown scenario '{}'", name);
            return;
        }
        LOG.info("[FleetBench] Scenario {}: {} bots, box {},{},{} .. {},{},{}{}", sc.name(), sc.bots(),
                sc.box()[0], sc.box()[1], sc.box()[2], sc.box()[3], sc.box()[4], sc.box()[5],
                sc.chaos().isBlank() ? "" : ", chaos " + sc.chaos() + " " + sc.chaosTarget());
        ServerLifecycleEvents.SERVER_STARTED.register(this::setUpWorld);
        ServerPlayConnectionEvents.JOIN.register((handler, sender, server) ->
                server.execute(() -> onJoin(server, handler.getPlayer())));
        ServerTickEvents.END_SERVER_TICK.register(this::onTick);
    }

    private static String readScenarioName() {
        Path props = Path.of("server.properties");
        if (!Files.exists(props)) return null;
        Properties p = new Properties();
        try (Reader r = Files.newBufferedReader(props, StandardCharsets.UTF_8)) {
            p.load(r);
        } catch (IOException e) {
            LOG.error("[FleetBench] Cannot read server.properties: {}", e.toString());
            return null;
        }
        return p.getProperty("manhattan-fleet-bench");
    }

    private void run(MinecraftServer server, String cmd) {
        LOG.info("[FleetBench] /{}", cmd);
        server.getCommands().performPrefixedCommand(server.createCommandSourceStack(), cmd); // feedback and errors go to the server log
    }

    private void setUpWorld(MinecraftServer server) {
        ServerLevel ow = server.overworld();
        GameRules gr = server.getGameRules();
        gr.set(GameRules.BLOCK_DROPS, false, server);
        gr.set(GameRules.SPAWN_MOBS, false, server);
        gr.set(GameRules.KEEP_INVENTORY, true, server);
        gr.set(GameRules.ADVANCE_TIME, false, server);
        run(server, "difficulty peaceful");
        run(server, "time set noon");
        run(server, "weather clear 1000000");
        run(server, "setworldspawn 0 -60 0");
        int[] b = sc.box();
        run(server, String.format(Locale.ROOT, "forceload add %d %d %d %d",
                Math.min(-16, b[0] - 16), Math.min(-16, b[2] - 16), Math.max(16, b[3] + 16), Math.max(16, b[5] + 16)));
        LOG.info("[FleetBench] World set up in {}", ow.dimension().identifier());
    }

    private void buildTerrain(MinecraftServer server) {
        int[] b = sc.box();
        run(server, String.format(Locale.ROOT, "fill %d %d %d %d %d %d dirt", b[0], b[1], b[2], b[3], b[4], b[5]));
        if (sc.pen()) {
            run(server, "fill -6 -60 -6 6 -58 -6 bedrock");
            run(server, "fill -6 -60 6 6 -58 6 bedrock");
            run(server, "fill -6 -60 -6 -6 -58 6 bedrock");
            run(server, "fill 6 -60 -6 6 -58 6 bedrock");
            run(server, "fill 6 -60 0 6 -59 0 air");
        }
        terrainDone = true;
        LOG.info("[FleetBench] Terrain built ({} solid blocks in the box)", solidInBox(server));
    }

    private void onJoin(MinecraftServer server, ServerPlayer p) {
        String name = p.getGameProfile().name();
        if (EDITOR.equals(name) && !server.getPlayerList().isOp(p.nameAndId())) {
            server.getPlayerList().op(p.nameAndId());
            LOG.info("[FleetBench] {} is operator (Fleet Editor).", name);
        }
        pendingSetup.put(name, ticks);
    }

    private void setUpPlayer(MinecraftServer server, String name) {
        if (server.getPlayerList().getPlayerByName(name) == null) return;
        if (equipped.add(name)) {
            int slot = equipped.size() - 1;
            double x = sc.pen() ? -3.5 + (slot % 4) * 2 : -6.5 + (slot % 4) * 2;
            double z = sc.pen() ? -3.5 + (slot / 4) * 2 : -6.5 + (slot / 4) * 2;
            run(server, "gamemode survival " + name);
            run(server, "clear " + name);
            run(server, String.format(Locale.ROOT, "tp %s %.1f -60 %.1f", name, x, z));
            run(server, "give " + name + " diamond_shovel[enchantments={efficiency:5,unbreaking:3}] 1");
            run(server, "give " + name + " diamond_pickaxe[enchantments={efficiency:5,unbreaking:3}] 1");
            run(server, "give " + name + " cooked_beef 32");
            LOG.info("[FleetBench] Equipped {}", name);
        }
    }

    private void onTick(MinecraftServer server) {
        if (sc == null) return;
        ++ticks;
        if (!pendingSetup.isEmpty()) {
            for (var it = pendingSetup.entrySet().iterator(); it.hasNext(); ) {
                var e = it.next();
                if (ticks - e.getValue() >= 40) {
                    setUpPlayer(server, e.getKey());
                    it.remove();
                }
            }
        }
        if (ticks % 20 != 0) return;
        long now = System.currentTimeMillis();
        if (!terrainDone && ticks >= 40) buildTerrain(server);
        FleetServerMod mod = FleetServerMod.instance();
        if (mod == null || mod.manager() == null) return;
        FleetManager fm = mod.manager();

        if (result != null) {
            if (resultRepeats < 30 && now - resultSaidMs >= 4_000L) {
                say(server, result);
                resultSaidMs = now;
                resultRepeats++;
            }
            return;
        }

        if (jobId == null) {
            for (FleetJob j : fm.jobsView().values()) {
                jobId = j.jobId;
                jobStartMs = now;
                LOG.info("[FleetBench] Job {} submitted: {} chunks", j.jobId, j.chunks.size());
                break;
            }
        }
        if (jobId == null) {
            int workers = 0;
            for (BotRecord bot : fm.botsView()) {
                if (bot.online && (bot.mode == ClientMode.WORKER || bot.mode == ClientMode.BOTH)) workers++;
            }
            if (terrainDone && workers >= sc.bots() && now - readySaidMs >= 10_000L) {
                int[] b = sc.box();
                say(server, String.format(Locale.ROOT, "[FleetBench] READY scenario=%s bots=%d box=%d,%d,%d,%d,%d,%d",
                        sc.name(), sc.bots(), b[0], b[1], b[2], b[3], b[4], b[5]));
                readySaidMs = now;
                ready = true;
            }
            if (ticks > 20 * 600 && !ready) {
                finish(server, false, "only " + workers + " of " + sc.bots() + " bots registered as fleet workers");
            }
            return;
        }

        FleetJob job = fm.jobsView().get(jobId);
        if (job == null) {
            finish(server, false, "job " + jobId + " vanished");
            return;
        }
        long elapsed = (now - jobStartMs) / 1000L;
        if (!chaosDone && !sc.chaos().isBlank() && elapsed >= 25) {
            chaosDone = true;
            String target = sc.chaosTarget();
            if (server.getPlayerList().getPlayerByName(target) == null) {
                LOG.warn("[FleetBench] chaos target {} is not online", target);
            } else if ("kick".equals(sc.chaos())) {
                LOG.info("[FleetBench] CHAOS: kicking {}", target);
                run(server, "kick " + target + " FleetBench chaos: dropped connection");
            } else if ("kill".equals(sc.chaos())) {
                LOG.info("[FleetBench] CHAOS: killing {}", target);
                run(server, "kill " + target);
            }
        }
        if (elapsed % 30 == 0) {
            int done = 0, claimed = 0;
            for (ChunkEntry ce : job.chunks.values()) {
                if (ce.state == ChunkState.DONE) done++;
                if (ce.state == ChunkState.CLAIMED || ce.state == ChunkState.DISCONNECTED_GRACE) claimed++;
            }
            LOG.info("[FleetBench] t+{}s: {}/{} chunks done, {} claimed, {} solid blocks left", elapsed, done,
                    job.chunks.size(), claimed, solidInBox(server));
        }
        if (job.status == JobStatus.DONE) {
            verify(server, job, elapsed);
        } else if (elapsed > sc.timeoutSec()) {
            finish(server, false, "timeout after " + elapsed + " s: " + summary(job) + ", " + solidInBox(server) + " solid left");
        }
    }

    private void verify(MinecraftServer server, FleetJob job, long elapsed) {
        List<String> problems = new ArrayList<>();
        int doneTwice = 0;
        for (ChunkEntry ce : job.chunks.values()) {
            if (ce.state != ChunkState.DONE) problems.add(ce.chunkKey + " " + ce.state + " (" + ce.reason + ")");
            if (ce.doneCount > 1) doneTwice++;
        }
        if (doneTwice > 0) problems.add(doneTwice + " chunk(s) marked DONE more than once");
        int solid = solidInBox(server);
        if (solid > 0) problems.add(solid + " block(s) still in the box");
        String detail = String.format(Locale.ROOT, "chunks=%d secs=%d bots=%d %s", job.chunks.size(), elapsed,
                sc.bots(), problems.isEmpty() ? "ok" : String.join("; ", problems));
        finish(server, problems.isEmpty(), detail);
    }

    private void finish(MinecraftServer server, boolean pass, String detail) {
        result = String.format(Locale.ROOT, "[FleetBench] RESULT scenario=%s pass=%s %s", sc.name(), pass, detail);
        LOG.info("{}", result);
        System.out.println("FLEET_BENCH_RESULT scenario=" + sc.name() + " pass=" + pass + " " + detail);
        resultSaidMs = 0L;
    }

    private static String summary(FleetJob job) {
        int[] n = new int[ChunkState.values().length];
        for (ChunkEntry ce : job.chunks.values()) n[ce.state.ordinal()]++;
        StringBuilder sb = new StringBuilder();
        for (ChunkState s : ChunkState.values()) if (n[s.ordinal()] > 0) sb.append(s).append('=').append(n[s.ordinal()]).append(' ');
        return sb.toString().trim();
    }

    private int solidInBox(MinecraftServer server) {
        ServerLevel ow = server.overworld();
        int[] b = sc.box();
        int n = 0;
        BlockPos.MutableBlockPos m = new BlockPos.MutableBlockPos();
        for (int x = b[0]; x <= b[3]; x++) {
            for (int y = b[1]; y <= b[4]; y++) {
                for (int z = b[2]; z <= b[5]; z++) {
                    m.set(x, y, z);
                    if (!ow.getBlockState(m).isAir()) n++;
                }
            }
        }
        return n;
    }

    private static void say(MinecraftServer server, String msg) {
        server.getPlayerList().broadcastSystemMessage(Component.literal(msg), false);
    }
}
