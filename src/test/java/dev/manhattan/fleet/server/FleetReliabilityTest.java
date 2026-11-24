package dev.manhattan.fleet.server;

import dev.manhattan.fleet.server.FleetModels.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Protocol v2 reliability tests: lease epochs, acknowledged and idempotent reports, safe persistence, owner
 * index, hazards, stock reservations, and a seeded chaos simulation of a whole fleet over a lossy network.
 */
final class FleetReliabilityTest {
    private static int passed;

    private FleetReliabilityTest() {}

    static int runAll() throws Exception {
        passed = 0;
        testWireContractMatchesClient();
        testStaleEpochRejectedAfterLeaseExpiry();
        testDuplicateReportIsIdempotent();
        testExpiredLeaseStillReportableWhenUntaken();
        testGraceInheritanceKeepsEpochPreemptionRaisesIt();
        testHeartbeatWithOldEpochDoesNotRenew();
        testCorruptStateFallsBackToBackupAndIsKept();
        testVersionOneStateIsReadAndMigrated();
        testMissingProjectPausesJob();
        testBotsHazardsAndStockSurviveRestart();
        testConfigReloadApplies();
        testHazardDedupeAndCap();
        testStockReservations();
        testClaimThroughputWithManyBotsAndChunks();
        for (long seed = 1; seed <= 12; seed++) {
            testChaosSimulation(seed, 8, 120);
        }
        testChaosSimulation(99, 32, 400);
        System.out.println("FleetReliabilityTest: " + passed + " tests passed.");
        return passed;
    }

    // ------------------------------------------------------------------ contract

    /** The wire contract must be byte-identical in both repositories (when the sibling checkout exists). */
    private static void testWireContractMatchesClient() throws IOException {
        Path mine = Path.of("src/main/java/dev/manhattan/fleet/wire/FleetWire.java");
        Path theirs = Path.of("../Manhattan/src/main/java/dev/manhattan/fleet/wire/FleetWire.java");
        check(Files.exists(mine), "FleetWire.java must exist");
        if (Files.exists(theirs)) {
            String a = Files.readString(mine, StandardCharsets.UTF_8).replace("\r\n", "\n");
            String b = Files.readString(theirs, StandardCharsets.UTF_8).replace("\r\n", "\n");
            check(a.equals(b), "FleetWire.java differs between manhattan-fleet-server and Manhattan; copy it over");
        } else {
            System.out.println("  (no ../Manhattan checkout: wire contract comparison skipped)");
        }
        passed++;
    }

    // ------------------------------------------------------------------ epochs & reports

    private static FleetManager oneJob(AtomicLong clock, int chunks, String... bots) {
        FleetManager fm = new FleetManager(new FleetConfig(), clock::get);
        for (int i = 0; i < bots.length; i++) {
            fm.registerBot("u" + i, bots[i], ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
        }
        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 64;
        p.maxDistance = 0;
        List<ChunkSpec> specs = new ArrayList<>();
        for (int i = 0; i < chunks; i++) specs.add(new ChunkSpec("t/" + (i * 64) + ",0", i * 4, 0, i * 64, 0));
        fm.submitJob("p", "P", "{\"id\":\"p\"}", "minecraft:overworld", p, specs, List.of(), "Ed");
        return fm;
    }

    private static void testStaleEpochRejectedAfterLeaseExpiry() {
        AtomicLong clock = new AtomicLong(10_000_000L);
        FleetManager fm = oneJob(clock, 1, "A", "B");
        WorkGrant a = fm.claimWork("u0", "minecraft:overworld", 0, 64, 0, null);
        check(!a.idle && a.leaseEpoch == 1, "A gets epoch 1");
        clock.addAndGet(31_000L); // lease (30 s) runs out without a heartbeat
        fm.tick(clock.get());
        WorkGrant b = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        check(!b.idle && b.leaseEpoch == 2, "B takes the chunk with epoch 2");
        ReportResult late = fm.reportChunk("u0", a.jobId, a.chunkKey, ChunkState.DONE, "", a.leaseEpoch, "m-a");
        check(!late.accepted(), "A's late DONE must be rejected: " + late);
        ReportResult ok = fm.reportChunk("u1", b.jobId, b.chunkKey, ChunkState.DONE, "", b.leaseEpoch, "m-b");
        check(ok.accepted(), "B's DONE must be accepted");
        check(fm.jobsView().get(a.jobId).chunks.get(a.chunkKey).doneCount == 1, "exactly one DONE");
        check(fm.verifyOwnerIndex() == null, "index consistent");
        passed++;
    }

    private static void testDuplicateReportIsIdempotent() {
        AtomicLong clock = new AtomicLong(11_000_000L);
        FleetManager fm = oneJob(clock, 2, "A");
        WorkGrant g = fm.claimWork("u0", "minecraft:overworld", 0, 64, 0, null);
        ReportResult r1 = fm.reportChunk("u0", g.jobId, g.chunkKey, ChunkState.DEFERRED, "x", g.leaseEpoch, "m1");
        ReportResult r2 = fm.reportChunk("u0", g.jobId, g.chunkKey, ChunkState.DEFERRED, "x", g.leaseEpoch, "m1");
        check(r1.accepted() && !r1.duplicate(), "first applied");
        check(r2.accepted() && r2.duplicate(), "resend answered as duplicate");
        check(fm.jobsView().get(g.jobId).chunks.get(g.chunkKey).tries == 1, "resend must not count as a try");
        passed++;
    }

    private static void testExpiredLeaseStillReportableWhenUntaken() {
        AtomicLong clock = new AtomicLong(12_000_000L);
        FleetManager fm = oneJob(clock, 1, "A");
        WorkGrant g = fm.claimWork("u0", "minecraft:overworld", 0, 64, 0, null);
        clock.addAndGet(45_000L);
        fm.tick(clock.get());
        check(fm.jobsView().get(g.jobId).chunks.get(g.chunkKey).state == ChunkState.PENDING, "lease lapsed");
        ReportResult r = fm.reportChunk("u0", g.jobId, g.chunkKey, ChunkState.DONE, "", g.leaseEpoch, "m1");
        check(r.accepted(), "work finished on a lapsed but untaken lease counts");
        check(fm.jobsView().get(g.jobId).status == JobStatus.DONE, "job done");
        passed++;
    }

    private static void testGraceInheritanceKeepsEpochPreemptionRaisesIt() {
        AtomicLong clock = new AtomicLong(13_000_000L);
        FleetManager fm = oneJob(clock, 1, "A", "B");
        WorkGrant g = fm.claimWork("u0", "minecraft:overworld", 0, 64, 0, null);
        fm.disconnectBot("u0");
        fm.registerBot("u0", "A", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
        WorkGrant again = fm.claimWork("u0", "minecraft:overworld", 0, 64, 0, null);
        check(again.inheritedFromGrace && again.leaseEpoch == g.leaseEpoch, "rejoin keeps the epoch");
        fm.disconnectBot("u0");
        WorkGrant stolen = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        check(!stolen.idle && stolen.leaseEpoch == g.leaseEpoch + 1, "pre-emption raises the epoch");
        check(fm.verifyOwnerIndex() == null, "index after pre-emption: " + fm.verifyOwnerIndex());
        ReportResult r = fm.reportChunk("u0", g.jobId, g.chunkKey, ChunkState.DONE, "", g.leaseEpoch, "m1");
        check(!r.accepted(), "old owner cannot report after pre-emption");
        passed++;
    }

    private static void testHeartbeatWithOldEpochDoesNotRenew() {
        AtomicLong clock = new AtomicLong(14_000_000L);
        FleetManager fm = oneJob(clock, 1, "A");
        WorkGrant g = fm.claimWork("u0", "minecraft:overworld", 0, 64, 0, null);
        ChunkEntry ce = fm.jobsView().get(g.jobId).chunks.get(g.chunkKey);
        long before = ce.leaseExpiresAtMs;
        clock.addAndGet(10_000L);
        fm.updateHeartbeat("u0", "minecraft:overworld", 0, 64, 0, 20, 20, 30, false, g.jobId, g.chunkKey,
                "WORKING", "", null, g.leaseEpoch + 7);
        check(ce.leaseExpiresAtMs == before, "wrong epoch must not renew");
        fm.updateHeartbeat("u0", "minecraft:overworld", 0, 64, 0, 20, 20, 30, false, g.jobId, g.chunkKey,
                "WORKING", "", null, g.leaseEpoch);
        check(ce.leaseExpiresAtMs > before, "right epoch renews");
        passed++;
    }

    // ------------------------------------------------------------------ persistence

    private static void testCorruptStateFallsBackToBackupAndIsKept() throws Exception {
        Path dir = Files.createTempDirectory("fleet-corrupt-");
        try {
            Path state = dir.resolve("manhattan-fleet.json");
            AtomicLong clock = new AtomicLong(20_000_000L);
            FleetManager fm = oneJob(clock, 3, "A");
            FleetStore store = new FleetStore(state);
            for (FleetJob j : fm.drainUnsavedProjects()) store.saveProjectIfAbsent(j.projectHash, j.projectJson);
            store.saveNowAndFlush(fm.exportState()); // first write: no backup yet
            store.saveNowAndFlush(fm.exportState()); // second write: the first becomes .bak
            store.close();
            Files.writeString(state, "{ this is not json", StandardCharsets.UTF_8);

            FleetStore s2 = new FleetStore(state);
            FleetStore.LoadResult res = s2.load();
            s2.close();
            check(res.ok() && res.state().jobs.size() == 1, "backup must be restored: " + res.message());
            check(countFiles(dir, ".corrupt-") == 1, "the corrupt file must be kept aside");

            // both unreadable: start empty, keep both files, never overwrite
            Files.writeString(state, "garbage", StandardCharsets.UTF_8);
            Files.writeString(dir.resolve("manhattan-fleet.json.bak"), "garbage too", StandardCharsets.UTF_8);
            FleetStore s3 = new FleetStore(state);
            FleetStore.LoadResult res3 = s3.load();
            s3.close();
            check(!res3.ok() && res3.state().jobs.isEmpty(), "unreadable everywhere -> not ok, empty");
            check(countFiles(dir, ".corrupt-") == 3, "all unreadable files kept");
        } finally {
            deleteTree(dir);
        }
        passed++;
    }

    private static void testVersionOneStateIsReadAndMigrated() throws Exception {
        Path dir = Files.createTempDirectory("fleet-v1-");
        try {
            Path state = dir.resolve("manhattan-fleet.json");
            String v1 = "{\"version\":1,\"jobs\":[{\"jobId\":\"job_007\",\"projectId\":\"p\",\"projectName\":\"Old\","
                    + "\"projectHash\":\"abc123\",\"projectJson\":\"{\\\"id\\\":\\\"p\\\"}\",\"status\":\"ACTIVE\","
                    + "\"chunks\":{\"t/0,0\":{\"chunkKey\":\"t/0,0\",\"state\":\"DONE\"},"
                    + "\"t/16,0\":{\"chunkKey\":\"t/16,0\",\"state\":\"PENDING\"}}}]}";
            Files.writeString(state, v1, StandardCharsets.UTF_8);
            FleetStore st = new FleetStore(state);
            FleetStore.LoadResult res = st.load();
            st.flush();
            check(res.ok() && res.state().jobs.size() == 1, "v1 file loads");
            FleetJob j = res.state().jobs.get(0);
            check("{\"id\":\"p\"}".equals(j.projectJson), "v1 inline project JSON is kept");
            check(Files.exists(st.projectPath("abc123")), "v1 project is written to its own file");
            FleetManager fm = new FleetManager(new FleetConfig(), () -> 1L);
            fm.importState(res.state());
            st.saveNowAndFlush(fm.exportState());
            st.close();
            String written = Files.readString(state, StandardCharsets.UTF_8);
            check(!written.contains("projectJson"), "new state file no longer carries project JSON");
            check(fm.submitJob("q", "Q", "{}", null, null, List.of(new ChunkSpec("t/0,0", 0, 0, 0, 0)), List.of(), "e")
                    .jobId.equals("job_008"), "job numbering continues after the loaded jobs");
        } finally {
            deleteTree(dir);
        }
        passed++;
    }

    private static void testMissingProjectPausesJob() throws Exception {
        Path dir = Files.createTempDirectory("fleet-missing-");
        try {
            Path state = dir.resolve("manhattan-fleet.json");
            AtomicLong clock = new AtomicLong(21_000_000L);
            FleetManager fm = oneJob(clock, 2, "A");
            FleetStore store = new FleetStore(state);
            fm.drainUnsavedProjects(); // project deliberately never written
            store.saveNowAndFlush(fm.exportState());
            store.close();
            FleetStore s2 = new FleetStore(state);
            FleetStore.LoadResult res = s2.load();
            s2.close();
            FleetJob j = res.state().jobs.get(0);
            check(j.status == JobStatus.PAUSED && j.statusReason.contains("missing"), "job paused: " + j.statusReason);
            FleetManager fm2 = new FleetManager(new FleetConfig(), clock::get);
            fm2.importState(res.state());
            fm2.registerBot("u0", "A", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
            check(!fm2.resumeJob(j.jobId), "cannot resume a job without its project");
            check(fm2.claimWork("u0", "minecraft:overworld", 0, 64, 0, null).idle, "nothing to hand out");
        } finally {
            deleteTree(dir);
        }
        passed++;
    }

    private static void testBotsHazardsAndStockSurviveRestart() throws Exception {
        Path dir = Files.createTempDirectory("fleet-bots-");
        try {
            AtomicLong clock = new AtomicLong(22_000_000L);
            FleetManager fm = oneJob(clock, 2, "A", "B");
            fm.stopWorker("u1", true);
            fm.reportHazard("A", "minecraft:overworld", 5, 60, 5, "death");
            fm.chestCoordinator().reportStock("minecraft:overworld", 1, 2, 3, Map.of("minecraft:dirt", 64), "u0", "A",
                    clock.get());
            FleetStore store = new FleetStore(dir.resolve("manhattan-fleet.json"));
            for (FleetJob j : fm.drainUnsavedProjects()) store.saveProjectIfAbsent(j.projectHash, j.projectJson);
            store.saveNowAndFlush(fm.exportState());
            FleetManager fm2 = new FleetManager(new FleetConfig(), clock::get);
            fm2.importState(store.load().state());
            store.close();
            check(fm2.bot("u1") != null && fm2.bot("u1").held && !fm2.bot("u1").online, "held bot comes back held, offline");
            check(fm2.hazards().size() == 1, "hazard survives");
            ChestRecord c = fm2.chestCoordinator().get("minecraft:overworld", 1, 2, 3);
            check(c != null && c.stock.get("minecraft:dirt") == 64, "stock survives");
            fm2.registerBot("u1", "B", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
            check(fm2.claimWork("u1", "minecraft:overworld", 0, 64, 0, null).idle, "held bot stays held after rejoin");
        } finally {
            deleteTree(dir);
        }
        passed++;
    }

    private static void testConfigReloadApplies() {
        AtomicLong clock = new AtomicLong(23_000_000L);
        FleetManager fm = oneJob(clock, 1, "A");
        FleetConfig c = new FleetConfig();
        c.leaseTtlSeconds = 120;
        fm.updateConfig(c);
        WorkGrant g = fm.claimWork("u0", "minecraft:overworld", 0, 64, 0, null);
        check(g.leaseExpiresAtMs - clock.get() == 120_000L, "reloaded lease TTL must apply");
        passed++;
    }

    private static void testHazardDedupeAndCap() {
        AtomicLong clock = new AtomicLong(24_000_000L);
        FleetManager fm = new FleetManager(new FleetConfig(), clock::get);
        check(fm.reportHazard("A", "minecraft:overworld", 0, 60, 0, "death") != null, "first hazard");
        check(fm.reportHazard("B", "minecraft:overworld", 1, 61, 1, "death") == null, "same spot within a minute merges");
        for (int i = 0; i < 300; i++) {
            clock.addAndGet(1_000L);
            fm.reportHazard("A", "minecraft:overworld", i * 10, 60, 0, "death");
        }
        check(fm.hazards().size() <= dev.manhattan.fleet.wire.FleetWire.MAX_HAZARDS, "hazards capped");
        clock.addAndGet(dev.manhattan.fleet.wire.FleetWire.HAZARD_TTL_MS + 1);
        check(fm.hazards().isEmpty(), "hazards expire");
        passed++;
    }

    private static void testStockReservations() {
        ChestCoordinator cc = new ChestCoordinator();
        String ow = "minecraft:overworld";
        ChestCoordinator.ReserveResult unseen = cc.reserve(ow, 0, 0, 0, "minecraft:dirt", 10, "u1", "A", 60_000L, 1_000L);
        check(unseen.granted() == 0 && unseen.available() == -1, "unseen chest: go and look");
        cc.reportStock(ow, 0, 0, 0, Map.of("minecraft:dirt", 100), "u1", "A", 2_000L);
        ChestCoordinator.ReserveResult a = cc.reserve(ow, 0, 0, 0, "minecraft:dirt", 70, "u1", "A", 60_000L, 3_000L);
        ChestCoordinator.ReserveResult b = cc.reserve(ow, 0, 0, 0, "minecraft:dirt", 70, "u2", "B", 60_000L, 3_000L);
        check(a.granted() == 70 && b.granted() == 30, "second bot only gets what is left: " + b);
        ChestCoordinator.ReserveResult c = cc.reserve(ow, 0, 0, 0, "minecraft:dirt", 10, "u3", "C", 60_000L, 3_000L);
        check(c.granted() == 0 && c.available() == 0, "nothing left for a third");
        cc.expireReservations(70_000L);
        ChestCoordinator.ReserveResult d = cc.reserve(ow, 0, 0, 0, "minecraft:dirt", 10, "u3", "C", 60_000L, 70_000L);
        check(d.granted() == 10, "lapsed reservations free the stock");
        cc.reportStock(ow, 0, 0, 0, Map.of(), "u3", "C", 71_000L);
        check(cc.get(ow, 0, 0, 0).state == ChestState.EMPTY && cc.get(ow, 0, 0, 0).reservations.isEmpty(),
                "an empty report empties the chest and settles the reporter's reservation");
        cc.reportStock(ow, 0, 0, 0, Map.of("minecraft:dirt", 5), "u1", "A", 72_000L);
        cc.reserve(ow, 0, 0, 0, "minecraft:dirt", 5, "u2", "B", 60_000L, 72_000L);
        check(cc.dropReservationsOf("u2") == 1, "a dead bot's reservations go");
        passed++;
    }

    private static void testClaimThroughputWithManyBotsAndChunks() {
        AtomicLong clock = new AtomicLong(30_000_000L);
        FleetManager fm = new FleetManager(new FleetConfig(), clock::get);
        int bots = 32;
        for (int i = 0; i < bots; i++) {
            fm.registerBot("u" + i, "B" + i, ClientMode.WORKER, List.of(), null, "minecraft:overworld", i * 40, 64, 0);
        }
        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = bots;
        p.maxDistance = 0;
        List<ChunkSpec> specs = new ArrayList<>();
        for (int x = 0; x < 80; x++) {
            for (int z = 0; z < 75; z++) specs.add(new ChunkSpec("t/" + (x * 16) + "," + (z * 16), x, z, x * 16 + 8, z * 16 + 8));
        }
        fm.submitJob("big", "Big", "{\"id\":\"big\"}", "minecraft:overworld", p, specs, List.of(), "Ed");
        long t0 = System.nanoTime();
        int cycles = 0;
        for (int round = 0; round < 60; round++) {
            for (int i = 0; i < bots; i++) {
                clock.addAndGet(5L);
                WorkGrant g = fm.claimWork("u" + i, "minecraft:overworld", i * 40, 64, 0, "x");
                if (g.idle) continue;
                fm.reportChunk("u" + i, g.jobId, g.chunkKey, ChunkState.DONE, "", g.leaseEpoch, "m" + round + "-" + i);
                cycles++;
            }
        }
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        System.out.println("  claim+report throughput: " + cycles + " cycles over 6000 chunks / 32 bots in " + ms + " ms");
        check(cycles == 60 * bots, "every bot gets work every round");
        check(ms < 20_000L, "claim+report must stay fast (" + ms + " ms)");
        passed++;
    }

    // ------------------------------------------------------------------ chaos simulation

    /** A message in flight: delivered at {@code atMs} (or dropped by the network before that). */
    private record Msg(long atMs, int seq, String to, String type, Object payload) {}

    private static final class SimBot {
        final String uuid;
        final String name;
        boolean online = true;
        long offlineUntil;
        long stalledUntil;
        WorkGrant grant;
        long workLeftMs;
        long nextHeartbeat;
        long claimSentAt = Long.MIN_VALUE;
        boolean claimInFlight;
        final LinkedHashMap<String, Report> outbox = new LinkedHashMap<>();
        final Set<String> reported = new HashSet<>(); // chunkKey#epoch already reported: ignore late leases for them
        int msgSeq;

        SimBot(int i) {
            uuid = "bot-" + i;
            name = "Bot" + i;
        }
    }

    private record Report(String msgId, String jobId, String chunkKey, ChunkState outcome, long epoch, long[] nextSend) {}

    /**
     * {@code bots} bots work a job of {@code chunks} chunks while the network drops 15%, duplicates 10% and
     * delays (reorders) messages, bots disconnect, die, stall past their lease, and the server restarts from
     * its files. At the end every chunk must be DONE exactly once, and the owner index must never drift.
     */
    private static void testChaosSimulation(long seed, int botCount, int chunks) throws Exception {
        Random rnd = new Random(seed);
        Path dir = Files.createTempDirectory("fleet-chaos-");
        try {
            AtomicLong clock = new AtomicLong(100_000_000L);
            FleetConfig cfg = new FleetConfig();
            cfg.maxChunkRetries = 1_000; // chaos defers a lot; it must never turn work into FAILED
            Path statePath = dir.resolve("manhattan-fleet.json");
            FleetStore store = new FleetStore(statePath);
            FleetManager[] fm = {new FleetManager(cfg, clock::get)};

            List<SimBot> bots = new ArrayList<>();
            for (int i = 0; i < botCount; i++) {
                SimBot b = new SimBot(i);
                bots.add(b);
                fm[0].registerBot(b.uuid, b.name, ClientMode.WORKER, List.of(), null, "minecraft:overworld",
                        rnd.nextInt(400), 64, rnd.nextInt(400));
            }
            AssignmentPolicy p = new AssignmentPolicy();
            p.maxBots = botCount;
            p.maxDistance = 0;
            List<ChunkSpec> specs = new ArrayList<>();
            int side = (int) Math.ceil(Math.sqrt(chunks));
            for (int i = 0; i < chunks; i++) {
                int cx = i % side, cz = i / side;
                specs.add(new ChunkSpec("t/" + (cx * 16) + "," + (cz * 16), cx, cz, cx * 16 + 8, cz * 16 + 8));
            }
            String jobId = fm[0].submitJob("chaos", "Chaos", "{\"id\":\"chaos\"}", "minecraft:overworld", p, specs,
                    List.of(), "Ed").jobId;

            PriorityQueue<Msg> net = new PriorityQueue<>(Comparator.comparingLong(Msg::atMs).thenComparingInt(Msg::seq));
            int[] seq = {0};
            Map<String, Integer> workFinished = new HashMap<>(); // chunk -> times some bot finished the work
            int staleRejected = 0;
            int restarts = 0;
            long nextRestart = clock.get() + 60_000L + rnd.nextInt(60_000);
            long deadline = clock.get() + 3L * 60L * 60_000L;

            while (fm[0].jobsView().get(jobId).status != JobStatus.DONE) {
                long now = clock.addAndGet(250L);
                check(now < deadline, "chaos seed " + seed + ": job did not finish within 3 simulated hours ("
                        + summary(fm[0].jobsView().get(jobId)) + ")");
                fm[0].tick(now);

                // --- server restart: everything in flight is lost, all bots reconnect later
                if (now >= nextRestart) {
                    restarts++;
                    nextRestart = now + 90_000L + rnd.nextInt(120_000);
                    for (FleetJob j : fm[0].drainUnsavedProjects()) store.saveProjectIfAbsent(j.projectHash, j.projectJson);
                    store.saveNowAndFlush(fm[0].exportState());
                    FleetManager reloaded = new FleetManager(cfg, clock::get);
                    reloaded.importState(store.load().state());
                    fm[0] = reloaded;
                    net.clear();
                    for (SimBot b : bots) {
                        b.online = false;
                        b.offlineUntil = now + 2_000L + rnd.nextInt(20_000);
                        b.grant = null;
                        b.claimInFlight = false;
                    }
                }
                FleetManager m = fm[0];

                // --- deliver due messages
                while (!net.isEmpty() && net.peek().atMs() <= now) {
                    Msg msg = net.poll();
                    if ("server".equals(msg.to())) {
                        SimBot from = (SimBot) ((Object[]) msg.payload())[0];
                        if (!from.online) continue; // its connection is gone
                        Object body = ((Object[]) msg.payload())[1];
                        switch (msg.type()) {
                            case "claim" -> {
                                WorkGrant g = m.claimWork(from.uuid, "minecraft:overworld", 0, 64, 0, "h");
                                sendTo(net, seq, rnd, now, from, "lease", g);
                            }
                            case "report" -> {
                                Report r = (Report) body;
                                ReportResult res = m.reportChunk(from.uuid, r.jobId(), r.chunkKey(), r.outcome(), "",
                                        r.epoch(), r.msgId());
                                if (!res.accepted()) staleRejected++;
                                sendTo(net, seq, rnd, now, from, "ack", r.msgId());
                                if (res.accepted() && !res.duplicate()) {
                                    sendTo(net, seq, rnd, now, from, "lease",
                                            m.claimWork(from.uuid, "minecraft:overworld", 0, 64, 0, "h"));
                                }
                            }
                            case "heartbeat" -> {
                                WorkGrant g = (WorkGrant) body;
                                m.updateHeartbeat(from.uuid, "minecraft:overworld", 0, 64, 0, 20, 20, 30, false,
                                        g == null ? null : g.jobId, g == null ? null : g.chunkKey, "WORKING", "", null,
                                        g == null ? 0L : g.leaseEpoch);
                            }
                            default -> {}
                        }
                    } else {
                        SimBot to = bots.get(Integer.parseInt(msg.to()));
                        if (!to.online) continue;
                        switch (msg.type()) {
                            case "lease" -> {
                                WorkGrant g = (WorkGrant) msg.payload();
                                to.claimInFlight = false;
                                if (g.idle || to.grant != null) break;
                                if (to.reported.contains(g.chunkKey + "#" + g.leaseEpoch)) break; // late copy
                                to.grant = g;
                                to.workLeftMs = 2_000L + rnd.nextInt(10_000);
                            }
                            case "ack" -> to.outbox.remove((String) msg.payload());
                            default -> {}
                        }
                    }
                }

                // --- bots act
                for (int i = 0; i < bots.size(); i++) {
                    SimBot b = bots.get(i);
                    if (!b.online) {
                        if (now >= b.offlineUntil) {
                            b.online = true;
                            m.registerBot(b.uuid, b.name, ClientMode.WORKER, List.of(), null, "minecraft:overworld",
                                    0, 64, 0);
                        }
                        continue;
                    }
                    double roll = rnd.nextDouble();
                    if (roll < 0.0008) { // disconnect
                        b.online = false;
                        b.offlineUntil = now + 5_000L + rnd.nextInt(80_000);
                        b.grant = null; // the real client drops its grant on disconnect; the outbox stays
                        b.claimInFlight = false;
                        m.disconnectBot(b.uuid);
                        continue;
                    }
                    if (roll < 0.0012) { // death: the server releases its chunks; the bot loses its task
                        m.onBotDeath(b.uuid);
                        b.grant = null;
                        b.claimInFlight = false;
                    }
                    if (roll > 0.9994) b.stalledUntil = now + 35_000L + rnd.nextInt(20_000); // stuck past the lease
                    if (now < b.stalledUntil) continue;

                    // resend unacknowledged reports every 3 s
                    for (Report r : b.outbox.values()) {
                        if (now >= r.nextSend()[0]) {
                            r.nextSend()[0] = now + 3_000L;
                            sendServer(net, seq, rnd, now, b, "report", r);
                        }
                    }
                    if (b.grant != null) {
                        if (now >= b.nextHeartbeat) {
                            b.nextHeartbeat = now + 3_000L;
                            sendServer(net, seq, rnd, now, b, "heartbeat", b.grant);
                        }
                        b.workLeftMs -= 250L;
                        if (b.workLeftMs <= 0) {
                            WorkGrant g = b.grant;
                            b.grant = null;
                            boolean defer = rnd.nextDouble() < 0.1;
                            if (!defer) workFinished.merge(g.chunkKey, 1, Integer::sum);
                            Report r = new Report(b.uuid + "-" + (++b.msgSeq), g.jobId, g.chunkKey,
                                    defer ? ChunkState.DEFERRED : ChunkState.DONE, g.leaseEpoch, new long[]{now + 3_000L});
                            b.reported.add(g.chunkKey + "#" + g.leaseEpoch);
                            b.outbox.put(r.msgId(), r);
                            sendServer(net, seq, rnd, now, b, "report", r);
                            b.claimInFlight = true;
                            b.claimSentAt = now;
                        }
                    } else if (!b.claimInFlight || now - b.claimSentAt > 4_000L) {
                        b.claimInFlight = true;
                        b.claimSentAt = now;
                        sendServer(net, seq, rnd, now, b, "claim", null);
                    }
                }

                String idx = m.verifyOwnerIndex();
                check(idx == null, "chaos seed " + seed + ": owner index drifted: " + idx);
            }

            FleetJob job = fm[0].jobsView().get(jobId);
            int doubles = 0;
            for (ChunkEntry ce : job.chunks.values()) {
                check(ce.state == ChunkState.DONE, "chaos seed " + seed + ": " + ce.chunkKey + " ended " + ce.state);
                check(ce.doneCount == 1, "chaos seed " + seed + ": " + ce.chunkKey + " DONE " + ce.doneCount + " times");
                if (workFinished.getOrDefault(ce.chunkKey, 0) > 1) doubles++;
            }
            System.out.println("  chaos seed " + seed + ": " + botCount + " bots, " + chunks + " chunks, " + restarts
                    + " restart(s), " + staleRejected + " stale/duplicate report(s) refused, " + doubles
                    + " chunk(s) finished twice (a lapsed lease was taken over), " + ((clock.get() - 100_000_000L) / 1000) + " s simulated");
            store.close();
        } finally {
            deleteTree(dir);
        }
        passed++;
    }

    private static void sendServer(PriorityQueue<Msg> net, int[] seq, Random rnd, long now, SimBot from,
                                   String type, Object body) {
        enqueue(net, seq, rnd, now, "server", type, new Object[]{from, body});
    }

    private static void sendTo(PriorityQueue<Msg> net, int[] seq, Random rnd, long now, SimBot to, String type,
                               Object body) {
        enqueue(net, seq, rnd, now, to.uuid.substring(4), type, body);
    }

    private static void enqueue(PriorityQueue<Msg> net, int[] seq, Random rnd, long now, String to, String type,
                                Object body) {
        if (rnd.nextDouble() < 0.15) return; // dropped
        int copies = rnd.nextDouble() < 0.10 ? 2 : 1; // duplicated
        for (int c = 0; c < copies; c++) {
            long delay = 50L + rnd.nextInt(rnd.nextDouble() < 0.1 ? 6_000 : 400); // some arrive much later
            net.add(new Msg(now + delay, seq[0]++, to, type, body));
        }
    }

    private static String summary(FleetJob job) {
        Map<ChunkState, Integer> n = new EnumMap<>(ChunkState.class);
        for (ChunkEntry ce : job.chunks.values()) n.merge(ce.state, 1, Integer::sum);
        return n.toString();
    }

    // ------------------------------------------------------------------ helpers

    private static long countFiles(Path dir, String contains) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(f -> f.getFileName().toString().contains(contains)).count();
        }
    }

    static void deleteTree(Path dir) {
        if (dir == null || !Files.exists(dir)) return;
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(f -> {
                try {
                    Files.deleteIfExists(f);
                } catch (IOException e) {
                    System.out.println("  could not delete " + f + ": " + e);
                }
            });
        } catch (IOException e) {
            System.out.println("  could not clean " + dir + ": " + e);
        }
    }

    private static void check(boolean cond, String msg) {
        if (!cond) throw new AssertionError("Assertion failed: " + msg);
    }
}
