package dev.manhattan.fleet.server;

import dev.manhattan.fleet.server.FleetModels.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Offline unit tests for {@link FleetManager}, {@link SleepCoordinator}, {@link ChestCoordinator},
 * and {@link FleetStore}. Run via {@code ./gradlew fleetTest}.
 */
public final class FleetServerTest {
    private static int passed = 0;

    public static void main(String[] args) throws Exception {
        testExplicitAndAutoPoolAssignment();
        testSpatialBufferAndRelaxation();
        testRejoinGraceInheritanceAndExpiry();
        testGracePreemptionWhenLastChunk();
        testBedPoolAssignment();
        testPersistenceRoundTrip();
        testSleepCoordinatorNearestAndFailover();
        testChestCoordinatorLocksAndSync();
        System.out.println("FleetServerTest: ALL " + passed + " tests passed.");
    }

    private static void testExplicitAndAutoPoolAssignment() {
        AtomicLong clock = new AtomicLong(1_000_000L);
        FleetManager fm = new FleetManager(new FleetConfig(), clock::get);

        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of("quarry"), null, "minecraft:overworld", 0, 64, 0);
        fm.registerBot("u2", "Bot2", ClientMode.WORKER, List.of("lumber"), null, "minecraft:overworld", 10, 64, 10);
        fm.registerBot("u3", "Bot3", ClientMode.WORKER, List.of("quarry"), null, "minecraft:overworld", 2000, 64, 2000);
        fm.registerBot("u4", "Editor1", ClientMode.EDITOR, List.of(), null, "minecraft:overworld", 0, 64, 0);

        AssignmentPolicy poolPolicy = new AssignmentPolicy();
        poolPolicy.mode = AssignmentMode.AUTO_POOL;
        poolPolicy.poolTag = "quarry";
        poolPolicy.maxBots = 2;
        poolPolicy.maxDistance = 500;

        List<ChunkSpec> chunks = List.of(
                new ChunkSpec("t_001/0,0", 0, 0, 16, 16),
                new ChunkSpec("t_001/64,0", 2, 0, 80, 16)
        );
        fm.submitJob("proj_quarry", "Quarry", "{\"id\":\"proj_quarry\"}", "minecraft:overworld",
                poolPolicy, chunks, List.of(), "Editor1");

        // Editor cannot claim work.
        assertTrue(fm.claimWork("u4", "minecraft:overworld", 0, 64, 0, null).idle, "Editor must stay idle");
        // Bot2 lacks "quarry" tag.
        assertTrue(fm.claimWork("u2", "minecraft:overworld", 10, 64, 10, null).idle, "Wrong tag must not claim");
        // Bot3 is > 500 blocks away.
        assertTrue(fm.claimWork("u3", "minecraft:overworld", 2000, 64, 2000, null).idle, "Too far must not claim");
        // Bot1 matches tag + distance.
        WorkGrant g1 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        assertTrue(!g1.idle && "t_001/0,0".equals(g1.chunkKey), "Bot1 should claim nearest chunk");
        assertTrue(g1.projectJson != null, "First grant must include projectJson");

        // Explicit job for Bot2 only.
        AssignmentPolicy explicit = new AssignmentPolicy();
        explicit.mode = AssignmentMode.EXPLICIT;
        explicit.explicitBots = List.of("Bot2");
        fm.submitJob("proj_wood", "Forest", "{\"id\":\"proj_wood\"}", "minecraft:overworld",
                explicit, List.of(new ChunkSpec("t_001/10,10", 0, 0, 10, 10)), List.of(), "Editor1");

        WorkGrant g2 = fm.claimWork("u2", "minecraft:overworld", 10, 64, 10, null);
        assertTrue(!g2.idle && "proj_wood".equals(g2.projectId), "Bot2 should claim explicit wood job");
        passed++;
    }

    private static void testSpatialBufferAndRelaxation() {
        AtomicLong clock = new AtomicLong(2_000_000L);
        FleetConfig cfg = new FleetConfig();
        cfg.spatialBufferChunks = 1;
        FleetManager fm = new FleetManager(cfg, clock::get);

        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
        fm.registerBot("u2", "Bot2", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);

        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 2;
        p.maxDistance = 0;
        List<ChunkSpec> chunks = List.of(
                new ChunkSpec("c0", 0, 0, 0, 0),
                new ChunkSpec("c1", 1, 0, 32, 0),   // adjacent to c0
                new ChunkSpec("c2", 2, 0, 64, 0)    // separated from c0 by 1 chunk
        );
        fm.submitJob("p1", "Dig", "{}", "minecraft:overworld", p, chunks, List.of(), "Admin");

        WorkGrant g1 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        assertTrue("c0".equals(g1.chunkKey), "Bot1 takes c0");

        // Bot2 is closer to c1, but c1 is within 1-chunk buffer of c0 -> must pick c2!
        WorkGrant g2 = fm.claimWork("u2", "minecraft:overworld", 20, 64, 0, null);
        assertTrue("c2".equals(g2.chunkKey), "Bot2 must respect 1-chunk spatial buffer and pick c2");

        // When Bot2 finishes c2 and only c1 remains, spatial buffer relaxes so c1 is claimed.
        fm.reportChunk("u2", g2.jobId, "c2", ChunkState.DONE, "");
        WorkGrant g3 = fm.claimWork("u2", "minecraft:overworld", 32, 64, 0, g2.projectHash);
        assertTrue("c1".equals(g3.chunkKey), "Spatial buffer must relax for final remaining chunk");
        assertTrue(g3.projectJson == null, "Cached projectHash should omit projectJson");
        passed++;
    }

    private static void testRejoinGraceInheritanceAndExpiry() {
        AtomicLong clock = new AtomicLong(3_000_000L);
        FleetConfig cfg = new FleetConfig();
        cfg.rejoinGraceSeconds = 60;
        FleetManager fm = new FleetManager(cfg, clock::get);

        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
        fm.registerBot("u2", "Bot2", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);

        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 2;
        p.maxDistance = 0;
        fm.submitJob("p1", "Dig", "{}", "minecraft:overworld", p, List.of(
                new ChunkSpec("c0", 0, 0, 0, 0),
                new ChunkSpec("c1", 2, 0, 64, 0)
        ), List.of(), "Admin");

        WorkGrant g1 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        assertTrue("c0".equals(g1.chunkKey), "Bot1 claims c0");

        // Bot1 disconnects -> c0 enters DISCONNECTED_GRACE for 60 s.
        fm.disconnectBot("u1");
        clock.addAndGet(30_000L);

        // Bot2 claims work -> takes c1 (does not steal c0 while c1 is available).
        WorkGrant g2 = fm.claimWork("u2", "minecraft:overworld", 0, 64, 0, null);
        assertTrue("c1".equals(g2.chunkKey), "Bot2 takes c1 while c0 is in grace");

        // Bot1 rejoins at 30 s -> inherits exact chunk c0!
        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 5, 64, 5);
        WorkGrant g1Rejoin = fm.claimWork("u1", "minecraft:overworld", 5, 64, 5, null);
        assertTrue("c0".equals(g1Rejoin.chunkKey) && g1Rejoin.inheritedFromGrace, "Bot1 must inherit c0 from grace");

        // Now disconnect Bot1 and let >60s pass -> c0 expires back to PENDING and Bot2 can take it.
        fm.disconnectBot("u1");
        fm.reportChunk("u2", g2.jobId, "c1", ChunkState.DONE, "");
        clock.addAndGet(65_000L);
        WorkGrant g2AfterExpiry = fm.claimWork("u2", "minecraft:overworld", 64, 64, 0, g2.projectHash);
        assertTrue("c0".equals(g2AfterExpiry.chunkKey) && !g2AfterExpiry.inheritedFromGrace,
                "Expired grace chunk must be reassigned to Bot2");
        passed++;
    }

    private static void testGracePreemptionWhenLastChunk() {
        AtomicLong clock = new AtomicLong(4_000_000L);
        FleetConfig cfg = new FleetConfig();
        cfg.rejoinGraceSeconds = 60;
        FleetManager fm = new FleetManager(cfg, clock::get);

        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
        fm.registerBot("u2", "Bot2", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);

        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 2;
        p.maxDistance = 0;
        fm.submitJob("p1", "Tiny", "{}", "minecraft:overworld", p, List.of(
                new ChunkSpec("c0", 0, 0, 0, 0)
        ), List.of(), "Admin");

        fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        fm.disconnectBot("u1");

        // Only 1 chunk exists and it's in DISCONNECTED_GRACE -> idle Bot2 preempts it so job doesn't stall.
        WorkGrant g2 = fm.claimWork("u2", "minecraft:overworld", 0, 64, 0, null);
        assertTrue(!g2.idle && "c0".equals(g2.chunkKey), "Idle peer must preempt last remaining grace chunk");
        passed++;
    }

    private static void testBedPoolAssignment() {
        AtomicLong clock = new AtomicLong(5_000_000L);
        FleetManager fm = new FleetManager(new FleetConfig(), clock::get);

        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
        fm.registerBot("u2", "Bot2", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);

        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 2;
        p.maxDistance = 0;
        List<int[]> beds = List.of(new int[]{10, 64, 10}, new int[]{12, 64, 10});
        fm.submitJob("p1", "Camp", "{}", "minecraft:overworld", p, List.of(
                new ChunkSpec("c0", 0, 0, 0, 0),
                new ChunkSpec("c1", 2, 0, 64, 0)
        ), beds, "Admin");

        WorkGrant g1 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        WorkGrant g2 = fm.claimWork("u2", "minecraft:overworld", 0, 64, 0, null);
        assertTrue(g1.assignedBed != null && g2.assignedBed != null, "Both bots must receive beds");
        assertTrue(g1.assignedBed[0] != g2.assignedBed[0], "Each bot must get a distinct bed from bedPool");
        passed++;
    }

    private static void testPersistenceRoundTrip() throws Exception {
        AtomicLong clock = new AtomicLong(6_000_000L);
        FleetManager fm = new FleetManager(new FleetConfig(), clock::get);
        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);

        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 2;
        p.maxDistance = 0;
        FleetJob job = fm.submitJob("p1", "Persist", "{\"id\":\"p1\"}", "minecraft:overworld", p, List.of(
                new ChunkSpec("c0", 0, 0, 0, 0),
                new ChunkSpec("c1", 2, 0, 64, 0)
        ), List.of(new int[]{1, 64, 1}), "Admin");
        fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);

        Path tmp = Files.createTempFile("manhattan-fleet-test-", ".json");
        try {
            FleetStore.save(tmp, fm.jobsView().values());
            FleetManager reloaded = new FleetManager(new FleetConfig(), clock::get);
            reloaded.loadPersistedJobs(FleetStore.load(tmp));
            FleetJob rJob = reloaded.jobsView().get(job.jobId);
            assertTrue(rJob != null && rJob.chunks.size() == 2, "Persisted job must reload cleanly");
            assertTrue(rJob.chunks.get("c0").state == ChunkState.DISCONNECTED_GRACE,
                    "Claimed chunk must become DISCONNECTED_GRACE after server restart");
        } finally {
            Files.deleteIfExists(tmp);
        }
        passed++;
    }

    private static void testSleepCoordinatorNearestAndFailover() {
        SleepCoordinator sc = new SleepCoordinator(60_000L);
        BotRecord far = new BotRecord();
        far.uuid = "u1"; far.name = "FarBot"; far.mode = ClientMode.WORKER; far.online = true;
        far.dimension = SleepCoordinator.OVERWORLD; far.x = 500; far.y = 64; far.z = 0;
        far.assignedBed = new int[]{0, 64, 0};

        BotRecord near = new BotRecord();
        near.uuid = "u2"; near.name = "NearBot"; near.mode = ClientMode.WORKER; near.online = true;
        near.dimension = SleepCoordinator.OVERWORLD; near.x = 12; near.y = 64; near.z = 0;
        near.assignedBed = new int[]{0, 64, 0};

        List<BotRecord> bots = List.of(far, near);
        SleepCoordinator.SleepOrder order1 = sc.tick(10_000L, true, bots);
        assertTrue(order1 != null && "u2".equals(order1.botUuid()), "Nearest bot to bed must be chosen");

        // Second tick during same night must not pick a second sleeper.
        assertTrue(sc.tick(15_000L, true, bots) == null, "Only one bot should be ordered to sleep");

        // NearBot fails -> immediate failover to FarBot.
        SleepCoordinator.SleepOrder order2 = sc.onSleepStatus("u2", "FAILED", 16_000L, bots);
        assertTrue(order2 != null && "u1".equals(order2.botUuid()), "Failover must select next-nearest bot");

        // Morning resets cycle.
        sc.tick(20_000L, false, bots);
        assertTrue(sc.currentOrder() == null, "Morning must clear sleep order");
        passed++;
    }

    private static void testChestCoordinatorLocksAndSync() {
        ChestCoordinator cc = new ChestCoordinator();
        cc.reportState("minecraft:overworld", 10, 64, 20, ChestState.EMPTY, "Bot1", 1000L);
        ChestRecord rec = cc.get("minecraft:overworld", 10, 64, 20);
        assertTrue(rec != null && rec.state == ChestState.EMPTY, "Chest state should sync");

        ChestCoordinator.LockResult l1 = cc.tryLock("minecraft:overworld", 10, 64, 20, "u1", "Bot1", 30_000L, 2000L);
        assertTrue(l1.granted(), "Bot1 should acquire lock");

        ChestCoordinator.LockResult l2 = cc.tryLock("minecraft:overworld", 10, 64, 20, "u2", "Bot2", 30_000L, 5000L);
        assertTrue(!l2.granted() && "Bot1".equals(l2.lockedByName()), "Bot2 must be blocked while Bot1 holds lock");

        // After TTL expires, Bot2 acquires lock.
        ChestCoordinator.LockResult l3 = cc.tryLock("minecraft:overworld", 10, 64, 20, "u2", "Bot2", 30_000L, 35_000L);
        assertTrue(l3.granted(), "Bot2 should acquire lock after TTL expiry");
        passed++;
    }

    private static void assertTrue(boolean cond, String msg) {
        if (!cond) {
            throw new AssertionError("Assertion failed: " + msg);
        }
    }
}
