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
        testTaskOrderAndTopDownYBandGating();
        testDeferredPriorityCooldownAndMaxRetries();
        testOneLayerLookaheadWhenTopBandClaimed();
        testPhaseSpilloverAndExplicitBarriers();
        testWorkerStopHoldReassignAndJobCancel();
        passed += FleetReliabilityTest.runAll();
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

        Path dir = Files.createTempDirectory("manhattan-fleet-test-");
        try {
            FleetStore store = new FleetStore(dir.resolve("manhattan-fleet.json"));
            for (FleetJob j : fm.drainUnsavedProjects()) store.saveProjectIfAbsent(j.projectHash, j.projectJson);
            store.saveNowAndFlush(fm.exportState());
            store.close();
            FleetManager reloaded = new FleetManager(new FleetConfig(), clock::get);
            FleetStore store2 = new FleetStore(dir.resolve("manhattan-fleet.json"));
            reloaded.importState(store2.load().state());
            store2.close();
            FleetJob rJob = reloaded.jobsView().get(job.jobId);
            assertTrue(rJob != null && rJob.chunks.size() == 2, "Persisted job must reload cleanly");
            assertTrue("{\"id\":\"p1\"}".equals(rJob.projectJson), "Project JSON must come back from its own file");
            assertTrue(rJob.chunks.get("c0").state == ChunkState.DISCONNECTED_GRACE,
                    "Claimed chunk must become DISCONNECTED_GRACE after server restart");
            assertTrue(reloaded.verifyOwnerIndex() == null, "Owner index must be rebuilt after a load");
        } finally {
            FleetReliabilityTest.deleteTree(dir);
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

    private static void testTaskOrderAndTopDownYBandGating() {
        AtomicLong clock = new AtomicLong(7_000_000L);
        FleetManager fm = new FleetManager(new FleetConfig(), clock::get);
        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);

        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 2;
        p.maxDistance = 0;

        // Step 1 is fill_water (further away at x=100), Step 2 is clear_area with 2 columns x 2 Y-bands.
        // Step 1 must finish first; then Step 2 must clear ALL y=64 chunks across both columns before ANY y=48!
        List<ChunkSpec> chunks = List.of(
                new ChunkSpec("fill_water/100,0", 3, 0, 100, 0),
                new ChunkSpec("clear_area/0,0,48", 0, 0, 0, 0),   // bottom band of col A (closer to bot!)
                new ChunkSpec("clear_area/0,0,64", 0, 0, 0, 0),   // top band of col A
                new ChunkSpec("clear_area/32,0,48", 1, 0, 32, 0), // bottom band of col B
                new ChunkSpec("clear_area/32,0,64", 1, 0, 32, 0)  // top band of col B
        );
        fm.submitJob("p1", "Layered", "{}", "minecraft:overworld", p, chunks, List.of(), "Admin");

        WorkGrant g1 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        assertTrue("fill_water/100,0".equals(g1.chunkKey), "Step 1 must run before Step 2 even if further away");
        fm.reportChunk("u1", g1.jobId, g1.chunkKey, ChunkState.DONE, "");

        WorkGrant g2 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, g1.projectHash);
        assertTrue("clear_area/0,0,64".equals(g2.chunkKey), "Top Y-band (64) of nearest col must be leased first");
        fm.reportChunk("u1", g2.jobId, g2.chunkKey, ChunkState.DONE, "");

        // Bot is still at x=0 (distance 0 to 0,0,48), but 32,0,64 is still unfinished on the top layer!
        WorkGrant g3 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, g1.projectHash);
        assertTrue("clear_area/32,0,64".equals(g3.chunkKey),
                "Must finish top Y-band across all columns before dropping to y=48, got " + g3.chunkKey);
        fm.reportChunk("u1", g3.jobId, g3.chunkKey, ChunkState.DONE, "");

        WorkGrant g4 = fm.claimWork("u1", "minecraft:overworld", 32, 64, 0, g1.projectHash);
        assertTrue("clear_area/32,0,48".equals(g4.chunkKey),
                "Bottom Y-band (48) unlocks only after entire top band is DONE");
        passed++;
    }

    private static void testDeferredPriorityCooldownAndMaxRetries() {
        AtomicLong clock = new AtomicLong(8_000_000L);
        FleetConfig cfg = new FleetConfig();
        cfg.maxChunkRetries = 3;
        cfg.deferCooldownSeconds = 10;
        FleetManager fm = new FleetManager(cfg, clock::get);
        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);

        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 2;
        p.maxDistance = 0;

        List<ChunkSpec> chunks = List.of(
                new ChunkSpec("clear/0,0,64", 0, 0, 0, 0),
                new ChunkSpec("clear/32,0,64", 1, 0, 32, 0),
                new ChunkSpec("clear/0,0,48", 0, 0, 0, 0)
        );
        FleetJob job = fm.submitJob("p1", "DeferTest", "{}", "minecraft:overworld", p, chunks, List.of(), "Admin");

        // Try 1 on 0,0,64 -> DEFERRED (e.g. 9 blocks across a gap).
        WorkGrant g1 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        assertTrue("clear/0,0,64".equals(g1.chunkKey), "Claims nearest top-band chunk");
        fm.reportChunk("u1", job.jobId, g1.chunkKey, ChunkState.DEFERRED, "9 left");

        // Even though bot is at x=0 and 0,0,64 is closer, PENDING 32,0,64 must be chosen next!
        WorkGrant g2 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, g1.projectHash);
        assertTrue("clear/32,0,64".equals(g2.chunkKey), "PENDING chunk must take priority over DEFERRED chunk");
        fm.reportChunk("u1", job.jobId, g2.chunkKey, ChunkState.DONE, "");

        // Before 10s cooldown elapses, 0,0,64 is not re-leased immediately (bot stays idle instead of spinning).
        WorkGrant gWait = fm.claimWork("u1", "minecraft:overworld", 32, 64, 0, g1.projectHash);
        assertTrue(gWait.idle, "DEFERRED chunk must wait for cooldown before retry");

        // Advance 10s -> Try 2 on 0,0,64 -> DEFERRED again.
        clock.addAndGet(10_000L);
        WorkGrant g3 = fm.claimWork("u1", "minecraft:overworld", 32, 64, 0, g1.projectHash);
        assertTrue("clear/0,0,64".equals(g3.chunkKey), "DEFERRED chunk retries after cooldown");
        fm.reportChunk("u1", job.jobId, g3.chunkKey, ChunkState.DEFERRED, "9 left");

        // Advance 10s -> Try 3 (final retry) -> DEFERRED transitions to FAILED!
        clock.addAndGet(10_000L);
        WorkGrant g4 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, g1.projectHash);
        assertTrue("clear/0,0,64".equals(g4.chunkKey), "Third and final attempt on DEFERRED chunk");
        fm.reportChunk("u1", job.jobId, g4.chunkKey, ChunkState.DEFERRED, "9 left");
        assertTrue(job.chunks.get("clear/0,0,64").state == ChunkState.FAILED,
                "Chunk must transition to FAILED once maxChunkRetries is reached");

        // Now that y=64 has no unfinished chunks, y=48 unlocks immediately!
        WorkGrant g5 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, g1.projectHash);
        assertTrue("clear/0,0,48".equals(g5.chunkKey),
                "Lower Y-band must unlock once stuck top-band chunk reaches max retries");
        passed++;
    }

    private static void testOneLayerLookaheadWhenTopBandClaimed() {
        AtomicLong clock = new AtomicLong(9_000_000L);
        FleetManager fm = new FleetManager(new FleetConfig(), clock::get);
        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
        fm.registerBot("u2", "Bot2", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 64, 64, 0);

        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 2;
        p.maxDistance = 0;

        // 2 columns x 3 Y-bands (64, 48, 32).
        List<ChunkSpec> chunks = List.of(
                new ChunkSpec("clear/0,0,64", 0, 0, 0, 0),
                new ChunkSpec("clear/0,0,48", 0, 0, 0, 0),
                new ChunkSpec("clear/0,0,32", 0, 0, 0, 0),
                new ChunkSpec("clear/64,0,64", 2, 0, 64, 0),
                new ChunkSpec("clear/64,0,48", 2, 0, 64, 0),
                new ChunkSpec("clear/64,0,32", 2, 0, 64, 0)
        );
        FleetJob job = fm.submitJob("p1", "Lookahead", "{}", "minecraft:overworld", p, chunks, List.of(), "Admin");

        // Both bots take top Y-band (64).
        WorkGrant b1 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        WorkGrant b2 = fm.claimWork("u2", "minecraft:overworld", 64, 64, 0, null);
        assertTrue("clear/0,0,64".equals(b1.chunkKey), "Bot1 takes col A y=64");
        assertTrue("clear/64,0,64".equals(b2.chunkKey), "Bot2 takes col B y=64");

        // Bot1 finishes col A y=64 while Bot2 is still working on col B y=64.
        fm.reportChunk("u1", job.jobId, b1.chunkKey, ChunkState.DONE, "");

        // 1-layer lookahead: Bot1 immediately gets col A y=48 (its column above is DONE), NOT col B y=48!
        WorkGrant b1Next = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, b1.projectHash);
        assertTrue(!b1Next.idle && "clear/0,0,48".equals(b1Next.chunkKey),
                "Bot1 should look ahead 1 layer to clear/0,0,48, got " + b1Next.chunkKey);

        // If Bot1 finishes col A y=48 while Bot2 is STILL on col B y=64, Bot1 must NOT drop 2 layers to y=32!
        fm.reportChunk("u1", job.jobId, b1Next.chunkKey, ChunkState.DONE, "");
        WorkGrant b1Wait = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, b1.projectHash);
        assertTrue(b1Wait.idle, "Bot1 must not drop 2 layers ahead (y=32) while y=64 is still unfinished");
        passed++;
    }

    private static void testPhaseSpilloverAndExplicitBarriers() {
        AtomicLong clock = new AtomicLong(10_000_000L);
        FleetManager fm = new FleetManager(new FleetConfig(), clock::get);
        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
        fm.registerBot("u2", "Bot2", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 64, 64, 0);

        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 2;
        p.maxDistance = 0;

        String projJson = """
                {"tasks":[
                  {"id":"fill_1","kind":"FILL_WATER"},
                  {"id":"clear_1","kind":"CLEAR_AREA"},
                  {"id":"clear_2","kind":"CLEAR_AREA"},
                  {"id":"clear_3","kind":"CLEAR_AREA","waitForPrevious":true}
                ]}""";
        List<ChunkSpec> chunks = List.of(
                new ChunkSpec("fill_1/0,0", 0, 0, 0, 0),
                new ChunkSpec("clear_1/0,0,64", 0, 0, 0, 0),
                new ChunkSpec("clear_2/64,0,64", 2, 0, 64, 0),
                new ChunkSpec("clear_3/128,0,64", 4, 0, 128, 0)
        );
        FleetJob job = fm.submitJob("p1", "Barriers", projJson, "minecraft:overworld", p, chunks, List.of(), "Admin");

        // Bot1 claims fill_1. Bot2 must NOT spill over into clear_1 (phase change FILL_WATER -> CLEAR_AREA).
        WorkGrant g1 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        assertTrue("fill_1/0,0".equals(g1.chunkKey), "Bot1 claims fill_1");
        WorkGrant g2Blocked = fm.claimWork("u2", "minecraft:overworld", 64, 64, 0, null);
        assertTrue(g2Blocked.idle, "Auto phase barrier must block CLEAR_AREA while FILL_WATER is unfinished");

        // Once fill_1 is DONE, Bot1 takes clear_1 and Bot2 automatically spills over to clear_2 (same kind)!
        fm.reportChunk("u1", job.jobId, g1.chunkKey, ChunkState.DONE, "");
        WorkGrant c1 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, g1.projectHash);
        WorkGrant c2 = fm.claimWork("u2", "minecraft:overworld", 64, 64, 0, null);
        assertTrue("clear_1/0,0,64".equals(c1.chunkKey), "Bot1 takes clear_1");
        assertTrue("clear_2/64,0,64".equals(c2.chunkKey), "Bot2 spills over to same-kind clear_2");

        // Bot2 finishes clear_2 while Bot1 still holds clear_1: clear_3 has waitForPrevious=true -> must block!
        fm.reportChunk("u2", job.jobId, c2.chunkKey, ChunkState.DONE, "");
        WorkGrant c3Blocked = fm.claimWork("u2", "minecraft:overworld", 128, 64, 0, c2.projectHash);
        assertTrue(c3Blocked.idle, "Explicit barrier (waitForPrevious=true) must block clear_3 until clear_1 finishes");

        // Once clear_1 finishes, clear_3 unlocks!
        fm.reportChunk("u1", job.jobId, c1.chunkKey, ChunkState.DONE, "");
        WorkGrant c3 = fm.claimWork("u2", "minecraft:overworld", 128, 64, 0, c2.projectHash);
        assertTrue("clear_3/128,0,64".equals(c3.chunkKey), "clear_3 unlocks once clear_1 finishes");
        passed++;
    }

    private static void testWorkerStopHoldReassignAndJobCancel() {
        AtomicLong clock = new AtomicLong(11_000_000L);
        FleetManager fm = new FleetManager(new FleetConfig(), clock::get);
        fm.registerBot("u1", "Bot1", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 0, 64, 0);
        fm.registerBot("u2", "Bot2", ClientMode.WORKER, List.of(), null, "minecraft:overworld", 32, 64, 0);

        AssignmentPolicy p = new AssignmentPolicy();
        p.maxBots = 2;
        p.maxDistance = 0;

        FleetJob j1 = fm.submitJob("p1", "Job1", "{}", "minecraft:overworld", p, List.of(
                new ChunkSpec("c1/0,0", 0, 0, 0, 0),
                new ChunkSpec("c1/32,0", 1, 0, 32, 0)
        ), List.of(), "Admin");
        FleetJob j2 = fm.submitJob("p2", "Job2", "{}", "minecraft:overworld", p, List.of(
                new ChunkSpec("c2/0,0", 0, 0, 0, 0)
        ), List.of(), "Admin");

        WorkGrant g1 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        assertTrue(j1.jobId.equals(g1.jobId), "Bot1 starts on Job1");

        // Stop & hold Bot1 -> releases its chunk back to PENDING and keeps Bot1 idle.
        assertTrue(fm.stopWorker("u1", true), "stopWorker should succeed");
        assertTrue(j1.chunks.get(g1.chunkKey).state == ChunkState.PENDING, "Released chunk must return to PENDING");
        assertTrue(fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null).idle, "Held worker must stay idle");

        // Reassign Bot1 directly to Job2 -> unholds and claims Job2!
        assertTrue(fm.reassignWorker("u1", j2.jobId), "reassignWorker should succeed");
        WorkGrant g2 = fm.claimWork("u1", "minecraft:overworld", 0, 64, 0, null);
        assertTrue(!g2.idle && j2.jobId.equals(g2.jobId), "Reassigned worker must claim from Job2");

        // Cancel all jobs -> returns u1 as affected, marks both CANCELLED, clearFinishedJobs removes them.
        List<String> affected = fm.cancelAllJobs();
        assertTrue(affected.contains("u1"), "cancelAllJobs must report active worker u1");
        assertTrue(fm.clearFinishedJobs() == 2, "clearFinishedJobs must remove both cancelled jobs");
        passed++;
    }

    private static void assertTrue(boolean cond, String msg) {
        if (!cond) {
            throw new AssertionError("Assertion failed: " + msg);
        }
    }
}
