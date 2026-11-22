package dev.manhattan.fleet.server;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure-Java Gson models for fleet jobs, chunk leases, bot registry, chest sync, and snapshots.
 */
public final class FleetModels {
    private FleetModels() {}

    public static final int PROTOCOL_VERSION = 1;

    public enum ClientMode {
        LOCAL,
        EDITOR,
        WORKER,
        BOTH
    }

    public enum AssignmentMode {
        EXPLICIT,
        AUTO_POOL
    }

    public enum JobStatus {
        ACTIVE,
        PAUSED,
        DONE,
        CANCELLED
    }

    public enum ChunkState {
        PENDING,
        CLAIMED,
        DISCONNECTED_GRACE,
        DONE,
        DEFERRED,
        FAILED
    }

    public enum ChestState {
        AVAILABLE,
        EMPTY,
        FULL
    }

    public static final class AssignmentPolicy {
        public AssignmentMode mode = AssignmentMode.AUTO_POOL;
        public List<String> explicitBots = new ArrayList<>();
        public String poolTag = "";
        public int maxBots = 2;
        public int maxDistance = 500;
        public int priority = 0;

        public void ensureInitialized() {
            if (mode == null) mode = AssignmentMode.AUTO_POOL;
            if (explicitBots == null) explicitBots = new ArrayList<>();
            if (poolTag == null) poolTag = "";
            if (maxBots < 1) maxBots = 1;
            if (maxDistance < 0) maxDistance = 0;
        }
    }

    public static final class ChunkSpec {
        public String chunkKey;
        public int cx;
        public int cz;
        public int centerX;
        public int centerZ;

        public ChunkSpec() {}

        public ChunkSpec(String chunkKey, int cx, int cz, int centerX, int centerZ) {
            this.chunkKey = chunkKey;
            this.cx = cx;
            this.cz = cz;
            this.centerX = centerX;
            this.centerZ = centerZ;
        }
    }

    public static final class ChunkEntry {
        public String chunkKey;
        public int cx;
        public int cz;
        public int centerX;
        public int centerZ;
        public ChunkState state = ChunkState.PENDING;
        public String ownerUuid;
        public String ownerName;
        public long leaseExpiresAtMs;
        public long graceExpiresAtMs;
        public long nextRetryAtMs;
        public int tries;
        public String reason;

        public ChunkEntry() {}

        public ChunkEntry(ChunkSpec spec) {
            this.chunkKey = spec.chunkKey;
            this.cx = spec.cx;
            this.cz = spec.cz;
            this.centerX = spec.centerX;
            this.centerZ = spec.centerZ;
            this.state = ChunkState.PENDING;
        }
    }

    public static final class FleetJob {
        public String jobId;
        public String projectId;
        public String projectName;
        public String projectHash;
        public String projectJson;
        public String dimension = "minecraft:overworld";
        public JobStatus status = JobStatus.ACTIVE;
        public AssignmentPolicy assignment = new AssignmentPolicy();
        public String createdBy = "";
        public long createdAtMs;
        public List<int[]> bedPool = new ArrayList<>();
        public Map<String, ChunkEntry> chunks = new LinkedHashMap<>();
        /** botUuid -> assigned bed [x, y, z] from bedPool. */
        public Map<String, int[]> botBeds = new LinkedHashMap<>();

        public void ensureInitialized() {
            if (dimension == null || dimension.isBlank()) dimension = "minecraft:overworld";
            if (status == null) status = JobStatus.ACTIVE;
            if (assignment == null) assignment = new AssignmentPolicy();
            assignment.ensureInitialized();
            if (bedPool == null) bedPool = new ArrayList<>();
            if (chunks == null) chunks = new LinkedHashMap<>();
            if (botBeds == null) botBeds = new LinkedHashMap<>();
        }
    }

    public static final class BotRecord {
        public String uuid;
        public String name;
        public ClientMode mode = ClientMode.LOCAL;
        public List<String> tags = new ArrayList<>();
        public boolean online;
        public String dimension = "minecraft:overworld";
        public int x;
        public int y;
        public int z;
        public double health = 20.0;
        public int food = 20;
        public int freeSlots = 36;
        public boolean inDanger;
        public String activeJobId;
        public String activeChunkKey;
        public String phase = "IDLE";
        public String alertSummary = "";
        public int[] localBed;
        public int[] assignedBed;
        public long lastHeartbeatMs;
        public long disconnectedAtMs;

        public int[] effectiveBed() {
            if (localBed != null && localBed.length == 3) return localBed;
            if (assignedBed != null && assignedBed.length == 3) return assignedBed;
            return null;
        }
    }

    public static final class WorkGrant {
        public boolean idle;
        public String jobId;
        public String projectId;
        public String projectHash;
        /** Included only when the worker does not already cache this projectHash. */
        public String projectJson;
        public String chunkKey;
        public int cx;
        public int cz;
        public int[] assignedBed;
        public long leaseExpiresAtMs;
        public boolean inheritedFromGrace;

        public static WorkGrant idle() {
            WorkGrant g = new WorkGrant();
            g.idle = true;
            return g;
        }
    }

    public static final class ChestRecord {
        public String dimension;
        public int x;
        public int y;
        public int z;
        public ChestState state = ChestState.AVAILABLE;
        public String updatedBy;
        public long updatedAtMs;
        public String lockedByUuid;
        public String lockedByName;
        public long lockExpiresAtMs;

        public String key() {
            return dimension + ":" + x + "," + y + "," + z;
        }
    }

    public static final class JobSummary {
        public String jobId;
        public String projectId;
        public String projectName;
        public String dimension;
        public JobStatus status;
        public AssignmentPolicy assignment;
        public int totalChunks;
        public int doneChunks;
        public int claimedChunks;
        public int failedChunks;
        public List<String> activeWorkers = new ArrayList<>();
    }

    public static final class PeerClaim {
        public String jobId;
        public String chunkKey;
        public int cx;
        public int cz;
        public int centerX;
        public int centerZ;
        public String ownerName;
        public ChunkState state;
    }

    public static final class FleetSnapshot {
        public long serverTimeMs;
        public String designatedSleeperUuid;
        public String designatedSleeperName;
        public List<BotRecord> bots = new ArrayList<>();
        public List<JobSummary> jobs = new ArrayList<>();
        public List<PeerClaim> activeClaims = new ArrayList<>();
    }
}
