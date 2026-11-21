package dev.manhattan.fleet.server;

import dev.manhattan.fleet.server.FleetModels.ChestRecord;
import dev.manhattan.fleet.server.FleetModels.ChestState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Coordinates shared chest empty/full reporting and short 1-bot-per-chest locks so workers
 * do not walk to depleted chests or crowd the same container simultaneously.
 */
public final class ChestCoordinator {
    private final Map<String, ChestRecord> chests = new LinkedHashMap<>();

    public record LockResult(boolean granted, String lockedByName, long expiresAtMs) {}

    public static String chestKey(String dimension, int x, int y, int z) {
        return (dimension == null ? "minecraft:overworld" : dimension) + ":" + x + "," + y + "," + z;
    }

    public ChestRecord reportState(String dimension, int x, int y, int z, ChestState state, String reporterName, long nowMs) {
        String key = chestKey(dimension, x, y, z);
        ChestRecord rec = chests.computeIfAbsent(key, k -> {
            ChestRecord r = new ChestRecord();
            r.dimension = dimension == null ? "minecraft:overworld" : dimension;
            r.x = x;
            r.y = y;
            r.z = z;
            return r;
        });
        rec.state = state == null ? ChestState.AVAILABLE : state;
        rec.updatedBy = reporterName;
        rec.updatedAtMs = nowMs;
        return rec;
    }

    public LockResult tryLock(String dimension, int x, int y, int z, String botUuid, String botName, long ttlMs, long nowMs) {
        expireLocks(nowMs);
        String key = chestKey(dimension, x, y, z);
        ChestRecord rec = chests.computeIfAbsent(key, k -> {
            ChestRecord r = new ChestRecord();
            r.dimension = dimension == null ? "minecraft:overworld" : dimension;
            r.x = x;
            r.y = y;
            r.z = z;
            return r;
        });

        if (rec.lockedByUuid == null || rec.lockedByUuid.equals(botUuid) || nowMs >= rec.lockExpiresAtMs) {
            rec.lockedByUuid = botUuid;
            rec.lockedByName = botName;
            rec.lockExpiresAtMs = nowMs + Math.max(5_000L, ttlMs);
            return new LockResult(true, botName, rec.lockExpiresAtMs);
        }
        return new LockResult(false, rec.lockedByName, rec.lockExpiresAtMs);
    }

    public boolean unlock(String dimension, int x, int y, int z, String botUuid) {
        String key = chestKey(dimension, x, y, z);
        ChestRecord rec = chests.get(key);
        if (rec != null && botUuid != null && botUuid.equals(rec.lockedByUuid)) {
            rec.lockedByUuid = null;
            rec.lockedByName = null;
            rec.lockExpiresAtMs = 0L;
            return true;
        }
        return false;
    }

    public int releaseAllForBot(String botUuid) {
        if (botUuid == null) return 0;
        int released = 0;
        for (ChestRecord rec : chests.values()) {
            if (botUuid.equals(rec.lockedByUuid)) {
                rec.lockedByUuid = null;
                rec.lockedByName = null;
                rec.lockExpiresAtMs = 0L;
                released++;
            }
        }
        return released;
    }

    public void expireLocks(long nowMs) {
        for (ChestRecord rec : chests.values()) {
            if (rec.lockedByUuid != null && nowMs >= rec.lockExpiresAtMs) {
                rec.lockedByUuid = null;
                rec.lockedByName = null;
                rec.lockExpiresAtMs = 0L;
            }
        }
    }

    public ChestRecord get(String dimension, int x, int y, int z) {
        return chests.get(chestKey(dimension, x, y, z));
    }

    public List<ChestRecord> snapshot() {
        return new ArrayList<>(chests.values());
    }
}
