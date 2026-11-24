package dev.manhattan.fleet.server;

import dev.manhattan.fleet.server.FleetModels.ChestRecord;
import dev.manhattan.fleet.server.FleetModels.ChestState;
import dev.manhattan.fleet.server.FleetModels.Reservation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Coordinates shared chest empty/full reporting, short 1-bot-per-chest locks, and fleet-wide item stock with
 * reservations, so workers do not walk to depleted chests, crowd one container, or all head for the same last
 * stack of an item.
 *
 * <p>Stock is only ever what a bot saw inside a chest it opened itself (the server never reads containers).
 * A reservation holds part of that stock for one bot for {@code RESERVATION_TTL_MS}; the next stock report
 * of that bot for that chest settles its reservations there (it took the items, or found less).
 */
public final class ChestCoordinator {
    private final Map<String, ChestRecord> chests = new LinkedHashMap<>();

    public record LockResult(boolean granted, String lockedByName, long expiresAtMs) {}

    /** Result of a reservation request: how many of the asked items this bot may take from that chest. */
    public record ReserveResult(int granted, int available, long expiresAtMs) {}

    public static String chestKey(String dimension, int x, int y, int z) {
        return (dimension == null ? "minecraft:overworld" : dimension) + ":" + x + "," + y + "," + z;
    }

    private ChestRecord record(String dimension, int x, int y, int z) {
        String key = chestKey(dimension, x, y, z);
        return chests.computeIfAbsent(key, k -> {
            ChestRecord r = new ChestRecord();
            r.dimension = dimension == null ? "minecraft:overworld" : dimension;
            r.x = x;
            r.y = y;
            r.z = z;
            return r;
        });
    }

    public ChestRecord reportState(String dimension, int x, int y, int z, ChestState state, String reporterName, long nowMs) {
        ChestRecord rec = record(dimension, x, y, z);
        rec.state = state == null ? ChestState.AVAILABLE : state;
        rec.updatedBy = reporterName;
        rec.updatedAtMs = nowMs;
        if (rec.state == ChestState.EMPTY) {
            rec.stock = new LinkedHashMap<>();
            rec.stockAtMs = nowMs;
            rec.stockBy = reporterName;
        }
        return rec;
    }

    // ------------------------------------------------------------------ stock & reservations

    /**
     * Replaces the known contents of a chest with what {@code botUuid} just saw in it, and settles that bot's
     * reservations on this chest.
     */
    public ChestRecord reportStock(String dimension, int x, int y, int z, Map<String, Integer> items,
                                   String botUuid, String botName, long nowMs) {
        ChestRecord rec = record(dimension, x, y, z);
        Map<String, Integer> clean = new LinkedHashMap<>();
        int total = 0;
        if (items != null) {
            for (Map.Entry<String, Integer> e : items.entrySet()) {
                if (e.getKey() == null || e.getKey().isBlank() || e.getValue() == null || e.getValue() <= 0) continue;
                clean.merge(e.getKey(), e.getValue(), Integer::sum);
                total += e.getValue();
            }
        }
        rec.stock = clean;
        rec.stockAtMs = nowMs;
        rec.stockBy = botName;
        rec.updatedBy = botName;
        rec.updatedAtMs = nowMs;
        if (total == 0) rec.state = ChestState.EMPTY;
        else if (rec.state == ChestState.EMPTY) rec.state = ChestState.AVAILABLE;
        if (rec.reservations == null) rec.reservations = new ArrayList<>();
        if (botUuid != null) rec.reservations.removeIf(r -> botUuid.equals(r.botUuid));
        expireReservations(rec, nowMs);
        return rec;
    }

    /** Items of {@code item} in this chest not reserved by bots other than {@code botUuid}. */
    public int unreserved(ChestRecord rec, String item, String botUuid) {
        if (rec == null || item == null) return 0;
        int have = rec.stock == null ? 0 : rec.stock.getOrDefault(item, 0);
        int reservedByOthers = 0;
        for (Reservation r : rec.reservations) {
            if (item.equals(r.item) && (botUuid == null || !botUuid.equals(r.botUuid))) reservedByOthers += r.count;
        }
        return Math.max(0, have - reservedByOthers);
    }

    /**
     * Reserves up to {@code count} of {@code item}. A chest whose stock nobody has seen yet grants nothing but
     * reports {@code available = -1}, so the bot knows it may go and look.
     */
    public ReserveResult reserve(String dimension, int x, int y, int z, String item, int count,
                                 String botUuid, String botName, long ttlMs, long nowMs) {
        ChestRecord rec = record(dimension, x, y, z);
        expireReservations(rec, nowMs);
        if (rec.stockAtMs <= 0 || item == null || item.isBlank()) return new ReserveResult(0, -1, 0L);
        rec.reservations.removeIf(r -> botUuid != null && botUuid.equals(r.botUuid) && item.equals(r.item));
        int available = unreserved(rec, item, botUuid);
        int granted = Math.max(0, Math.min(count, available));
        long exp = nowMs + Math.max(5_000L, ttlMs);
        if (granted > 0) {
            Reservation r = new Reservation();
            r.botUuid = botUuid;
            r.botName = botName;
            r.item = item;
            r.count = granted;
            r.expiresAtMs = exp;
            rec.reservations.add(r);
        }
        return new ReserveResult(granted, available, granted > 0 ? exp : 0L);
    }

    /** Drops every reservation of a bot (it died or left). */
    public int dropReservationsOf(String botUuid) {
        if (botUuid == null) return 0;
        int n = 0;
        for (ChestRecord rec : chests.values()) {
            if (rec.reservations == null) continue;
            int before = rec.reservations.size();
            rec.reservations.removeIf(r -> botUuid.equals(r.botUuid));
            n += before - rec.reservations.size();
        }
        return n;
    }

    /** Drops lapsed reservations everywhere; true when any lapsed. */
    public boolean expireReservations(long nowMs) {
        boolean any = false;
        for (ChestRecord rec : chests.values()) any |= expireReservations(rec, nowMs);
        return any;
    }

    private static boolean expireReservations(ChestRecord rec, long nowMs) {
        if (rec.reservations == null) {
            rec.reservations = new ArrayList<>();
            return false;
        }
        boolean any = false;
        for (Iterator<Reservation> it = rec.reservations.iterator(); it.hasNext(); ) {
            if (nowMs >= it.next().expiresAtMs) {
                it.remove();
                any = true;
            }
        }
        return any;
    }

    // ------------------------------------------------------------------ locks

    public LockResult tryLock(String dimension, int x, int y, int z, String botUuid, String botName, long ttlMs, long nowMs) {
        expireLocks(nowMs);
        ChestRecord rec = record(dimension, x, y, z);
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

    // ------------------------------------------------------------------ persistence

    /** State and stock survive a restart; locks and reservations do not (their bots are offline then). */
    public List<ChestRecord> exportChests() {
        List<ChestRecord> out = new ArrayList<>();
        for (ChestRecord rec : chests.values()) {
            ChestRecord c = new ChestRecord();
            c.dimension = rec.dimension;
            c.x = rec.x;
            c.y = rec.y;
            c.z = rec.z;
            c.state = rec.state;
            c.updatedBy = rec.updatedBy;
            c.updatedAtMs = rec.updatedAtMs;
            c.stock = new LinkedHashMap<>(rec.stock == null ? Map.of() : rec.stock);
            c.stockAtMs = rec.stockAtMs;
            c.stockBy = rec.stockBy;
            c.reservations = new ArrayList<>();
            out.add(c);
        }
        return out;
    }

    public void importChests(List<ChestRecord> saved) {
        chests.clear();
        if (saved == null) return;
        for (ChestRecord c : saved) {
            if (c == null) continue;
            if (c.stock == null) c.stock = new LinkedHashMap<>();
            c.reservations = new ArrayList<>();
            c.lockedByUuid = null;
            c.lockedByName = null;
            c.lockExpiresAtMs = 0L;
            if (c.state == null) c.state = ChestState.AVAILABLE;
            chests.put(chestKey(c.dimension, c.x, c.y, c.z), c);
        }
    }
}
