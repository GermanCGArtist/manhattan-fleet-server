package dev.manhattan.fleet.server;

import dev.manhattan.fleet.server.FleetModels.BotRecord;
import dev.manhattan.fleet.server.FleetModels.ClientMode;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Coordinates 1-player sleeping across the fleet: when night or thunder begins in the Overworld,
 * selects the single online worker closest to its bed (personal or site-assigned) while all other
 * bots keep working. Automatically fails over to the next-nearest bot on failure, disconnect,
 * death, or timeout.
 */
public final class SleepCoordinator {
    public static final String OVERWORLD = "minecraft:overworld";

    public record SleepOrder(String botUuid, String botName, int[] bedPos, long expiresAtMs) {}

    private final long sleepTimeoutMs;
    private boolean cycleActive;
    private SleepOrder currentOrder;
    private final Set<String> failedThisCycle = new HashSet<>();

    public SleepCoordinator(long sleepTimeoutMs) {
        this.sleepTimeoutMs = Math.max(15_000L, sleepTimeoutMs);
    }

    public SleepOrder currentOrder() {
        return currentOrder;
    }

    public String designatedSleeperUuid() {
        return currentOrder != null ? currentOrder.botUuid() : null;
    }

    public String designatedSleeperName() {
        return currentOrder != null ? currentOrder.botName() : null;
    }

    /**
     * Advances the sleep coordinator. Returns a newly issued {@link SleepOrder} when a sleeper is
     * selected (or re-selected after timeout), or {@code null} if no change occurred.
     */
    public SleepOrder tick(long nowMs, boolean isNightOrThunder, Collection<BotRecord> bots) {
        if (!isNightOrThunder) {
            if (cycleActive || currentOrder != null) {
                cycleActive = false;
                currentOrder = null;
                failedThisCycle.clear();
            }
            return null;
        }

        cycleActive = true;

        if (currentOrder != null) {
            if (nowMs >= currentOrder.expiresAtMs()) {
                failedThisCycle.add(currentOrder.botUuid());
                currentOrder = null;
            } else {
                return null;
            }
        }

        return selectNextSleeper(nowMs, bots);
    }

    /**
     * Handles status reports from the designated sleeper: {@code COMPLETED} clears the active order,
     * {@code FAILED} blacklists that bot for this night cycle and immediately picks the next-nearest bot.
     */
    public SleepOrder onSleepStatus(String botUuid, String status, long nowMs, Collection<BotRecord> bots) {
        if (currentOrder == null || !currentOrder.botUuid().equals(botUuid)) {
            return null;
        }
        if ("COMPLETED".equalsIgnoreCase(status)) {
            currentOrder = null;
            return null;
        }
        if ("FAILED".equalsIgnoreCase(status)) {
            failedThisCycle.add(botUuid);
            currentOrder = null;
            if (cycleActive) {
                return selectNextSleeper(nowMs, bots);
            }
        }
        return null;
    }

    /** Called on worker disconnect or death; immediately promotes the next-nearest bot if needed. */
    public SleepOrder onBotUnavailable(String botUuid, long nowMs, Collection<BotRecord> bots) {
        if (currentOrder == null || !currentOrder.botUuid().equals(botUuid)) {
            return null;
        }
        failedThisCycle.add(botUuid);
        currentOrder = null;
        if (cycleActive) {
            return selectNextSleeper(nowMs, bots);
        }
        return null;
    }

    private SleepOrder selectNextSleeper(long nowMs, Collection<BotRecord> bots) {
        BotRecord best = null;
        double bestDistSq = Double.MAX_VALUE;
        int[] bestBed = null;

        for (BotRecord bot : bots) {
            if (bot == null || !bot.online) continue;
            if (bot.mode != ClientMode.WORKER && bot.mode != ClientMode.BOTH) continue;
            if (bot.inDanger) continue;
            if (!OVERWORLD.equals(bot.dimension)) continue;
            if (failedThisCycle.contains(bot.uuid)) continue;

            int[] bed = bot.effectiveBed();
            if (bed == null || bed.length != 3) continue;

            double dx = bot.x - bed[0];
            double dy = bot.y - bed[1];
            double dz = bot.z - bed[2];
            double distSq = dx * dx + dy * dy + dz * dz;
            if (distSq < bestDistSq) {
                bestDistSq = distSq;
                best = bot;
                bestBed = new int[]{bed[0], bed[1], bed[2]};
            }
        }

        if (best == null) {
            return null;
        }

        currentOrder = new SleepOrder(best.uuid, best.name, bestBed, nowMs + sleepTimeoutMs);
        return currentOrder;
    }
}
