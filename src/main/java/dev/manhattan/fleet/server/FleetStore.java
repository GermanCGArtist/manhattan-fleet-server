package dev.manhattan.fleet.server;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dev.manhattan.fleet.server.FleetModels.FleetJob;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Atomic JSON persistence for server fleet state ({@code <world>/data/manhattan-fleet.json}).
 */
public final class FleetStore {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private static final class PersistedState {
        int version = 1;
        List<FleetJob> jobs = new ArrayList<>();
    }

    private FleetStore() {}

    public static List<FleetJob> load(Path path) {
        if (path == null || !Files.exists(path)) {
            return List.of();
        }
        try {
            String json = Files.readString(path, StandardCharsets.UTF_8);
            PersistedState state = GSON.fromJson(json, PersistedState.class);
            if (state != null && state.jobs != null) {
                return state.jobs;
            }
        } catch (Exception ignored) {
        }
        return List.of();
    }

    public static void save(Path path, Collection<FleetJob> jobs) {
        if (path == null) return;
        PersistedState state = new PersistedState();
        if (jobs != null) {
            state.jobs.addAll(jobs);
        }
        String json = GSON.toJson(state);
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, json, StandardCharsets.UTF_8);
            try {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicUnsupported) {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException ignored) {
        }
    }
}
