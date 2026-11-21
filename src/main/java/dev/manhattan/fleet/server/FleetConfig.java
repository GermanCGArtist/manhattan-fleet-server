package dev.manhattan.fleet.server;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Server-side configuration stored in {@code config/manhattan-fleet.json}.
 */
public final class FleetConfig {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    public List<String> editors = new ArrayList<>();
    public int leaseTtlSeconds = 30;
    public int rejoinGraceSeconds = 60;
    public int spatialBufferChunks = 1;
    public int chestLockTtlSeconds = 30;
    public int sleepTimeoutSeconds = 60;

    public void ensureInitialized() {
        if (editors == null) editors = new ArrayList<>();
        if (leaseTtlSeconds < 5) leaseTtlSeconds = 30;
        if (rejoinGraceSeconds < 0) rejoinGraceSeconds = 60;
        if (spatialBufferChunks < 0) spatialBufferChunks = 0;
        if (chestLockTtlSeconds < 5) chestLockTtlSeconds = 30;
        if (sleepTimeoutSeconds < 15) sleepTimeoutSeconds = 60;
    }

    public boolean isEditorWhitelisted(UUID uuid, String name) {
        if (editors == null || editors.isEmpty()) return false;
        String uuidStr = uuid != null ? uuid.toString().toLowerCase(Locale.ROOT) : "";
        String nameLower = name != null ? name.toLowerCase(Locale.ROOT) : "";
        for (String entry : editors) {
            if (entry == null || entry.isBlank()) continue;
            String trimmed = entry.trim().toLowerCase(Locale.ROOT);
            if (trimmed.equals(uuidStr) || trimmed.equals(nameLower)) {
                return true;
            }
        }
        return false;
    }

    public static FleetConfig loadOrCreate(Path path) {
        if (Files.exists(path)) {
            try {
                String json = Files.readString(path, StandardCharsets.UTF_8);
                FleetConfig cfg = GSON.fromJson(json, FleetConfig.class);
                if (cfg != null) {
                    cfg.ensureInitialized();
                    return cfg;
                }
            } catch (Exception ignored) {
                // Fall back to defaults below.
            }
        }
        FleetConfig cfg = new FleetConfig();
        cfg.ensureInitialized();
        save(path, cfg);
        return cfg;
    }

    public static void save(Path path, FleetConfig cfg) {
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, GSON.toJson(cfg), StandardCharsets.UTF_8);
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            try {
                Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
                Files.writeString(tmp, GSON.toJson(cfg), StandardCharsets.UTF_8);
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ignored) {
            }
        }
    }
}
