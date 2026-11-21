# Manhattan Fleet Server

A lean, coordination-only server-side Fabric companion mod for [Manhattan](https://github.com/GermanCGArtist/Manhattan) multi-bot fleets on Minecraft 26.3 (Java 25).

## Features

- **Legit coordination-only:** Never peeks at unloaded chunks or chest inventories directly. All world state comes from client reports.
- **Editor / Worker / Both modes:** Submit Projects from an Editor client and log off—workers continue autonomously and survive server restarts.
- **Flexible bot targeting:** Assign jobs to explicit bots, tag/pool groups, or auto-assign with `maxBots` and `maxDistance` guardrails across multiple concurrent jobs.
- **Lease-based chunk distribution:** 1-chunk spatial separation between active workers, 60 s rejoin grace window on disconnect (with automatic preemption when idle peers have no other work), and instant release on death.
- **Coordinated 1-player sleeping:** Picks the single worker nearest its bed (personal or site `bedPool`) while all other bots keep working, with automatic failover if the chosen sleeper fails or disconnects.
- **Shared chest sync & locks:** Propagates client-reported empty/full chest state and enforces short 1-bot-per-chest locks.
- **Server commands:** `/fleet status`, `/fleet pause <jobId>`, `/fleet resume <jobId>`, `/fleet cancel <jobId>`, `/fleet reload`.

## Configuration

`config/manhattan-fleet.json` (auto-created on first launch):

```json
{
  "editors": [],
  "leaseTtlSeconds": 30,
  "rejoinGraceSeconds": 60,
  "spatialBufferChunks": 1,
  "chestLockTtlSeconds": 30,
  "sleepTimeoutSeconds": 60
}
```

Editors are authorized if they have server op level ≥ 2 **or** their username/UUID is listed in `editors`.

## Building & Testing

```bash
./gradlew build
./gradlew fleetTest
```

Output jar: `build/libs/manhattan-fleet-server-<version>.jar`

## License

MIT — see [LICENSE](LICENSE).
