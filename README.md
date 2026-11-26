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

## Reliability (protocol v2, 0.2.0+)

Needs Manhattan 2.15.0+ on every client (an older client gets a message naming both versions).

- **Lease epochs:** every grant raises the chunk's epoch; reports and lease renewals must name it, so a bot whose
  lease ran out can never finish or fail a chunk another bot now holds.
- **Acknowledged, idempotent reports:** clients resend chunk reports from a saved outbox until a `report_ack`
  arrives; the last `msgId` per chunk is kept (and saved), so a resent report is answered, never applied twice.
- **Hand-off:** a finished chunk is answered with the next lease in the same round trip.
- **Safe persistence:** `<world>/data/manhattan-fleet.json` is written on a background thread, the previous good
  file is kept as `.bak`, an unreadable file is moved aside (`.corrupt-<time>`) and never overwritten, projects are
  stored once per hash in `manhattan-fleet-projects/`. Bots (held, tags, sticky job), shared hazards and chest stock
  survive restarts. A job whose project file is missing is paused with a reason.
- **Multi-bot awareness:** path intents (the next steps of each bot's path) are relayed to bots within 160 blocks
  (4 per second per bot, never stored); deaths are shared as hazards; chest contents seen by bots are shared as stock
  with 90 s item reservations.
- **Owner index:** claims, renewals and releases no longer scan every chunk of every job.
- **Compressed & chunked packets (0.2.1+):** large Fleet job payloads are automatically gzip-compressed and chunked under Minecraft's 32 KB C2S packet limit.

## Related Suite Mods

- **[Manhattan](https://github.com/GermanCGArtist/Manhattan)** — Client automation scripting, Midtown path engine, and fleet UI
- **[Liskov](https://github.com/GermanCGArtist/Liskov)** — Shared ImGui UI host, Drives filesystem, VCS, and `liskov:sync`
- **[Vitruvius](https://github.com/GermanCGArtist/Vitruvius)** — Blender-style 3D schematic scene editor (`F6`)
- **Neurath** — Transit network diagram, JourneyMap vector overlay, and Spectate mode (`N` / `V`)

The wire contract lives in `src/main/java/dev/manhattan/fleet/wire/FleetWire.java` and is byte-identical with
Manhattan's copy; `./gradlew fleetTest` compares both when the checkouts sit side by side.

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

Output jars: `build/libs/manhattan-fleet-server-<version>.jar`, and the test-only
`build/libs/manhattan-fleet-benchbridge-<version>.jar` used by Manhattan's Deming fleet scenarios (never install it
on a real server).

`fleetTest` runs the coordinator tests plus a seeded chaos simulation: up to 32 simulated bots over a network that
drops, duplicates and reorders messages, with disconnects, deaths, stalls past the lease and server restarts from
the saved files; every chunk must end DONE exactly once.

## License

MIT — see [LICENSE](LICENSE).
