# NEON-Sage Edge: Local Kafka Mirror and `neon_sage_edge` Client

Sage nodes deployed within NEON towers can pull data from some of the internal data streams available in real time within the tower. NEON uses Kafka (with Avro-encoded messages) to provide access to the NEON tower data.

This project provides scientists with a controlled, lightweight way to pull specific NEON data streams and use them for real-time calculations at the edge. It has two parts:

- **Local Kafka mirror** — two pieces on the Sage node:
  - **MirrorMaker 2 (MM2)** is the only component that connects to NEON's Kafka. It holds the NEON credentials, reads the allowlisted topics, and is therefore the piece that determines the load placed on NEON's infrastructure.
  - **Local Kafka** (single node) receives what MM2 copies and caches recent data for local readers. It never talks to NEON; it can only slow MM2 down (see Upstream throttling).
- **`neon_sage_edge` client library** — a thin Python client used by Sage edge applications (plugins). It reads from the local Kafka with **read-only** access, decodes Avro messages, and provides a simple API. It holds no NEON credentials and never connects to NEON's Kafka.

In the first version (v1), the mirror has exactly two jobs: **hold a cache of recent values** (local Kafka) and **limit the load upstream on NEON** (MM2, with help from a quota on the local Kafka). Everything else (what the data means, calibration, data levels, quality flags) is between the scientist and NEON's data products and is out of scope for this infrastructure.

To focus the design, two driving use cases guide the requirements:

A) A lightweight, at-the-edge, real-time eddy covariance approximation.

B) A lightweight anomaly detection example that uses relatively high-frequency data.

## Architecture

```
 NEON Kafka (tower)
        │   MirrorMaker 2: one upstream consumer, allowlisted topics only,
        │   NEON credentials, byte-rate throttled
        ▼
 ┌──────────────────────────────────────────┐
 │  Local Kafka (single node, KRaft)        │  time-based retention = cache
 │   - mirrored NEON topics (NEON names)    │  read-only for plugins
 │   - MirrorMaker 2 is the only writer     │
 └──────────────────────────────────────────┘
        │   Kafka protocol, read-only, in-cluster only
        ▼
 plugin A   plugin B   plugin C ...   (each uses neon_sage_edge client)
```

MirrorMaker 2 (MM2) ships with Apache Kafka and runs as a single process in "dedicated" mode. Local Kafka and MM2 can run in one pod built from the stock `apache/kafka` image.

Because MM2 holds one upstream subscription per mirrored topic and Kafka fans data out to any number of local readers, the load on NEON's Kafka depends only on the **allowlist**, not on how many plugins are running. Launching 100 copies of a plugin does not change the upstream load.

## Key Design Components

1) **Mirror selection (allowlist).** The set of mirrored topics is a static list in MM2's configuration (`neon->local.topics`), maintained by Sage/NEON administrators. MM2 mirrors every listed topic continuously, whether or not a plugin is reading it. Upstream load is therefore set by the allowlist, which should stay small and deliberate. Topic names are kept identical to NEON's (`IdentityReplicationPolicy`).

2) **Upstream throttling.** MM2 is the only client of NEON's Kafka, so upstream load is whatever MM2 reads. MM2 itself has no rate-limit setting, so the limit is built from these pieces:
   - **Which topics:** MM2's allowlist (component 1). The limit on the total number of topics is simply the length of the allowlist.
   - **Rate (indirect):** a Kafka client quota on MM2's identity *at the local Kafka* (`producer_byte_rate`). The local Kafka cannot touch NEON; it slows MM2's writes, MM2's send buffer fills, and MM2 stops fetching from NEON until it drains. Upstream rate therefore tracks the local quota, with short bursts of up to one fetch batch.
   - **Burst size and request rate:** MM2's own consumer settings toward NEON (`fetch.max.bytes`, `max.partition.fetch.bytes`, `fetch.max.wait.ms`, `fetch.min.bytes`) and a single task (`tasks.max = 1`) bound how much each request pulls and how often MM2 polls.
   - **Hard limit (direct):** only a quota that NEON sets on the MM2 account on *their* Kafka is enforced at NEON. This is the strongest protection and is under NEON's control; we should ask for one.

   Notes:
   - Kafka quotas are **byte-rate** limits, not message-rate limits.
   - Throttling **delays** data; it does not drop or thin it. If the mirror falls behind for longer than NEON's retention, data is lost upstream.

3) **Cache (retention).** Each mirrored topic keeps a bounded, time-based history on the node (e.g. `log.retention.ms` = 1 hour, with `log.segment.ms` small enough, e.g. 5 minutes, that retention is reasonably precise). MM2 is configured *not* to copy NEON's topic configurations, so local retention is set by the node, not inherited from NEON. At the expected data rates (a handful of streams at ~20 Hz), an hour of history is a few MB.

4) **Read-only plugin access.** Plugins can only read. This is enforced by Kafka itself, not by convention:
   - The local Kafka has an authenticated listener (SASL) used only by MM2, which is a super-user. Its credentials are a node secret mounted only into the Kafka/MM2 pod.
   - Plugins connect on an unauthenticated in-cluster listener as the `ANONYMOUS` principal. ACLs grant `ANONYMOUS` only `READ` and `DESCRIBE` on the mirrored topics. Everything else is denied (`allow.everyone.if.no.acl.found=false`).
   - `auto.create.topics.enable=false`.
   - Plugins read partitions directly (`assign()`), not through consumer groups. They need no group permissions and leave no stale state behind. This also matches the access pattern: plugins read "from now" or "from N seconds ago", not from a saved position.

5) **Credential isolation and network policy.** NEON credentials exist only in MM2's configuration. A Kubernetes NetworkPolicy blocks plugin pods from reaching NEON's Kafka directly, so the mirror is the only access path in practice.

6) **Data access model.** The client supports:
   - *subscribe*: receive messages for a topic as they arrive;
   - *latest*: get the most recent message for a topic (across all of its partitions); and
   - *window*: get the last N seconds of a topic from the local cache (`offsets_for_times(now − N)`, then read forward), limited by local retention.

7) **Message decoding.** NEON messages are Avro. The client decodes them into Python dicts and attaches the Kafka record timestamp, topic, partition, and offset. Field names, sensor timestamps, and quality flags are whatever NEON's schema provides; the client does not interpret them. How the client obtains schemas is an open design issue (see below).

8) **Gap and staleness detection.** Kafka does not drop messages for slow readers; a slow reader just falls behind. The client turns the cases that matter into explicit signals:
   - *Gap*: the reader's position was deleted by retention before it was read. The client sets `auto.offset.reset=error`, catches the out-of-range error, skips forward, and reports a gap (with the time span lost).
   - *Lag*: the client reports how far behind real time the reader is and can optionally skip to the latest data.
   - *Stale stream*: no new message on a topic for several times its nominal period. This indicates an upstream outage, tower network problem, or the mirror being throttled.

9) **Error handling.** Errors are explicit and typed: topic not mirrored (not on the allowlist), local Kafka unavailable, gap, stale stream, and decode error. Reconnecting after a local Kafka restart is handled by the Kafka client library; because the cache is on disk, data written before the restart is still available.

10) **Topic discovery.** `list_topics()` returns the mirrored topic names from Kafka metadata (internal topics filtered out). External documentation explains how NEON topic names map to sensors and measurements. Descriptions and nominal rates in the API itself are future work.

11) **Usage statistics.** v1 reports a minimal set to Beehive from a small sidecar that reads Kafka's metrics: upstream bytes and message rates per topic, MM2 throttle time, mirror lag, and per-`client.id` bytes delivered. In v1, plugin identity is the `client.id` each plugin reports for itself (the client sets it from the plugin name), so per-plugin numbers are best-effort.

12) **Replay / mock mode.** Off-tower, the same client runs against a local Kafka in Docker preloaded with recorded NEON topics. Scientists develop and test plugins on a laptop with no code changes.

## Driving Use Case Requirements

| | A) Eddy covariance approximation | B) Anomaly detection |
|---|---|---|
| Streams | Sonic anemometer (3D wind, sonic temperature) and gas analyzer (CO₂, H₂O) | 3–4 high-frequency variables |
| Rate | Native turbulence rate (to confirm with NEON; believed to be 20 Hz) | Native rate of chosen streams |
| Alignment | Streams must be time-aligned using sensor timestamps (done in the plugin in v1) | Loose alignment acceptable |
| Window | ~30-minute averaging periods (plugin accumulates via *subscribe*) | 5-minute accumulation |
| Latency tolerance | Minutes | Minutes |
| Output to Beehive | Derived fluxes and diagnostics | Summary statistics and anomaly flags |

Both use cases tolerate latency in minutes, so mirroring delay (typically under a second) and short throttling delays are not a concern.

## Hello World Examples

#### 1. Simple pull and print

The plugin pulls meteorological values (temperature, pressure, humidity, and wind speed) and prints them to the console for CLI testing. It does no computation. Optionally, it publishes to Beehive to exercise the full publish path to the cloud, subject to the data-release question below.

#### 2. Simple computation

The plugin subscribes to 3 or 4 high-frequency streams and accumulates them into a small array. After 5 minutes of data, it runs a simple statistical anomaly detection and publishes the basic statistics and any anomaly flags to Beehive. This example also demonstrates gap handling and the replay mode.

## Simplest Path to Hello World #1

The goal is to get one real NEON value printed by a Sage plugin with the fewest moving parts, then harden. Each step has a clear "done" check.

**Step 0 — Gather from NEON (blocking).**
- Kafka bootstrap address(es) reachable from the tower network, and the security protocol (SASL mechanism, TLS).
- A read-only account for the mirror, with `READ` and `DESCRIBE` on one or two meteorological topics.
- How Avro schemas are provided: Schema Registry URL, a `_schemas` topic we may read, or `.avsc` files.
- *Done when:* we have a topic name, credentials, and its schema.

**Step 1 — Prove connectivity from the node (no Kafka install).**
From the Sage node, run a one-off consumer (`kcat`, or a 20-line Python script using `confluent-kafka` + `fastavro`) directly against NEON for one topic. Decode a message and print it.
- *Done when:* a decoded NEON record prints on the node. This validates network path, credentials, partitions, Avro framing (plain Avro vs. Confluent wire format with a 5-byte schema-ID header), and timestamp fields, before any infrastructure exists.

**Step 2 — Stand up local Kafka + MM2 for one topic.**
One pod on the node's k3s, `apache/kafka` image, single-node KRaft, with a small persistent volume. Run MM2 in dedicated mode in the same pod. Minimal MM2 configuration:

```properties
clusters = neon, local
neon.bootstrap.servers = <from NEON>
neon.security.protocol = <from NEON>        # plus SASL settings, from a node secret
local.bootstrap.servers = localhost:9092

neon->local.enabled = true
neon->local.topics = <one met topic>
local->neon.enabled = false

replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy
offset-syncs.topic.location = target   # never write to NEON's cluster
sync.topic.configs.enabled = false     # local retention, not NEON's
sync.topic.acls.enabled = false
emit.heartbeats.enabled = false
emit.checkpoints.enabled = false
sync.group.offsets.enabled = false
replication.factor = 1
```

The last group of settings matters: by default MM2 writes some bookkeeping topics to the *source* cluster. With these settings MM2 only reads from NEON. Set broker defaults for retention (`log.retention.ms`, `log.segment.ms`) and `auto.create.topics.enable=false`.
- *Done when:* `kafka-console-consumer.sh` inside the pod shows the topic filling at the expected rate.

**Step 3 — Minimal client.**
About 100 lines of Python: `connect()`, `list_topics()`, `latest(topic)`, and `subscribe(topic)` (a generator), using `confluent-kafka` (which has arm64 wheels) and `fastavro`. Schemas for the one topic are bundled with the client or mounted as a file (the simplest option; see open issues).
- *Done when:* the client runs on a laptop against the Docker version of the same setup.

**Step 4 — Hello world plugin on the node.**
Package Hello World #1 as a Sage plugin using the client, and run it on the node (e.g. with `pluginctl`).
- *Done when:* meteorological values print in the plugin's logs.

**Step 5 — Lock it down (before anyone else's plugin runs).**
Add the SASL listener for MM2, the `ANONYMOUS` read-only ACLs, the NetworkPolicy blocking direct access to NEON, a starting `producer_byte_rate` quota on MM2 at the local Kafka, and conservative MM2 fetch settings toward NEON.
- *Done when:* a test plugin can read but fails to produce or create a topic, and cannot reach NEON's Kafka.

**Deferred past hello world:** `window()`, gap/staleness signals, typed errors beyond the basics, the usage-statistics sidecar, the replay dataset, and Beehive publishing. Hello World #2 needs `window()` or `subscribe()` with gap handling, plus the replay dataset.

## Open Design Issues

- **Avro schemas.** Does NEON use a Confluent Schema Registry (messages prefixed with a schema ID) or plain Avro? Options, simplest first:
  1. Bundle `.avsc` files with the client or mount them from a node ConfigMap. Simple, but must be updated when NEON changes a schema.
  2. Mirror NEON's `_schemas` topic (if NEON grants read access) and have the client read it directly to build its schema-ID → schema map. No extra service, and it follows schema changes automatically.
  3. Mirror `_schemas` and run a local read-only Schema Registry on it. Standard tooling, but adds another Java service to the node.

  Proxying plugins to NEON's registry is ruled out because it would require NEON credentials outside the mirror.
- **Network path and credentials.** What is the bootstrap address reachable from the tower network, and which authentication mechanism does NEON's Kafka use?
- **Rates.** What are the native rates of the turbulence and meteorological streams of interest? These are needed to size retention and set staleness thresholds.
- **Starting limits.** What allowlist and upstream byte-rate quota are acceptable to NEON? Will NEON also set a quota on their side?
- **Retention.** How much local history should the cache hold (e.g. 1 hour)? This bounds `window()`.
- **Data release policy.** Is republishing provisional, real-time NEON values to Beehive acceptable, or should Beehive publishing be limited to derived products?
- **Node footprint.** Kafka and MM2 together need roughly 1–2 GB of RAM (two JVM processes). Is that acceptable on the target node hardware? If not, Redpanda (Kafka-compatible, single C++ binary) is an alternative, but it has a different license and still needs MM2.
- **Second message system.** Sage nodes already use their own messaging for the plugin → Beehive path. Adding Kafka is acceptable as a contained, read-only cache, but this should be acknowledged with the Sage infrastructure team.

## Future Versions

- **Reduced-rate mirroring.** MM2 copies every message; it cannot sample. To mirror a topic at, e.g., 1 Hz instead of 20 Hz, add a small processing step: a stream processor that consumes the full-rate topic locally and writes a downsampled topic (e.g. `<topic>.1hz`). This adds a second writer and a naming convention. Note that it reduces local fan-out and storage, not upstream load; reducing upstream load requires NEON to publish a lower-rate topic.
- **On-demand mirroring.** Mirror a topic only while some plugin is reading it, by having a small controller update MM2's topic list through its REST API (requires running MM2 under Kafka Connect rather than dedicated mode). Only worthwhile if the allowlist grows large.
- **Per-plugin identity and quotas.** Have the Sage scheduler issue per-plugin credentials (SASL/SCRAM or mTLS). This makes per-plugin usage statistics trustworthy and enables Kafka's built-in per-user `consumer_byte_rate` quotas for fair share. Not needed while plugins play well together.
- **Topic catalog.** A small compacted topic (e.g. `_neon_catalog`) with descriptions and nominal rates, so `list_topics()` can return them.
- **Time-alignment helper.** A client utility that aligns several streams by sensor timestamp, useful for use case A.
- **Plugin-side error reporting.** The client reports decode errors, gaps, and staleness events to Beehive, alongside the broker-side metrics.
