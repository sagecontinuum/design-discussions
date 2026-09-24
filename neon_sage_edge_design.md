# NEON-Sage Edge: Broker Service and `neon_sage_edge` Client

Sage nodes deployed within NEON towers have the possibility to pull raw data from some of the internal data streams available in real time within the tower. NEON uses Kafka to provide access to the NEON tower data.

This project provides scientists with a controlled, lightweight way to pull specific NEON data streams and use them for real-time calculations at the edge. It has two parts:

- **NEON Broker Service** — a single node-resident service that is the only component permitted to connect to the NEON Kafka infrastructure. It holds the Kafka credentials, maintains upstream subscriptions, enforces throttles and quotas, and records usage statistics.
- **`neon_sage_edge` client library** — a thin Python client, used by Sage edge applications (plugins), that talks to the broker over a local node API. It holds no credentials and never connects to Kafka directly.

Sage plugins will not be given direct access to Kafka data streams. All access goes through the broker, and this is enforced by the node's network policy, not just by convention.

To focus the design, two driving use cases guide the requirements:

A) A lightweight, at-the-edge, real-time eddy covariance approximation.

B) A lightweight anomaly detection example that uses relatively high-frequency data.

## Architecture

```
 NEON Kafka (tower)
        │   one upstream subscription per topic
        ▼
 ┌─────────────────────────┐
 │  NEON Broker Service    │  credentials, allowlist, throttles,
 │  (node-resident)        │  per-topic ring buffers, usage stats → Beehive
 └─────────────────────────┘
        │   local node API (fan-out)
        ▼
 plugin A   plugin B   plugin C ...   (each uses neon_sage_edge client)
```

Because the broker keeps one upstream subscription per topic and fans data out locally, the load placed on NEON's Kafka infrastructure depends on the number of distinct topics requested, not on the number of plugins running.

## Key Design Components

1) **Simple throttling.** The broker enforces:
   - an *upstream* limit on total topics, message rate, and bytes pulled from NEON Kafka, which protects NEON's infrastructure

Note: Later, we can explore a *per-plugin* quota if "fairness" becomes an issue (future work, see below)

   Limits are set by Sage/NEON administrators through node configuration. Plugins cannot change them. Because enforcement is centralized in the broker, accidentally launching 100 copies of a plugin is throttled correctly: the upstream load does not grow.
   
2) **Concurrent access.** Multiple plugins can subscribe to the same or different topics at the same time. The broker fans a single upstream stream out to all subscribers.

3) **Credential isolation and enforcement.** NEON Kafka credentials are stored as a node secret mounted only into the broker. Plugin pods are blocked from reaching the NEON Kafka brokers by network policy, so the broker is the only access path in practice.

4) **Access policy.** The broker exposes only topics on an allowlist provided by NEON. Requests for other topics return a clear error.

5) **Plugin identity and usage statistics.** The broker identifies each calling plugin and publishes usage metrics to Beehive. At minimum these include per-plugin and per-topic subscriptions, messages and bytes delivered, throttle events, and errors; they also include upstream bytes and message rates.

6) **Topic discovery.** The client provides `list_topics()`, which returns the allowlisted topic names, descriptions, and nominal publishing rates, so discovery stays accurate as NEON's streams evolve. External documentation will explain how NEON topic names map to sensors and measurements. The API uses NEON's own topic names.

7) **Data access model.** The client supports:
   - *subscribe*: receive messages for a topic as they arrive;
   - *latest*: get the most recent value for a topic; and
   - *window*: get the last N seconds of a topic from the broker's ring buffer.

   Each topic has a bounded buffer. If a plugin falls behind, the oldest undelivered messages are dropped and the plugin is notified through a gap/overflow indicator; it is never silently stalled.

8) **Data semantics.** Messages delivered to plugins are decoded Python objects that carry:
   - NEON's sensor timestamp (not arrival time at the node);
   - any NEON quality flags; and
   - an indication of data level (raw/L0 vs. calibrated), with calibration applied or not as documented per topic (see Open Questions).

9) **Robust error handling.** Errors are explicit and typed. They cover throttle and quota exceeded, unknown or non-allowlisted topics, broker unavailable, upstream Kafka unavailable or tower network outage, broker restart during a subscription (the client reconnects automatically), and data gaps.

10) **Replay / mock mode.** The client can run off-tower against recorded NEON Kafka data using the same API. This lets scientists develop and test plugins without deploying to a NEON site.

## Driving Use Case Requirements

| | A) Eddy covariance approximation | B) Anomaly detection |
|---|---|---|
| Streams | Sonic anemometer (3D wind, sonic temperature) and gas analyzer (CO₂, H₂O) | 3–4 high-frequency variables |
| Rate | Native turbulence rate (to confirm with NEON; believed to be 20 Hz) | Native rate of chosen streams |
| Alignment | Streams must be time-aligned using sensor timestamps | Loose alignment acceptable |
| Window | ~30-minute averaging periods | 5-minute accumulation |
| Latency tolerance | Minutes | Minutes |
| Output to Beehive | Derived fluxes and diagnostics | Summary statistics and anomaly flags |

## Hello World Examples

Two simple examples demonstrate the client and broker.

#### 1. Simple pull and print

The plugin pulls meteorological values (temperature, pressure, humidity, and wind speed) and prints them to the console for CLI testing. It does no computation. Optionally, it publishes to Beehive to exercise the full publish path to the cloud, subject to the data-release question below.

#### 2. Simple computation

The plugin subscribes to 3 or 4 high-frequency streams and accumulates them into a small array. After 5 minutes of data, it runs a simple statistical anomaly detection and publishes the basic statistics and any anomaly flags to Beehive. This example also demonstrates gap handling and the replay mode.

## Open Questions

- **Data level:** Are the tower Kafka streams L0 (raw) data? If so, will the broker apply calibration coefficients, or will plugins receive raw values?
- **Encoding:** What message encoding and schema mechanism does NEON use?
- **Rates:** What are the native rates of the turbulence and meteorological streams of interest?
- **Data release policy:** Is republishing provisional, real-time NEON values to Beehive acceptable, or should Beehive publishing be limited to derived products?
- **Limits:** What upstream throttle limits and topic allowlist are acceptable good starting points?

## Future Work

- **Per Plugin** throttles.  Let's assume for now that everyone can play well together, and "fair share" is not an issue.  However, if we find problems, we might consider how to provide per-plugin limits.

