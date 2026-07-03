# WA Road Incidents — Real-Time Streaming Pipeline

A real-time data pipeline that ingests live Western Australian road incident
data from Main Roads WA every 5 minutes, streams it through Azure Event Hubs,
processes it with Databricks Structured Streaming to detect road-incident
hotspots, and serves the results through a live map and analytics dashboard —
built entirely on managed Azure services, fully automated on a schedule.

![Status](https://img.shields.io/badge/status-automated-brightgreen)
![Platform](https://img.shields.io/badge/platform-Azure-blue)

---

## Architecture

```
Main Roads WA — WebEOC Road Incidents (ArcGIS FeatureServer, public, no auth)
        │  polled every 5 minutes
        ▼
Databricks Job — Producer Task
  reads watermark → fetches API → filters new records → pushes to Event Hubs → updates watermark
        │
        ▼
Azure Event Hubs  (Standard tier, Kafka-compatible)
  topic: wa-road-incidents
        │  consumed every 5 minutes (dependent task)
        ▼
Databricks Job — Structured Streaming Task
  parses JSON → windowed 1-hour aggregation per road → hotspot flag (≥3/hr)
        │
        ├──▶ Delta table: raw_incidents
        └──▶ Delta table: road_hotspots
                │
                ▼
        Databricks SQL Dashboard — live map + hotspot chart
```

## Why this project

Main Roads WA publishes incident data (crashes, closures, hazards, roadworks)
on a genuine 5-minute cadence — a real latency problem, not a simulated one.
That makes a message-broker-based streaming architecture an actual design
decision rather than a tooling choice for its own sake.

## Tools & Technologies

Azure Event Hubs · Azure Databricks (serverless) · Structured Streaming ·
Delta Lake · Databricks Jobs & Workflows · Databricks SQL (Lakeview)
Dashboards · PySpark · Python (`requests`, `kafka-python`) · ArcGIS REST API

## Repo contents

```
notebooks/
  producer_notebook.py      Watermark read → API fetch → filter → push to Event Hubs
  streaming_notebook.py     Structured Streaming: parse, aggregate, write to Delta
docs/
  Project_Brief.md          Architecture, component summary, design decisions
  Learning_Guide.md         Full build log: every step, concept, and bug fixed
```

## Key design decisions

| Decision | Why |
|---|---|
| Azure-native, not self-hosted Docker | Local Docker Desktop/WSL2 infrastructure proved unreliable independent of the pipeline code; Azure/Databricks also matches the target job market better |
| Watermark stored as a Delta table, not a separate database | No extra Azure resource needed; keeps everything inside Databricks |
| Date filtering done client-side in Python | The source API's `UpdateDate` field is a string type, not a true date — server-side date filtering silently returned zero results |
| `trigger(availableNow=True)` for streaming | Required on serverless compute; also a natural fit for a 5-minute scheduled Job over an always-on stream |

Full reasoning for every decision, including the ones that didn't work out, is
in [`docs/Learning_Guide.md`](docs/Learning_Guide.md).

## Known limitations

- **Azure Schema Registry** was designed and registered, but full code
  integration was blocked by a Microsoft Entra ID directory permission
  (App Registration creation disabled on this Azure for Students account) —
  an institutional policy, not a technical dead end. Documented in full in the
  Learning Guide.
- **Dashboard auto-refresh scheduling** wasn't available in this workspace
  tier; the underlying data pipeline is fully automated regardless — the
  dashboard reflects live data on open/manual refresh.

## Setup / reproduction

1. Provision an Event Hubs namespace (**Standard tier** — required for the
   Kafka-compatible endpoint) and one Event Hub (`wa-road-incidents`).
2. In Databricks: create the watermark Delta table, then import
   `notebooks/producer_notebook.py`, filling in your own Event Hubs
   connection string.
3. Import `notebooks/streaming_notebook.py`, same connection string.
4. Schedule both as tasks within one Databricks Job (5-minute schedule,
   streaming task depends on the producer task).
5. Build a Databricks SQL Dashboard against the two resulting Delta tables.

---

*Built as a portfolio project targeting Perth, WA's Azure/Databricks-heavy
data engineering job market.*
