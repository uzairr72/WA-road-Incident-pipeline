# WA Road Incidents — Real-Time Streaming Pipeline

### Project Brief---

## 1\. Overview

A real-time data pipeline that ingests live Western Australian road incident
data from Main Roads WA every 5 minutes, streams it through Azure Event Hubs,
processes it with Databricks Structured Streaming to detect road-incident
hotspots, and serves the results through a live map and analytics dashboard —
built entirely on managed Azure services.

**One-line pitch:**

> "I built a real-time streaming pipeline that ingests live WA road incident
> data every 5 minutes, streams it through Azure Event Hubs, processes it with
> Databricks Structured Streaming to detect incident hotspots, stores results
> in Delta Lake, and serves a live map and analytics dashboard — the whole
> pipeline automated and running on a schedule with zero manual intervention."

\---

## 2\. Architecture

```
Main Roads WA — WebEOC Road Incidents (ArcGIS FeatureServer, public, no auth)
        │  polled every 5 minutes
        ▼
Databricks Job — Producer Task
  • reads watermark (Delta table)
  • fetches current incidents from the API
  • filters to genuinely new/updated records (Python-side date comparison)
  • pushes new records to Event Hubs
  • updates the watermark
        │
        ▼
Azure Event Hubs  (Standard tier, Kafka-compatible endpoint)
  topic: wa-road-incidents
        │  consumed every 5 minutes (dependent task, runs after producer)
        ▼
Databricks Job — Structured Streaming Task
  • reads from Event Hubs via Spark Structured Streaming
  • parses JSON into typed columns
  • computes rolling 1-hour incident counts per road (windowed aggregation)
  • flags hotspots (≥3 incidents/hour on a road)
        │
        ├──▶ Delta table: raw\_incidents    (individual incident records)
        └──▶ Delta table: road\_hotspots    (hourly counts per road)
                │
                ▼
        Databricks SQL Dashboard
        • live incident map (lat/long markers)
        • hotspot bar chart, colour-flagged by threshold
```

### Component summary

|Component|Service|Role|
|-|-|-|
|Data source|Main Roads WA ArcGIS FeatureServer|Live incident feed, 5-min cadence, no auth required|
|Orchestration|Azure Databricks Jobs|Scheduled, dependent-task execution (producer → streaming)|
|Message broker|Azure Event Hubs (Standard)|Kafka-protocol-compatible buffer between ingestion and processing|
|Stream processing|Databricks Structured Streaming|Parsing, windowed aggregation, hotspot detection|
|Watermark storage|Delta table|Tracks incremental ingestion state|
|Result storage|Delta tables (`raw\_incidents`, `road\_hotspots`)|Queryable, persistent output|
|Dashboard|Databricks SQL (Lakeview)|Map + hotspot visualisation|

\---

## 3\. Data Source

**Endpoint:**

```
https://services2.arcgis.com/cHGEnmsJ165IBJRM/arcgis/rest/services/WebEoc\_RoadIncidents/FeatureServer/1/query
```

Public, no API key. Query parameters: `where=1=1`, `outFields=\*`, `f=json`,
`outSR=4326` (forces standard lat/long output instead of the service's default
Web Mercator projection).

**Key fields:** `GlobalID` (stable unique ID), `UpdateDate`, `IncidentTy`,
`Road`, `Region`, `Suburb`, `TrafficImp`, plus `geometry.x`/`geometry.y`
(longitude/latitude).

\---

## 4\. Key Design Decisions

|Decision|Reasoning|
|-|-|
|Azure-native over self-hosted Docker (Kafka/Airflow/Cassandra)|A full session of Docker Desktop/WSL2 infrastructure instability, unrelated to the pipeline's own logic, made local infra unreliable; Azure/Databricks also better matches Perth's actual DE job market (Azure, Databricks, Power BI-heavy)|
|Delta table for watermark, not Azure SQL|Avoids provisioning and paying for a separate resource; keeps everything inside Databricks; Delta Lake is itself a relevant, demoable skill|
|Client-side (Python) date filtering, not server-side `where` filtering|The API's `UpdateDate` field is declared `esriFieldTypeString`, not a true date type — server-side date comparison silently returned zero matches. Filtering in Python after parsing with `strptime` sidesteps the API's loose typing|
|`trigger(availableNow=True)` over continuous streaming|Required by this workspace's serverless compute tier; also a good architectural fit — pairs cleanly with a 5-minute scheduled Job rather than an always-on stream|
|`outputMode("complete")` for hotspot aggregation|Delta's streaming sink doesn't support `"update"` mode; `"complete"` is correct at this data scale. At larger scale, `.foreachBatch()` with a Delta `MERGE` would be the production-grade choice|
|Combined Job with dependent tasks, not two fully independent Jobs|Guarantees the streaming task never runs before that cycle's data has been produced, at the cost of some architectural decoupling purity|

\---

## 5\. Known Limitations

* **Azure Schema Registry was attempted but not completed.** Its Python SDK
requires Microsoft Entra ID (service principal) authentication; creating an
App Registration was blocked by a directory-level policy on this Azure for
Students / university-tied account (`"you do not have access"` /
`Users can register applications = No`). This is an institutional Entra ID
permission boundary, separate from Azure subscription permissions, and not
fixable from the student account side. A real Avro schema was still
designed and registered directly in the Event Hubs Schema Registry via the
Azure Portal.
* **Dashboard auto-refresh scheduling** was not available in this workspace
tier's UI. The underlying data pipeline (both Databricks Jobs) is fully
automated on a 5-minute schedule regardless; the dashboard reflects current
data on open/manual refresh.
* **Aggregation uses `"complete"` output mode**, rewriting the full result
table each run — appropriate at this data volume, not intended to
demonstrate production-scale incremental upsert patterns.

\---

## 6\. Tools \& Technologies

Azure Event Hubs · Azure Databricks (serverless compute) · Databricks
Structured Streaming · Delta Lake · Databricks Jobs \& Workflows · Databricks
SQL / Lakeview Dashboards · PySpark · Python (`requests`, `kafka-python`) ·
ArcGIS REST API · SQL

\---

## 7\. Reproduction Notes

1. Provision an Event Hubs namespace (Standard tier — required for the Kafka
endpoint) and one Event Hub (`wa-road-incidents`).
2. In Databricks: create the watermark Delta table, then the producer
notebook (reads watermark → calls API → filters → pushes to Event Hubs →
updates watermark).
3. Create a Structured Streaming notebook (reads from Event Hubs → parses →
windowed aggregation → writes to two Delta tables), using
`trigger(availableNow=True)` and Unity Catalog Volume checkpoint paths.
4. Schedule both as tasks within one Databricks Job (5-minute schedule,
streaming task depends on producer task).
5. Build a Databricks SQL Dashboard from the two Delta tables.

