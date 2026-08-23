# Medallion Architecture — The Complete Guide

## 🧠 /firstprinciples — What Is Medallion Architecture, Really?

Raw data arriving into a data platform is almost never immediately usable — it's messy, inconsistent, sometimes duplicated, and mixed with fields nobody downstream actually needs. Throwing every consumer (analysts, ML models, dashboards) directly at that raw data means **everyone re-invents the same cleaning logic**, inconsistently, over and over.

**Medallion Architecture** (popularized by Databricks, but a generalization of long-standing data warehousing practice) solves this by organizing data into **progressive quality tiers**, each one refining the previous:

- **Bronze**: raw data, ingested as-is, with minimal or no transformation — the unfiltered source of truth.
- **Silver**: cleaned, validated, conformed data — deduplicated, type-corrected, joined with reference data, ready for general analytical use.
- **Gold**: business-level, aggregated, purpose-built data — the polished, curated layer that directly powers dashboards, reports, and ML features.

> Medallion Architecture is a data organization pattern that structures a data lake (or lakehouse) into Bronze, Silver, and Gold layers, each applying progressively more cleaning, validation, and business logic — so data quality improves in well-defined, auditable stages rather than all at once.

**The one-sentence definition:**
> Medallion Architecture is a layered data refinement pipeline — raw in, progressively cleaner and more business-ready data out — that gives every layer a clear, single responsibility instead of one massive, tangled transformation.

---

## 🥉🥈🥇 Simple Analogy

Imagine a **mineral refinery processing raw ore into finished metal**.

- **Bronze** = the **raw ore**, straight out of the ground — full of rock, dirt, and impurities, but preserved exactly as extracted. Nobody uses raw ore directly, but you never throw it away either — it's your permanent record of what was actually mined.
- **Silver** = the **smelted, purified metal** — impurities removed, consistent composition verified, ready to be shaped into standard bars. Still a general-purpose material, usable by many different downstream manufacturers.
- **Gold** = the **finished, polished product** — a specific jewelry piece or engineered component, shaped for a **specific purpose**, ready to be sold or used directly by an end customer.

You'd never hand raw ore to a jeweler, and you'd never re-mine ore every time you need a new piece of jewelry — you refine it **once**, in stages, and each stage produces something more broadly reusable than trying to jump straight from ore to jewelry.

---

## 👁️ /visualizelearning — The Three-Layer Flow

```
┌────────────────────────────────────────────────────────────────────┐
│  SOURCES: APIs, databases, event streams, files, IoT sensors, etc. │
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
┌────────────────────────────────────────────────────────────────────┐
│                          🥉 BRONZE LAYER                          │
│   Raw, as-ingested data — minimal transformation                   │
│   Schema: whatever the source gives you (often semi-structured)    │
│   Purpose: permanent historical record, reprocessing source        │
└────────────────────────────┬───────────────────────────────────────┘
                             │  clean, dedupe, validate, conform types
                             ▼
┌───────────────────────────────────────────────────────────────────────┐
│                          🥈 SILVER LAYER                             │
│   Cleaned, validated, joined with reference/dimension data            │
│   Schema: enforced, consistent, query-ready                           │
│   Purpose: general-purpose analytical base for many downstream needs  │
└────────────────────────────┬──────────────────────────────────────────┘
                             │  aggregate, apply business logic, model
                             ▼
┌───────────────────────────────────────────────────────────────────────┐
│                          🥇 GOLD LAYER                               │
│   Business-level aggregates, KPIs, curated feature tables             │
│   Schema: purpose-built for specific consumers                        │
│   Purpose: directly powers dashboards, reports, ML features, APIs     │
└────────────────────────────┬──────────────────────────────────────────┘
                             ▼
                 BI Dashboards, ML Models, Reports, APIs
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

### Bronze layer internals
- Data is ingested with **minimal transformation** — often just appending metadata columns (ingestion timestamp, source file name, batch ID) to the raw payload for lineage/auditability.
- Typically stored in a format that preserves the original structure closely (raw JSON, Avro, or a lightly-typed Parquet/Delta table) — schema enforcement is usually **loose or absent** here, since the goal is fidelity to the source, not usability.
- Because Bronze is the **permanent historical record**, it's usually **append-only** — new data is added, existing data is rarely modified or deleted, enabling **full reprocessing** of downstream layers from scratch if transformation logic changes.

### Silver layer internals
- Transformations applied here typically include: **deduplication**, **null/type handling**, **schema enforcement**, **conforming to standard formats** (e.g., unifying date formats across sources), and **joining with reference/dimension data** to enrich records.
- Data quality rules and validation checks are commonly applied at this stage — records that fail validation might be quarantined (routed to a separate "quarantine" table) rather than silently dropped or silently let through.
- Silver tables are typically modeled closer to **normalized, entity-centric structures** (e.g., one table per business entity: `customers`, `orders`, `products`) — general-purpose enough to serve many different Gold-layer use cases.

### Gold layer internals
- This is where **business logic, aggregation, and denormalization** happen — joining multiple Silver tables together, computing metrics (`daily_active_users`, `monthly_revenue_by_region`), and shaping data specifically around how it will be **consumed** (e.g., a single wide table optimized for a specific dashboard's queries).
- Gold tables are often **denormalized** deliberately (trading some redundancy for query simplicity/performance), unlike Silver's more normalized structure.
- This layer frequently also includes **ML feature tables** — pre-computed, curated features ready to be pulled directly into a training pipeline or feature store.

```
   BRONZE (raw)                         SILVER (cleaned)            GOLD (business)
┌────────────────┐                    ┌─────────────────┐          ┌─────────────────┐
│ {"cust_id":"7",│                    │ customer_id: 7  │          │ region: APAC    │
│  "amt":"19.99",│  ───────────────>  │ amount: 19.99   │  ────>   │ total_rev: 4.2M │
│  "ts":16920... │  clean,            │ order_date:     │ aggregate│ top_product: X  │
│  "dup":true}   │  dedupe,           │  2026-08-01     │ business │ (one row per    │
│  (raw, messy)  │  type-fix          │ logic           │          │  region/month)  │
└────────────────┘  (typed, deduped)  └─────────────────┘          └─────────────────┘
```

### Reprocessing and lineage
- Because Bronze preserves raw data indefinitely, a bug discovered in Silver/Gold transformation logic can be **fixed and the affected layers fully reprocessed from Bronze** — without needing to re-fetch data from the original source system (which may not even have it anymore, e.g., an API with limited retention).
- This is one of Medallion Architecture's most valuable properties: **Bronze acts as a permanent, replayable source of truth**, decoupling downstream correctness from the availability/retention policies of upstream source systems.

---

## 🏗️ /architecture — How It Fits Into a Modern Data Platform

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA LAKEHOUSE                                  │
│  (e.g., Delta Lake, Apache Iceberg, Apache Hudi on cloud object storage)│
│                                                                         │
│   🥉 BRONZE (Delta/Iceberg tables) ──> 🥈 SILVER ──> 🥇 GOLD          │
│         Orchestrated by a scheduler (Airflow, Databricks Workflows,     │
│         dbt, or similar), with each layer as its own pipeline stage     │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        Consumers: BI tools (Tableau, Power BI), ML pipelines,
        reverse-ETL to operational systems, ad-hoc SQL analysts
```

Medallion Architecture is a **logical organization pattern**, not tied to a specific technology — it's commonly implemented with Delta Lake, Apache Iceberg, or Apache Hudi on cloud object storage (S3, ADLS, GCS), orchestrated by tools like Airflow, dbt, or Databricks Workflows, but the same Bronze/Silver/Gold thinking applies even in traditional data warehouses.

---

## 🔄 /flow — Full Pipeline Lifecycle

```
1. Raw data lands from sources (APIs, CDC streams, file drops, events)
2. BRONZE ingestion job: append raw data + metadata columns, minimal transform
3. SILVER transformation job: read Bronze, clean/dedupe/validate/conform,
     write to Silver tables (often incremental, processing only new Bronze data)
4. GOLD transformation job: read Silver, join/aggregate/apply business logic,
     write to Gold tables (often fully or partially recomputed per refresh)
5. Consumers (BI tools, ML pipelines, APIs) query Gold directly for
     day-to-day use; Silver for more flexible/custom analytical needs;
     Bronze rarely queried directly, mainly for reprocessing/audit
6. If a bug is found in Silver/Gold logic: fix the transformation code,
     REPROCESS from Bronze — no need to re-fetch from the original source
```

---

## 💻 /examples — Representative Pipeline Code

```python
# BRONZE — minimal transformation, preserve raw structure + add metadata
raw_df = spark.read.json("s3://landing-zone/orders/")
bronze_df = raw_df.withColumn("ingestion_ts", F.current_timestamp()) \
                   .withColumn("source_file", F.input_file_name())
bronze_df.write.format("delta").mode("append").save("s3://lake/bronze/orders")

# SILVER — clean, dedupe, enforce types, join with reference data
bronze = spark.read.format("delta").load("s3://lake/bronze/orders")
silver_df = (
    bronze
    .dropDuplicates(["order_id"])
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("order_date", F.to_date("order_date"))
    .join(spark.table("dim_customers"), "customer_id", "left")
    .filter(F.col("amount").isNotNull())   # basic validation
)
silver_df.write.format("delta").mode("overwrite").save("s3://lake/silver/orders")

# GOLD — business aggregation, purpose-built for a dashboard
silver = spark.read.format("delta").load("s3://lake/silver/orders")
gold_df = (
    silver.groupBy("region", F.date_trunc("month", "order_date").alias("month"))
          .agg(F.sum("amount").alias("total_revenue"),
               F.countDistinct("customer_id").alias("unique_customers"))
)
gold_df.write.format("delta").mode("overwrite").save("s3://lake/gold/monthly_revenue")
```

---

## 🏭 /production — Real-World Use Cases

1. **Enterprise data platforms consolidating many source systems**: companies ingesting data from dozens of operational systems (CRM, ERP, event streams, third-party APIs) use Bronze as a unified, consistent landing zone regardless of source format, then apply shared Silver-layer cleaning logic once instead of per-consumer.
2. **ML feature engineering pipelines**: feature stores are commonly built directly on top of Gold-layer tables, since Gold already represents clean, business-validated, aggregated data — reducing duplicated feature logic across multiple ML teams.
3. **Regulatory/compliance-driven data retention**: industries with strict audit requirements (finance, healthcare) rely on Bronze's raw, immutable, append-only nature as a defensible historical record, independent of whatever transformation logic existed at any point in time.
4. **Self-service BI enablement**: organizations expose Gold-layer tables (denormalized, business-friendly naming, pre-aggregated) directly to business analysts via BI tools, while keeping Silver/Bronze restricted to data engineering teams — reducing the risk of analysts misinterpreting raw or semi-cleaned data.
5. **Incremental migration from legacy data warehouses**: teams migrating from traditional ETL/warehouse architectures to a lakehouse often adopt Medallion Architecture as the target design, mapping existing staging/ODS/mart layers onto Bronze/Silver/Gold respectively, easing the conceptual transition.

---

## 🚀 /optimization — Tuning Levers

| Lever | What it does |
|---|---|
| **Incremental processing (not full reprocessing) for Silver/Gold** | Only process newly-arrived Bronze data where possible, using techniques like Delta Lake's Change Data Feed, dramatically reducing pipeline runtime |
| **Partition Bronze/Silver tables by ingestion date** | Enables efficient pruning for both incremental processing and historical queries |
| **Apply data quality checks at the Silver boundary** | Catching issues early (Bronze→Silver) prevents bad data from propagating into expensive-to-recompute Gold aggregates |
| **Use table formats supporting ACID transactions** (Delta Lake, Iceberg, Hudi) | Enables safe concurrent reads/writes and reliable incremental updates across all three layers |
| **Denormalize deliberately at Gold, not earlier** | Keeps Silver flexible/reusable while making Gold as fast and simple as possible for its specific consumers |
| **Materialize only the Gold tables actually queried frequently** | Avoid maintaining unused Gold tables that add pipeline cost without corresponding consumer value |
| **Version/tag transformation logic alongside data** | Supports reliable full reprocessing from Bronze when business logic changes, without ambiguity about which logic produced which historical Gold output |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Skipping Bronze and transforming directly from source to Silver/Gold** — losing the permanent, replayable raw record; if a downstream bug is found, or source data retention expires, full reprocessing becomes impossible.
2. **Over-engineering Bronze with heavy transformation** — defeating its purpose as a raw, faithful record; transformation logic bugs then also corrupt what should have been an immutable source of truth.
3. **Allowing business logic to leak into Silver** — Silver should stay general-purpose (cleaning/conforming only); embedding business-specific aggregation logic there makes it harder to reuse Silver for multiple different Gold-layer needs.
4. **Fully reprocessing Gold (or even Silver) on every run when incremental processing is possible** — unnecessarily expensive compute costs, especially as historical data volume grows over time.
5. **Exposing Silver or Bronze directly to business users/BI tools** — leads to inconsistent metric definitions across the organization when different analysts apply their own ad-hoc business logic on top of the same "cleaned but not yet business-modeled" data.
6. **Not applying data quality checks at layer boundaries** — letting invalid or malformed data silently flow from Bronze through Silver into Gold, where it's much more expensive (and harder) to trace back and correct.

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: What are the three layers of Medallion Architecture?**
> A: Bronze (raw, as-ingested data), Silver (cleaned, validated, conformed data), and Gold (business-level, aggregated, purpose-built data for direct consumption).

**Q2: Why keep a raw Bronze layer instead of transforming data immediately?**
> A: It preserves a permanent, replayable historical record independent of source system retention, enabling full reprocessing of downstream layers if transformation logic changes or bugs are discovered.

**Q3: What kind of transformations happen at the Silver layer specifically?**
> A: Deduplication, type/schema enforcement, null handling, conforming to standard formats, and joining with reference/dimension data — general-purpose cleaning, not business-specific aggregation.

**Q4: Why is Gold often denormalized while Silver is more normalized?**
> A: Silver stays general-purpose and reusable across many downstream needs (normalized, entity-centric); Gold is shaped specifically around how it will be consumed (dashboards, reports), where denormalization trades some redundancy for query simplicity and performance.

**Q5: Should business analysts query the Silver or Bronze layer directly?**
> A: Generally no — exposing Silver/Bronze to business users risks inconsistent metric definitions, since each analyst might apply their own ad-hoc business logic; Gold is the intended, curated interface for that audience.

**Q6: Is Medallion Architecture tied to a specific technology like Delta Lake?**
> A: No — it's a logical organizational pattern; it's commonly implemented with Delta Lake, Apache Iceberg, or Apache Hudi, but the same Bronze/Silver/Gold thinking applies in traditional data warehouses too.

**Q7: What's a common mistake when implementing this pattern?**
> A: Skipping the Bronze layer and transforming directly from source into Silver/Gold, losing the ability to fully reprocess historical data if transformation logic changes or a bug is found.

---

## ✅ /keytakeaways

- **Medallion Architecture** organizes data into **Bronze (raw) → Silver (cleaned) → Gold (business-ready)** layers, each with a distinct, well-defined responsibility.
- **Bronze** is the permanent, append-only, raw historical record — enabling full reprocessing independent of source system retention.
- **Silver** applies general-purpose cleaning, validation, and conforming — reusable across many different downstream Gold use cases.
- **Gold** applies business logic, aggregation, and often deliberate denormalization — purpose-built for direct consumption by dashboards, reports, and ML pipelines.
- It's a **logical pattern, not a specific technology** — commonly built on Delta Lake, Iceberg, or Hudi, but applicable to traditional warehouses too.
- Keep **business logic out of Silver**, keep **Bronze immutable and lightly transformed**, and expose **Gold (not Silver/Bronze) to business users** for consistent, trustworthy metrics.