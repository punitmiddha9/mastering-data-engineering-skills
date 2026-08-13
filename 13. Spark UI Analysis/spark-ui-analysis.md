# Spark UI Analysis — The Complete Guide


## 🧠 /firstprinciples — What Is the Spark UI, Really?

Every running Spark application exposes a **built-in web dashboard** (by default at `http://<driver-host>:4040`, or via the **Spark History Server** for completed applications) that shows **exactly what the cluster is doing, in real time** — every job, stage, task, executor, and piece of cached data.

> The Spark UI is Spark's live (and historical) observability dashboard — it exposes the actual, measured execution of your job (not the plan, the *reality*), making it the primary tool for diagnosing performance problems, failures, and resource usage.

Where `.explain()` shows you **what Spark planned to do**, the Spark UI shows you **what Spark actually did** — task durations, shuffle sizes, memory usage, GC time, failures and retries — the ground truth needed to confirm or refute any theory about why a job is slow.

**The one-sentence definition:**
> The Spark UI is a real-time (and post-hoc, via History Server) dashboard exposing Jobs, Stages, Tasks, Executors, SQL query plans, and Storage — the essential toolkit for debugging Spark performance and failures using actual runtime data.

---

## 🩺 Simple Analogy

Imagine a **hospital patient hooked up to a full set of monitors** — heart rate, blood oxygen, blood pressure — versus just **reading the treatment plan on a clipboard**.

- The **treatment plan** (like `.explain()`) tells you what was *intended*: "administer medication at 9am, check vitals at 10am."
- The **monitors** (the Spark UI) tell you what's *actually happening, right now*: heart rate spiking, oxygen dropping in one specific reading — the real, measured signal that tells a doctor exactly where to look and what's actually going wrong.

You wouldn't diagnose a patient by reading the treatment plan alone — you'd check the monitors. Same with Spark: you don't diagnose a slow job by staring at code, you check the **Spark UI's live vitals**.

---

## 👁️ /visualizelearning — The Spark UI Tab Map

```
┌─────────────────────────────────────────────────────────────────┐
│                         SPARK UI TABS                           │
├─────────────────────────────────────────────────────────────────┤
│  JOBS       → one row per action triggered; overall duration,   │
│               status, and links to the stages within it         │
├─────────────────────────────────────────────────────────────────┤
│  STAGES     → one row per stage (split at shuffle boundaries);  │
│               task count, duration, shuffle read/write, I/O     │
├─────────────────────────────────────────────────────────────────┤
│  TASKS (within a stage) → per-task duration, GC time, shuffle   │
│               read size — THIS is where you spot skew           │
├─────────────────────────────────────────────────────────────────┤
│  STORAGE    → cached RDDs/DataFrames, storage level, size,      │
│               % in memory vs disk                               │
├─────────────────────────────────────────────────────────────────┤
│  ENVIRONMENT → all active Spark configs, JVM/classpath info     │
├─────────────────────────────────────────────────────────────────┤
│  EXECUTORS  → per-executor CPU/memory usage, task counts,       │
│               shuffle I/O, GC time — cluster-wide health view   │
├─────────────────────────────────────────────────────────────────┤
│  SQL / DATAFRAME → query-level view: initial vs FINAL plan      │
│               (post-AQE), visual DAG per query                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

### How the UI gets its data
- The **Driver** hosts a lightweight web server (Jetty) that serves the UI.
- As the job runs, Spark's internal **`SparkListener`** event bus fires events for every job/stage/task start, completion, and failure — the UI subscribes to these events and renders them live.
- After the application ends, if **event logging is enabled** (`spark.eventLog.enabled=true`), all these events are persisted to a log file, which the **Spark History Server** can later replay to reconstruct the exact same UI for a completed application — critical for debugging jobs after the fact.

```
   Running Application                     Completed Application
┌─────────────────────┐                   ┌──────────────────────────┐
│  Driver's built-in  │  event log file   │   Spark History Server   │
│  web UI (port 4040) │ ───────────────>  │   replays the event log  │
│  live, in-memory    │   (persisted)     │   to reconstruct the UI  │
└─────────────────────┘                   └──────────────────────────┘
```

### Reading the Jobs → Stages → Tasks hierarchy
- Each **Job** corresponds to one action (`.collect()`, `.count()`, `.write()`).
- Each Job breaks into **Stages** at shuffle boundaries (see: narrow vs wide transformations).
- Each Stage breaks into **Tasks**, one per partition — this is the finest-grained view, and where most real diagnosis happens.

### Key metrics to actually read on the Tasks page
| Metric | What it tells you |
|---|---|
| **Duration (min/median/max)** | A big gap between median and max = classic **data skew** signature |
| **Shuffle Read Size / Records** | Uneven values across tasks = skewed shuffle partitions |
| **GC Time** | High GC time relative to task duration = memory pressure, too many small objects, or insufficient executor memory |
| **Scheduler Delay** | High values suggest the cluster is resource-starved or over-subscribed |
| **Task Deserialization Time** | Unusually high = large closures or inefficient serialization |
| **Spill (Memory) / Spill (Disk)** | Non-zero spill = an operation (sort/aggregate/shuffle) didn't fit in allotted memory and spilled to disk — a common performance killer |

### Reading the SQL tab (Catalyst + AQE visibility)
- For DataFrame/SQL queries, the **SQL tab** shows a **visual DAG per query**, with each node annotated with real runtime metrics (rows produced, data size, time spent) — not just the plan Catalyst chose, but what actually happened at each operator.
- With **AQE enabled**, you can toggle between the **"Initial Plan"** (Catalyst's pre-execution estimate) and the **"Final Plan"** (post-AQE, reflecting runtime adjustments like coalesced partitions or switched join strategies) — a direct way to confirm AQE actually did something useful.

---

## 🏗️ /architecture — Where the UI Sits in the Spark Stack

```
┌─────────────────────────────────────────────────────────────────┐
│                           DRIVER                                │
│  ┌─────────────────┐      ┌────────────────────────────────┐    │
│  │ SparkContext /  │─────>│  SparkListener Bus (internal   │    │
│  │ Spark Session   │      │  event stream: job/stage/task  │    │
│  │                 │      │  start/end/failure events)     │    │
│  └─────────────────┘      └────────────┬───────────────────┘    │
│                                        ▼                        │
│                          ┌──────────────────────────┐           │
│                          │   Web UI (Jetty server)  │           │
│                          │   port 4040 by default   │           │
│                          └──────────────────────────┘           │
│                                        │                        │
│                          (optional) event log written           │
│                                        ▼                        │
│                          ┌────────────────────────┐             │
│                          │  Spark History Server  │             │
│                          │  (for completed apps)  │             │
│                          └────────────────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 /flow — A Practical Diagnostic Workflow

```
1. Job runs slower than expected / fails
2. Open Spark UI → JOBS tab → find the slow/failed job
3. Click into it → STAGES tab → identify the longest-running stage
4. Click into that stage → TASKS tab:
     a. Sort by duration → check min/median/max gap → SKEW?
     b. Check Shuffle Read Size per task → uneven? → SKEW?
     c. Check GC Time → high? → memory pressure?
     d. Check Spill (Memory/Disk) → non-zero? → insufficient memory for the operation
5. If it's a SQL/DataFrame query → SQL tab → compare Initial vs Final Plan
     → confirm expected optimizations (pushdown, pruning, broadcast, AQE) applied
6. Check EXECUTORS tab → any executor with disproportionate task/shuffle load,
     or executors that died/were re-added (signals instability, OOM, preemption)
7. Form a hypothesis (skew / memory / bad join strategy / too many small tasks)
8. Apply a targeted fix (salting, broadcast join, repartition, memory tuning)
9. Re-run → compare the SAME metrics in the UI to confirm the fix actually worked
```

---

## 💻 /examples — What to Look For, Concretely

```python
# Accessing the UI
# While a job is running:
#   http://<driver-host>:4040
# For completed apps (requires event logging configured):
spark.conf.set("spark.eventLog.enabled", "true")
spark.conf.set("spark.eventLog.dir", "hdfs:///spark-events")
#   Then browse via the Spark History Server UI

# Trigger a job to inspect
df = spark.read.parquet("large_dataset.parquet")
result = df.groupBy("customer_id").sum("amount")
result.collect()   # 🔥 now go check Jobs → Stages → Tasks in the UI

# Programmatically checking cached storage (mirrors the Storage tab)
df.cache()
df.count()
spark.catalog.isCached("some_table_name")   # sanity-check equivalent
```

```
What a SKEWED stage looks like in the Tasks table:

Task ID | Duration | Shuffle Read Size
--------|----------|------------------
  1     |  12s     |     40 MB
  2     |  14s     |     38 MB
  3     |  9m 40s  |    3.2 GB     ⚠️  <- clear skew signature
  4     |  11s     |     41 MB

What a HEALTHY, balanced stage looks like:

Task ID | Duration | Shuffle Read Size
--------|----------|------------------
  1     |  13s     |     42 MB
  2     |  14s     |     41 MB
  3     |  12s     |     43 MB
  4     |  13s     |     40 MB
```

---

## 🏭 /production — Real-World Use Cases

1. **On-call incident response for failed/slow production pipelines**: the first move for any data engineer paged for a stuck or failed Spark job is almost always opening the Spark UI (or History Server for a completed run) to look at the Stages and Tasks tabs before touching any code.
2. **Capacity planning and cost optimization**: the Executors tab's CPU/memory utilization over the life of a job informs whether a cluster is over- or under-provisioned — directly translating to cloud cost decisions (fewer, larger executors vs more, smaller ones).
3. **Validating query optimization efforts**: after applying a fix (broadcast join hint, repartitioning, caching), engineers re-run the job and compare Spark UI metrics (task duration variance, shuffle size, stage count) before/after to confirm the change actually helped, not just "felt" faster.
4. **Auditing AQE behavior in production**: platform teams use the SQL tab's Initial vs Final Plan comparison to confirm AQE is actively coalescing partitions or switching join strategies as expected across a fleet of migrated jobs.
5. **Diagnosing intermittent OOM/executor loss**: the Executors tab reveals executors that were removed and re-added mid-job (often due to out-of-memory kills or spot-instance preemption), directly pointing engineers toward memory configuration or infrastructure stability issues rather than code bugs.

---

## 🚀 /optimization — Tuning Levers (Using the UI to Guide Them)

| UI Signal | Likely Cause | Typical Fix |
|---|---|---|
| Large min/max task duration gap | Data skew | Salting, broadcast join, or enable AQE skew handling |
| Uneven shuffle read sizes across tasks | Skewed shuffle key | Same as above |
| High GC Time relative to task duration | Memory pressure / too many small objects | Increase executor memory, use DataFrame API over RDDs, serialized caching |
| Non-zero Spill (Memory/Disk) | Operation exceeded allotted memory | Increase `spark.sql.shuffle.partitions`, increase executor memory, reduce data volume via earlier filtering |
| Many tiny tasks (sub-second duration) | Over-partitioned data | `coalesce()`, or enable AQE's partition coalescing |
| Few, very large tasks | Under-partitioned data | `repartition()` to increase parallelism |
| `Exchange` nodes in SQL plan you didn't expect | Unintended shuffle (e.g., wide op) | Review code for avoidable wide transformations |
| Executors frequently removed/re-added | OOM kills or spot preemption | Increase executor memory, review on-heap/off-heap config, or use more stable instance types |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Diagnosing performance issues by guessing/reading code alone**, without ever opening the Spark UI — missing the fastest, most direct source of ground-truth diagnostic data.
2. **Not enabling event logging in production** — losing the ability to inspect a completed (especially a failed) job's UI after the fact via the History Server, forcing engineers to reproduce the issue live instead.
3. **Only looking at the Jobs tab** and stopping there — the real diagnostic value is almost always one or two levels deeper, in the Stages and Tasks tabs.
4. **Ignoring the SQL tab's Initial vs Final Plan comparison** — missing confirmation of whether AQE, broadcast joins, or pushdown optimizations actually took effect.
5. **Misreading stage order as strict sequential wall-clock time** — independent stages/jobs can sometimes run concurrently if the cluster has spare capacity, so the UI's ordering isn't always a literal timeline.
6. **Not correlating Executors tab data with Stages/Tasks data** — a slow stage might be explained by a single struggling executor (hardware issue, noisy neighbor) rather than a data or code problem; checking only one tab misses this.

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: What is the Spark UI and what port does it run on by default?**
> A: Spark's built-in web dashboard for monitoring a running (or, via History Server, a completed) application — showing Jobs, Stages, Tasks, Executors, Storage, Environment, and SQL query plans. Default port: 4040.

**Q2: How would you diagnose data skew using the Spark UI?**
> A: Open the Stages tab, find the slow stage, then check the Tasks tab for a large gap between median and max task duration or uneven shuffle read sizes — both are classic skew signatures.

**Q3: What's the difference between the live Spark UI and the History Server?**
> A: The live UI (port 4040) is served directly by the running application's driver and disappears when the app ends; the History Server reconstructs the same UI after the fact by replaying persisted event logs (requires `spark.eventLog.enabled=true`).

**Q4: What does "Spill (Memory)" or "Spill (Disk)" in the Tasks tab indicate?**
> A: That an operation (like a sort or aggregation) exceeded its allotted memory and had to spill intermediate data to disk — a common sign of insufficient memory allocation or overly large partitions.

**Q5: How can the SQL tab confirm whether AQE actually helped a query?**
> A: By comparing the "Initial Plan" (Catalyst's pre-execution plan) against the "Final Plan" (post-AQE, reflecting runtime adjustments like partition coalescing or join strategy switches) for the same query.

**Q6: What would high GC Time in the Tasks tab suggest?**
> A: Memory pressure — often caused by too many small JVM objects (e.g., RDD-heavy code instead of DataFrames), insufficient executor memory, or inefficient caching choices.

**Q7: Why check the Executors tab when diagnosing a slow job?**
> A: It reveals cluster-wide health signals — like an executor with disproportionate load, or executors being removed and re-added (often due to OOM kills or spot-instance preemption) — that wouldn't be obvious from the Stages/Tasks view alone.

---

## ✅ /keytakeaways

- The **Spark UI** (port 4040 live, or via **History Server** for completed apps) is Spark's ground-truth observability dashboard — showing what actually happened, not just what was planned.
- The core diagnostic hierarchy: **Jobs → Stages → Tasks** — most real debugging happens at the **Tasks** level.
- Key task-level signals: **duration variance (skew)**, **shuffle read size imbalance**, **GC time**, and **spill to memory/disk**.
- The **SQL tab's Initial vs Final Plan** comparison directly confirms whether AQE and other optimizations actually took effect.
- The **Executors tab** reveals cluster-wide health issues (OOM kills, preemption, uneven load) that Stages/Tasks alone might not explain.
- **Always enable event logging in production** so failed/completed jobs remain diagnosable after the fact via the History Server.