# Spark Spill to Disk — The Complete Guide

## 🧠 /firstprinciples — What Is Spill to Disk, Really?

Certain Spark operations — sorting, aggregating, joining (specifically sort-merge/hash joins), and shuffling — need **working memory** to hold intermediate data while they compute: a hash table being built, a set of records being sorted, buffers accumulating shuffle output.

If that operation's working data **doesn't fit in the memory Spark has allotted** for it (execution memory, from the Unified Memory pool), Spark doesn't simply crash. Instead, it **writes the excess data to local disk temporarily**, continues processing in smaller chunks, and later **reads it back and merges it** as needed to produce the correct final result.

> Spilling to disk is Spark's mechanism for gracefully handling memory-intensive operations that exceed their allotted execution memory — trading disk I/O (slower) for avoiding an out-of-memory failure (job death).

**The one-sentence definition:**
> Spill is Spark's safety valve: when an operation's working set exceeds available execution memory, Spark writes the overflow to disk instead of crashing, at the cost of significantly slower processing for that operation.

Spilling is **not inherently a bug** — it's a designed fallback. But **frequent or large spills are almost always a performance red flag** worth investigating.

---

## 🧺 Simple Analogy

Imagine you're **sorting a massive stack of documents** on your desk, but your desk (execution memory) can only hold **so many papers at once**.

- If the stack fits on your desk, you sort it directly — fast, simple, all in one pass.
- If the stack is **too large for your desk**, you instead: sort **smaller batches** that DO fit on the desk, **write each sorted batch onto the floor** (spill to disk) in labeled piles, and once you've processed everything, **pick the piles back up and merge them together** in the correct final order.

You still get a correctly sorted final stack — but the extra trips to and from the floor (disk I/O) make the whole process **significantly slower** than if everything had fit on the desk the whole time.

---

## 👁️ /visualizelearning — Spill vs No-Spill Execution

```
NO SPILL (working set fits in execution memory)
┌───────────────────────────────────────────┐
│         Execution Memory (RAM)            │
│  ┌──────────────────────────────────────┐ │
│  │   Entire sort/aggregation/join       │ │
│  │   working set held here, in-memory   │ │
│  └──────────────────────────────────────┘ │
└───────────────────────────────────────────┘
        Fast — single in-memory pass


WITH SPILL (working set EXCEEDS execution memory)
┌─────────────────────────────────────────┐
│         Execution Memory (RAM)          │
│  ┌──────────────┐                       │
│  │  Chunk 1     │ ──   processed, then  │
│  └──────────────┘      WRITTEN TO DISK  │
└─────────────────────────────────────────┘         ┌─────────────┐
                                                    │  Disk       │
┌─────────────────────────────────────────┐         │ ┌─────────┐ │
│         Execution Memory (RAM)          │  ───>   │ │Spill 1  │ │
│  ┌──────────────┐                       │         │ ├─────────┤ │
│  │  Chunk 2     │ ── processed, then    │  ───>   │ │Spill 2  │ │
│  └──────────────┘     WRITTEN TO DISK   │         │ ├─────────┤ │
└─────────────────────────────────────────┘         │ │Spill 3  │ │
                                                    │ └─────────┘ │
              (repeat for each chunk)               └─────────────┘
                        │
                        ▼
        FINAL MERGE PHASE: read all spilled chunks back
        from disk, merge them together into the correct
        final sorted/aggregated/joined result
        Slower — extra disk writes AND reads, plus merge overhead
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

### Which operations can spill
- **Sorting** (`orderBy`, `sortBy`, sort-merge joins): Spark's `UnsafeExternalSorter` sorts data in memory-sized chunks, spilling each sorted chunk to disk when memory runs low, then performs a final **k-way merge** of all spilled chunks (plus any remaining in-memory data) to produce the fully sorted output.
- **Aggregation** (`groupBy`, `reduceByKey` with large numbers of distinct keys): the hash table used to accumulate per-key aggregates can grow too large for memory; Spark's `UnsafeFixedWidthAggregationMap` spills partial aggregation state to disk and merges it back later.
- **Shuffle**: both the **write side** (buffering records before writing shuffle files) and **read side** (buffering fetched shuffle blocks before processing) can spill if buffers grow beyond their memory allotment.
- **Joins** (sort-merge join, shuffle hash join): the build side of a hash join, or either side of a sort-merge join, can spill if it doesn't fit in the execution memory allocated to that task.

### The spill mechanism, step by step
1. An operation requests memory from the **Unified Memory Manager's execution pool** for its working data structure (hash map, sort buffer).
2. As the operation processes more data, it periodically checks whether it's exceeding its **current memory allocation** (which itself can shrink if other tasks are competing for the shared execution pool).
3. If memory pressure is detected, the operation **serializes its current in-memory working set to a temporary file on local disk** (under `spark.local.dir`), then **frees that memory** and continues processing new data into a fresh, empty in-memory structure.
4. This repeats as many times as needed — each spill produces another temporary file.
5. Once all input data has been consumed, a **final merge phase** reads all spilled files (using efficient streaming/merge algorithms, not loading everything back into memory at once) and combines them with any remaining in-memory data to produce the correct final result.

```
                    Operation requests execution memory
                                  │
                                  ▼
                     ┌──────────────────────────┐
                     │  Memory available?       │
                     └───────┬───────┬──────────┘
                        YES  │       │  NO
                             ▼       ▼
                   Continue in    SPILL: serialize current
                   memory          in-memory data to a temp
                                   file on local disk (spark.local.dir),
                                   free that memory, continue
                                             │
                                             ▼
                              (repeats as needed — multiple spill files)
                                             │
                                             ▼
                         FINAL MERGE: stream-read all spilled
                         files + remaining in-memory data,
                         merge into the correct final result
```

### Where to see it in the Spark UI
- The **Tasks tab** (and **Stage summary metrics**) shows two distinct spill columns:
  - **Spill (Memory)**: the size of the data **before** it was serialized for spilling (i.e., its footprint while still in memory).
  - **Spill (Disk)**: the size of the data **after** serialization, as actually written to disk (often smaller, due to compact serialized encoding).
- **Non-zero values in either column are a direct, unambiguous signal** that an operation exceeded its memory allocation during that task.

---

## 🏗️ /architecture — Where Spill Fits in the Memory Model

```
┌────────────────────────────────────────────────────────────────────┐
│                     EXECUTOR (per task)                            │
│                                                                    │
│   ┌────────────────────────────────────────────────────────────┐   │
│   │            EXECUTION MEMORY (Unified Memory pool)          │   │
│   │   Operation's working set lives here WHILE it fits         │   │
│   └─────────────────────┬──────────────────────────────────────┘   │
│                         │  memory pressure detected                │
│                         ▼                                          │
│   ┌────────────────────────────────────────────────────────────┐   │
│   │              spark.local.dir (LOCAL DISK)                  │   │
│   │        temporary spill files, per task, per operation      │   │
│   │        cleaned up automatically once the task completes    │   │
│   └────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
```

Spill is a **task-local, execution-memory-specific** phenomenon — it's Spark's mechanism to keep a single task from crashing when its slice of the shared execution memory pool isn't enough for its current workload, given everything else happening concurrently on that executor.

---

## 🔄 /flow — Full Lifecycle of a Spilling Operation

```
1. Task begins a memory-intensive operation (large groupBy, sort, or join)
2. Data streams in; an in-memory structure (hash map/sort buffer) accumulates it
3. Memory pressure builds (either genuinely large data, OR because other
     concurrent tasks on the same executor are competing for the shared pool)
4. Threshold exceeded → current in-memory data serialized and spilled to
     a temp file under spark.local.dir; memory freed
5. Steps 2-4 repeat as more data streams in, potentially producing
     MULTIPLE spill files over the task's execution
6. All input data exhausted → FINAL MERGE phase begins:
     spilled files + any remaining in-memory data are combined
     (e.g., k-way merge for sorts, hash map merge for aggregations)
7. Correct final result produced and returned/passed downstream
8. Temporary spill files cleaned up once no longer needed
```

---

## 💻 /examples — Detecting and Reducing Spill

```python
df = spark.read.parquet("huge_dataset.parquet")

# An operation prone to spilling: large-cardinality aggregation
result = df.groupBy("high_cardinality_key").count()
result.collect()

# Check the Spark UI's Stages/Tasks tab for "Spill (Memory)" / "Spill (Disk)"
# Non-zero values = this operation exceeded its execution memory allotment
```

```python
# Common mitigation #1: increase shuffle partitions
# (smaller partitions per task = smaller per-task working set = less spill)
spark.conf.set("spark.sql.shuffle.partitions", "800")   # up from default 200

# Common mitigation #2: increase executor memory,
# giving the execution pool more room before it needs to spill
# (set via spark-submit --executor-memory, not at runtime)

# Common mitigation #3: reduce data volume BEFORE the memory-intensive
# operation — filter/project early so less data needs to be held at once
df.filter(df.amount > 100).select("high_cardinality_key", "amount") \
  .groupBy("high_cardinality_key").count()

# Common mitigation #4: enable AQE, which can help right-size
# post-shuffle partitions, reducing the chance of any single
# oversized partition overwhelming its task's memory allocation
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

---

## 🏭 /production — Real-World Use Cases

1. **Large-cardinality groupBy operations** (e.g., aggregating by a high-cardinality `user_id` or `session_id` across billions of rows): teams routinely see spill in the Spark UI for these stages, and address it by increasing shuffle partitions or executor memory rather than accepting the resulting slowdown as "just how it is."
2. **Sort-heavy reporting jobs** (global `ORDER BY` across a huge dataset for export/reporting): sort operations are one of the most common sources of spill; teams monitor spill metrics specifically for these jobs since sorting is inherently memory-intensive at scale.
3. **Skewed joins/aggregations**: a single oversized partition (from data skew) is a very common trigger for spill — the task handling that partition simply has more data than its memory allocation was sized for, even if every other task in the stage runs cleanly with zero spill.
4. **Capacity planning for memory-intensive ETL stages**: platform teams reviewing recurring pipeline performance specifically look for stages with consistent, sizeable spill metrics as candidates for either more executor memory or increased partition counts, rather than accepting the recurring disk I/O tax indefinitely.
5. **Diagnosing sudden slowdowns after a data volume increase**: a pipeline that ran cleanly for months can start spilling heavily once underlying data volume crosses a threshold relative to its (unchanged) memory configuration — a common, easily overlooked cause of gradual production performance degradation.

---

## 🚀 /optimization — Tuning Levers

| Lever | What it does |
|---|---|
| **Increase `spark.sql.shuffle.partitions`** | Smaller partitions per task reduce each task's working-set size, lowering spill likelihood |
| **Increase executor memory** | Gives the execution memory pool more headroom before spill is triggered |
| **Filter/project data earlier in the pipeline** | Reduces the volume of data a memory-intensive operation has to hold at once |
| **Enable AQE** (`spark.sql.adaptive.enabled`) | Helps right-size post-shuffle partitions and handle skew, reducing the chance of oversized, spill-prone partitions |
| **Address data skew directly** (salting, broadcast joins) | Removes the root cause of a single oversized, spill-prone partition |
| **Monitor Spill (Memory) / Spill (Disk) in the Spark UI** | Direct, measurable confirmation of which stages/operations are actually affected |
| **Use fast local disks (SSD) for `spark.local.dir`** | Reduces the I/O penalty when spill is unavoidable, even if it doesn't eliminate spill itself |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Ignoring non-zero spill metrics** because the job "still completes" — spill doesn't cause failure, but it can silently be responsible for a large fraction of a job's total runtime.
2. **Assuming spill only happens on genuinely huge datasets** — spill can also result from **data skew** (one oversized partition) even when the overall dataset and cluster are reasonably sized.
3. **Increasing executor memory blindly without checking partition count first** — sometimes simply increasing `spark.sql.shuffle.partitions` (making each task's slice smaller) is a cheaper, more effective fix than adding memory.
4. **Not distinguishing "Spill (Memory)" from "Spill (Disk)"** — the memory figure shows pre-serialization size, disk shows post-serialization size; conflating them can lead to misjudging how much actual I/O occurred.
5. **Placing `spark.local.dir` on a slow or space-constrained disk** — heavy, frequent spilling to a slow disk compounds the performance penalty; under-provisioned local disk space can even cause job failures if spill files exhaust available space.
6. **Treating every spill occurrence as an emergency** — occasional, small spill on a few tasks in an otherwise healthy stage is often a non-issue; the real red flag is **consistent, large spill across most/all tasks in a stage**, indicating systemic under-provisioning.

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: What does "spill to disk" mean in Spark?**
> A: When a memory-intensive operation (sort, aggregation, join, shuffle) exceeds its allotted execution memory, Spark writes the excess intermediate data to local disk temporarily, then reads it back and merges it later to produce the correct final result — trading speed for avoiding an OOM failure.

**Q2: What kinds of operations commonly cause spill?**
> A: Sorting (`orderBy`, sort-merge joins), large-cardinality aggregations (`groupBy`), and shuffle read/write buffering — anything that needs to hold a working set in memory that can grow unpredictably large.

**Q3: How can you detect spill in a running or completed job?**
> A: Check the Spark UI's Stages/Tasks tab for the "Spill (Memory)" and "Spill (Disk)" columns; non-zero values directly indicate that operation exceeded its execution memory allocation for that task.

**Q4: What's the difference between "Spill (Memory)" and "Spill (Disk)" metrics?**
> A: "Spill (Memory)" shows the data's size before serialization (its in-memory footprint); "Spill (Disk)" shows its size after serialization, as actually written to disk — often smaller due to compact encoding.

**Q5: How would you reduce spill in a groupBy-heavy job?**
> A: Increase `spark.sql.shuffle.partitions` (smaller per-task working sets), increase executor memory, filter/project data earlier to shrink volume before aggregation, or enable AQE to better right-size partitions.

**Q6: Can data skew cause spill even on a well-provisioned cluster?**
> A: Yes — a single oversized partition from a skewed key can exceed its task's memory allocation and spill, even if every other task in the same stage processes its (smaller, non-skewed) partition entirely in memory.

**Q7: Does spilling to disk cause a job to fail?**
> A: No — spilling is a designed fallback mechanism specifically to avoid an out-of-memory failure; it allows the job to complete correctly, just more slowly due to the added disk I/O and merge overhead.

---

## ✅ /keytakeaways

- **Spill to disk** is Spark's safety valve: when a memory-intensive operation (sort, aggregation, join, shuffle) exceeds its execution memory allocation, the overflow is written to disk, then read back and merged later.
- It's a **designed fallback**, not a crash — but **frequent or large spills are a real performance red flag**, not a harmless side effect.
- Common causes: **genuinely large data volume**, **data skew** (one oversized partition), or **too few shuffle partitions** (each task's slice too large).
- **Detect it** directly via the Spark UI's "Spill (Memory)" and "Spill (Disk)" task metrics.
- **Fix it** by increasing shuffle partitions, increasing executor memory, filtering data earlier, enabling AQE, or addressing skew directly — usually in that rough order of cost-effectiveness.
- Occasional small spill on a few tasks is often fine; **consistent, large spill across most tasks in a stage** signals systemic under-provisioning worth fixing.