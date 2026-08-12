# Spark Cache vs Persist — The Complete Guide

## 🧠 /firstprinciples — What Are Cache and Persist, Really?

Because of **lazy evaluation**, every time you call an action (`.count()`, `.collect()`, `.write()`) on a DataFrame/RDD, Spark **recomputes the entire upstream lineage from scratch** — unless you tell it to remember the result.

`.cache()` and `.persist()` both do exactly that: they tell Spark, **"after you compute this the first time, keep it around (in memory and/or disk) so future actions can reuse it instead of recomputing."**

- **`.cache()`** is a convenience shortcut — it's just `.persist()` with **one fixed default storage level**: `MEMORY_AND_DISK` (or `MEMORY_ONLY` in older RDD-only contexts) for DataFrames.
- **`.persist(storageLevel)`** is the full-control version — you explicitly choose **where and how** the data should be stored: memory only, memory + disk, disk only, serialized, replicated, and more.

**The one-sentence definition:**
> `cache()` is `persist()` with a fixed default storage level; `persist()` lets you choose exactly how and where Spark stores the computed result for reuse by later actions.

Both are still **lazy**: calling `.cache()`/`.persist()` alone does nothing observable — the actual storing only happens the **next time an action runs**.

---

## 🍲 Simple Analogy

Imagine you're cooking a complex sauce base that takes **an hour to prepare**, and you'll need it for three different dishes tonight.

- **Without caching**: every time a dish needs the sauce base, you **remake it from scratch** — three separate hours of prep for the same result.
- **`.cache()`**: after making it once, you put it in the **fridge** (a sensible default storage spot) — quick to grab again for the next two dishes, no re-cooking.
- **`.persist(storageLevel)`**: you get to **choose exactly how to store it** — keep it in the fridge (memory) for speed, freeze a backup in the garage freezer (disk) in case the fridge fills up, or even make **two portions and store them in two different fridges** (replication) in case one fridge breaks.

`cache()` is "just put it in the fridge, default settings." `persist()` is "tell me exactly how and where to store this, I have specific needs."

---

## 👁️ /visualizelearning — Without vs With Caching

```
WITHOUT CACHING (3 actions on the same lineage = 3 full recomputations)

df = read → filter → expensive_join     (lazy, nothing computed yet)

df.count()   🔥 recomputes EVERYTHING from scratch
df.show()    🔥 recomputes EVERYTHING from scratch AGAIN
df.write()   🔥 recomputes EVERYTHING from scratch A THIRD TIME


WITH CACHING (compute once, reuse for all subsequent actions)

df = read → filter → expensive_join
df.cache()                              (still lazy — nothing happens yet)

df.count()   🔥 COMPUTES once, AND stores the result (memory/disk)
df.show()    ✅ REUSES the stored result — no recomputation
df.write()   ✅ REUSES the stored result — no recomputation
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

### The mechanics
1. Calling `.cache()`/`.persist()` marks the DataFrame/RDD's plan node as **"should be cached"** — but this is purely a marker, no computation happens.
2. The **first action** that touches this DataFrame triggers actual computation, **and** — as each partition finishes computing — the **`BlockManager`** on each executor stores that partition's result according to the chosen **`StorageLevel`**.
3. **Subsequent actions** on the same DataFrame check the `BlockManager` first; if the relevant partitions are already cached, Spark **skips recomputing that lineage entirely** and reads directly from the cached blocks.

### Storage Levels (the core of what `.persist()` controls)

| StorageLevel | Where stored | Serialized? | Replicated? |
|---|---|---|---|
| `MEMORY_ONLY` | RAM only | No (Java objects) | No |
| `MEMORY_ONLY_SER` | RAM only | Yes (compact bytes) | No |
| `MEMORY_AND_DISK` | RAM, spills to disk if full | No | No |
| `MEMORY_AND_DISK_SER` | RAM, spills to disk if full | Yes | No |
| `DISK_ONLY` | Disk only | Yes | No |
| `MEMORY_AND_DISK_2` | RAM + disk | No | Yes (2 copies, different executors) |
| `OFF_HEAP` | Off-heap memory (Tungsten-managed) | Yes | No |

`.cache()` is simply shorthand for `.persist(StorageLevel.MEMORY_AND_DISK)` for DataFrames/Datasets (RDDs historically default to `MEMORY_ONLY`).

### What happens under memory pressure
- If cached data doesn't fit in available memory, Spark's **unified memory manager** must decide what to do:
  - With `MEMORY_ONLY`: partitions that don't fit are simply **not cached** (recomputed on demand later) — silent, no crash, but reduced benefit.
  - With `MEMORY_AND_DISK`: partitions that don't fit in memory **spill to local disk** instead — slower than RAM, but still avoids full recomputation.
- Spark uses an **LRU (Least Recently Used) eviction policy** within the cache when new data needs to make room and memory is constrained.

```
                     ACTION triggers computation
                              │
                              ▼
                  ┌───────────────────────┐
                  │   Partition computed  │
                  └───────────┬───────────┘
                              ▼
                  ┌─────────────────────────┐
                  │  BlockManager stores    │
                  │  it per StorageLevel:   │
                  │  enough RAM? → memory   │
                  │  full? → spill to disk  │
                  │         (or evict/drop) │
                  └─────────────────────────┘
                              │
              Next action reads from BlockManager
              instead of recomputing the lineage
```

### Unpersisting
- `.unpersist()` explicitly removes the cached data from memory/disk, freeing resources — important for long-running applications (e.g., notebooks, streaming jobs) where cached DataFrames that are no longer needed would otherwise waste memory indefinitely.

---

## 🏗️ /architecture — Where Caching Sits in the Execution Model

```
┌───────────────────────────────────────────────────────────────┐
│                     Logical Plan (DAG)                        │
│   read → filter → join → [CACHE MARKER] → downstream ops      │
└───────────────────────────┬───────────────────────────────────┘
                            │  first action triggers computation
                            ▼
┌───────────────────────────────────────────────────────────────┐
│      Executors compute partitions, BlockManager stores        │
│         each per the chosen StorageLevel (mem/disk/etc.)      │
└───────────────────────────┬───────────────────────────────────┘
                            │  subsequent actions
                            ▼
┌─────────────────────────────────────────────────────────────┐
│     Reads served DIRECTLY from BlockManager — the upstream  │
│     lineage (read → filter → join) is NOT re-executed       │
└─────────────────────────────────────────────────────────────┘
```

Caching effectively **truncates the DAG's re-execution path** for future actions — everything upstream of the cache point becomes a "already done, just fetch it" operation instead of a live computation.

---

## 🔄 /flow — Full Lifecycle of Cache/Persist

```
1. df = spark.read.parquet(...).filter(...).join(other, ...)   → lazy plan built
2. df.cache()  (or df.persist(StorageLevel.MEMORY_AND_DISK_SER))
     → marks df for caching, still nothing computed
3. df.count()  🔥 FIRST action:
     a. Full upstream lineage executes (read, filter, join)
     b. As each partition completes, BlockManager stores it per StorageLevel
4. df.show()   ✅ reads directly from cached blocks — no re-execution
5. df.write()  ✅ reads directly from cached blocks — no re-execution
6. df.unpersist()  → cached blocks explicitly freed when no longer needed
```

---

## 💻 /examples — Code Comparisons

```python
from pyspark import StorageLevel

df = spark.read.parquet("large_dataset.parquet").filter(df.amount > 100)

# cache() — convenience shortcut, default storage level (MEMORY_AND_DISK)
df.cache()

# persist() — explicit control over storage level
df.persist(StorageLevel.MEMORY_AND_DISK_SER)   # serialized, spills to disk if needed
# df.persist(StorageLevel.DISK_ONLY)            # for very large data that won't fit in RAM
# df.persist(StorageLevel.MEMORY_AND_DISK_2)    # replicated for resilience

df.count()   # 🔥 triggers computation AND populates the cache
df.show()    # ✅ served from cache — fast, no recomputation

# Always clean up when done, especially in long-running apps/notebooks
df.unpersist()

# Verify what's cached via the Spark UI:
# http://<driver>:4040 → "Storage" tab shows cached RDDs/DataFrames,
# their storage level, size in memory/disk, and % cached
```

```python
# Demonstrating the cost WITHOUT caching (anti-pattern)
df = spark.read.parquet("large.parquet").filter(...).join(other, ...)
print(df.count())   # full computation #1
df.write.parquet("out1/")  # full computation #2 — same lineage, recomputed
```

---

## 🏭 /production — Real-World Use Cases

1. **Multi-branch pipelines sharing an expensive upstream computation**: a cleaned/joined DataFrame that feeds three different downstream reports is cached once after the expensive cleaning/join step, so each report only pays for its own unique logic — not the shared upstream cost three times over.
2. **Iterative machine learning training loops**: algorithms that repeatedly touch the same training DataFrame across many iterations (e.g., custom gradient descent implementations, or iterative graph algorithms) cache the DataFrame to avoid re-reading and re-transforming raw data on every single iteration.
3. **Interactive notebook exploration**: data scientists caching intermediate DataFrames after expensive joins/aggregations so that subsequent exploratory `.show()`/`.describe()` calls in later notebook cells are fast, instead of waiting minutes for each new query.
4. **Streaming jobs with a slowly-changing reference dataset**: a small, rarely-updated lookup/reference DataFrame (e.g., a currency rate table) is cached once and reused across many streaming micro-batches, avoiding redundant reads on every batch trigger.
5. **Choosing `DISK_ONLY` or `SER` storage levels for very large intermediate results** in memory-constrained production clusters — teams deliberately trade some read speed for avoiding executor OOM errors, using `.persist()`'s explicit control rather than relying on `.cache()`'s default.

---

## 🚀 /optimization — Tuning Levers

| Lever | What it does |
|---|---|
| **Cache only DataFrames reused by multiple actions/branches** | Caching a DataFrame used just once adds overhead with zero benefit |
| **Choose a serialized storage level** (`MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`) for large data | Reduces memory footprint (compact binary vs full Java objects), at some CPU cost to deserialize on read |
| **Use `DISK_ONLY` for data too large for available memory** | Still avoids full recomputation, at the cost of disk I/O speed vs memory speed |
| **Always `.unpersist()` when a cached DataFrame is no longer needed** | Frees memory/disk for other operations, especially critical in long-running jobs/notebooks |
| **Monitor the Spark UI "Storage" tab** | Confirms what's actually cached, its size, and whether it's fully in memory or partially spilled to disk |
| **Avoid caching right before a single, one-time final action** | If there's no reuse, caching adds storage overhead for no payoff — just let the action run normally |
| **Cache AFTER expensive operations, not before** | Caching a raw, un-filtered DataFrame wastes memory on rows that get filtered out anyway downstream |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Calling `.cache()`/`.persist()` without ever triggering an action afterward** — since both are lazy, nothing is actually stored until the next action runs; assuming the data is "cached" immediately after the call is a common misunderstanding.
2. **Caching a DataFrame that's only used once** — pure overhead (memory/disk usage, serialization cost) with zero benefit, since there's no repeated reuse to amortize the cost against.
3. **Never calling `.unpersist()`** in long-running applications (streaming jobs, notebooks) — cached DataFrames accumulate in memory indefinitely, eventually causing memory pressure or eviction of more useful cached data.
4. **Using the default `.cache()` storage level for very large datasets** that don't fit in memory, without realizing `MEMORY_AND_DISK` will spill significant data to disk (slower than expected) — sometimes `DISK_ONLY` or a serialized level is a more deliberate, appropriate choice.
5. **Caching too early in the pipeline** (before filters/projections that would shrink the data) — wastes memory holding rows that get discarded downstream anyway; cache after the expensive/reused computation, as close as possible to the point of actual reuse.
6. **Assuming caching guarantees data stays in memory** — under memory pressure, Spark can silently evict or fail to cache some partitions (with `MEMORY_ONLY`), leading to partial, inconsistent performance benefits that are easy to misdiagnose.

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: What's the difference between `cache()` and `persist()`?**
> A: `cache()` is a shortcut for `persist()` using a fixed default storage level (`MEMORY_AND_DISK` for DataFrames); `persist()` lets you explicitly choose the storage level (memory-only, disk-only, serialized, replicated, etc.).

**Q2: Does calling `.cache()` immediately store the data?**
> A: No — both `.cache()` and `.persist()` are lazy; the data is only actually computed and stored when the next action triggers execution.

**Q3: What happens if cached data doesn't fit in memory?**
> A: Depends on the storage level: with `MEMORY_ONLY`, partitions that don't fit simply aren't cached (recomputed later); with `MEMORY_AND_DISK`, they spill to local disk instead of being dropped.

**Q4: When should you cache a DataFrame?**
> A: When it's reused by multiple actions or downstream branches — caching something used only once adds overhead without any benefit.

**Q5: What does `.unpersist()` do, and why does it matter?**
> A: It explicitly removes cached data from memory/disk; important in long-running applications to free resources for other operations once the cached data is no longer needed.

**Q6: What's the difference between `MEMORY_ONLY` and `MEMORY_ONLY_SER`?**
> A: `MEMORY_ONLY` stores data as regular deserialized Java objects (faster access, more memory usage); `MEMORY_ONLY_SER` stores data in a compact serialized binary format (less memory, extra CPU cost to deserialize on read).

**Q7: Where would you check what's currently cached in a Spark application?**
> A: The Spark UI's "Storage" tab, which shows cached RDDs/DataFrames, their storage level, and how much is in memory vs spilled to disk.

---

## ✅ /keytakeaways

- **`cache()`** = `persist()` with a fixed default storage level (`MEMORY_AND_DISK`); **`persist()`** = full control over exactly how/where data is stored.
- Both are **lazy** — nothing is actually stored until the next action runs.
- Cache **only** DataFrames reused across multiple actions or branches — caching something used once is pure overhead.
- Under memory pressure, `MEMORY_ONLY` silently drops uncached partitions; `MEMORY_AND_DISK` spills to disk instead — choose deliberately for large datasets.
- Always **`.unpersist()`** cached data once it's no longer needed, especially in long-running apps.
- Check the **Spark UI's Storage tab** to confirm what's actually cached and how.