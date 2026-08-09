# Spark Repartition vs Coalesce — The Complete Guide


## 🧠 /firstprinciples — What Are Repartition and Coalesce, Really?

Both `repartition()` and `coalesce()` change **how many partitions** your RDD/DataFrame has — but they achieve it through fundamentally different mechanisms, with very different costs.

- **`repartition(n)`**: performs a **full shuffle**, redistributing every single row across the cluster to produce exactly `n` **evenly-sized** partitions. It can **increase or decrease** partition count.
- **`coalesce(n)`**: **merges existing adjacent partitions** together **without a full shuffle** (by default), to reduce the partition count to `n`. It can only meaningfully **decrease** partition count, and the resulting partitions may be **unevenly sized**.

**The one-sentence definition:**
> `repartition()` is a wide transformation that shuffles all data to create evenly balanced partitions at any target count; `coalesce()` is a narrow(-ish) transformation that cheaply merges existing partitions to reduce their number, without guaranteeing balance.

Both exist because "how many partitions, and how evenly sized" directly controls **parallelism** — too few partitions underutilizes the cluster; too many creates scheduling overhead; badly distributed partitions cause skew.

---

## 📦 Simple Analogy

Imagine you have **20 half-full moving boxes** and you want fewer boxes for a truck.

- **`coalesce()`** = you just **tape adjacent boxes together** into bigger groups — box 1+2 become one unit, box 3+4 become another, and so on. Fast, no repacking needed. But if box 1 already had way more stuff than box 2, that imbalance carries over — some "merged boxes" end up heavier than others.

- **`repartition()`** = you **dump everything out of all 20 boxes onto the floor**, then carefully re-pack it into exactly `n` new boxes, each weighing roughly the same. Much more work (you touched every single item), but you get a **perfectly balanced** result.

Coalesce is fast because it avoids re-touching everything; repartition is thorough because it re-touches everything to guarantee balance.

---

## 👁️ /visualizelearning — Coalesce vs Repartition

```
COALESCE (5 → 2 partitions, NO shuffle — just merges adjacent partitions)
┌────┐┌────┐┌────┐┌────┐┌────┐
│ P1 ││ P2 ││ P3 ││ P4 ││ P5 │
└────┘└────┘└────┘└────┘└────┘
   │     │     │     │     │
   └──┬──┘     └──┬──┴──┬──┘
      ▼           ▼
  ┌────────┐  ┌────────────┐
  │ P1+P2  │  │ P3+P4+P5   │   <- sizes may be UNEVEN, depends on originals
  └────────┘  └────────────┘
  Cheap: no network shuffle, data stays where it already was


REPARTITION (5 → 2 partitions, FULL shuffle — every row redistributed)
┌────┐┌────┐┌────┐┌────┐┌────┐
│ P1 ││ P2 ││ P3 ││ P4 ││ P5 │
└────┘└────┘└────┘└────┘└────┘
   \    |    |    |    /
    \   |    |    |   /   ALL rows shuffled across the network
     ▼  ▼    ▼    ▼  ▼
  ┌──────────┐  ┌──────────┐
  │  New P1  │  │  New P2  │   <- EVENLY sized, but expensive (full shuffle)
  └──────────┘  └──────────┘
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

### `coalesce(n)` internals
- Spark computes a mapping of **which existing partitions to group together** to reach the target count `n`, trying to combine **partitions that are already co-located** on the same executor where possible (minimizing data movement).
- No `Partitioner`/hash redistribution is applied — data simply stays in its original partition, and partitions are logically merged.
- Because there's no shuffle, `coalesce()` **cannot increase** partition count meaningfully (you can't "split" a partition without moving data around, which is exactly what a shuffle would do) — calling `coalesce()` with a larger `n` than the current count is a **no-op**.
- This also means: if your original partitions were already skewed, **coalesce preserves and can even worsen that imbalance**, since it just groups existing (possibly uneven) partitions together.

### `repartition(n)` internals
- Applies a `Partitioner` (default: round-robin or hash-based depending on context) across **all rows**, writing shuffle files bucketed by target partition, exactly like any other wide-dependency operation.
- Triggers the full shuffle machinery: **shuffle write → MapOutputTracker metadata → shuffle read/fetch → new stage**.
- Because every row is redistributed from scratch, the result is **evenly balanced** by row count (not necessarily by byte size if row sizes vary a lot, but generally far more even than coalesce).
- Can also **increase** partition count, since it isn't constrained to merging existing units — it fully redistributes data into as many buckets as requested.

### `repartition(col)` — key-based repartitioning
- A variant, `df.repartition(n, "some_column")` (or without `n`), partitions data by the **hash of a specific column** rather than evenly by row count — useful for ensuring all rows with the same key land in the same partition ahead of a later groupBy/join, but this can **reintroduce skew** if that column's values are themselves imbalanced.

```
coalesce(n):  existing partitions ──▶ [grouped locally, no network] ──▶ n partitions
repartition(n): existing partitions ──▶ [SHUFFLE: write, transfer, read] ──▶ n partitions
```

---

## 🏗️ /architecture — Where Each Sits in the DAG

```
coalesce() — typically a NARROW dependency (when reducing partitions)
┌─────────────────────────────────────────────┐
│  Stage stays the SAME — coalesce is fused   │
│  into the existing pipeline, no new stage   │
└─────────────────────────────────────────────┘

repartition() — always a WIDE dependency
┌───────────────┐  SHUFFLE  ┌───────────────┐
│   Stage N     │ ─────────>│   Stage N+1   │
│ (before       │           │ (new stage    │
│  repartition) │           │  begins here) │
└───────────────┘           └───────────────┘
```

**Interview one-liner:** *"`coalesce` avoids a shuffle by merging what's already there; `repartition` always shuffles because it needs to fully redistribute data to guarantee balance or increase partition count."*

---

## 🔄 /flow — Decision Flow: Which One Should You Use?

```
Do you need to INCREASE partition count?
   │
   ├── YES ──▶ Must use repartition() (coalesce can't meaningfully increase)
   │
   └── NO (decreasing) ──▶ Is even balance across partitions important
                            for what comes next (e.g., before a heavy
                            downstream computation)?
                              │
                              ├── YES ──▶ repartition() (pay the shuffle
                              │            cost for guaranteed balance)
                              │
                              └── NO  ──▶ coalesce() (cheap, good enough,
                                           e.g., before writing output files)
```

---

## 💻 /examples — Code Comparisons

```python
df = spark.read.parquet("large_dataset.parquet")   # e.g., 500 partitions

# Reduce to 10 partitions cheaply before writing output — coalesce is ideal
# (no shuffle; avoids creating 500 tiny output files)
df.coalesce(10).write.parquet("output/")

# Increase parallelism for heavy downstream processing — repartition needed
# (coalesce CANNOT increase partitions; this would be a no-op with coalesce)
df.repartition(200).groupBy("customer_id").sum("amount")

# Fixing skew before a join by repartitioning on the join key
df.repartition(100, "customer_id").join(other_df, "customer_id")

# Common mistake: calling coalesce(1) on a huge dataset "to get one output file"
# This merges everything into ONE partition — extremely slow for big data,
# and can even cause a single executor to run out of memory
df.coalesce(1).write.csv("single_file_output/")  # ⚠️ dangerous at scale

# Checking actual partition count
print(df.rdd.getNumPartitions())
```

---

## 🏭 /production — Real-World Use Cases

1. **Reducing small output files before writing** ("the small files problem"): after a heavy shuffle-based transformation leaves data spread across 200 partitions, teams `coalesce()` down to a smaller number (e.g., 10–20) before writing to Parquet/Delta, avoiding thousands of tiny files that hurt downstream read performance.
2. **Increasing parallelism after reading a small number of large input files**: if a job reads from just 4 large gzip files (gzip isn't splittable), Spark ends up with only 4 partitions no matter the cluster size; `repartition()` is used immediately after reading to unlock real cluster-wide parallelism for subsequent processing.
3. **Balancing partitions ahead of an expensive join/aggregation**: engineers proactively `repartition()` on the join key before a large join to ensure even data distribution and avoid skew-related stragglers during the join itself.
4. **Cost control on cloud clusters (avoiding tiny task overhead)**: pipelines that over-partition data (e.g., leftover 2000 partitions from an earlier stage) get `coalesce()`'d down before a lighter final step, avoiding the scheduling overhead of thousands of small, near-empty tasks.
5. **Streaming pipelines writing to partitioned storage** (e.g., data lakes partitioned by date/hour): `coalesce()` is commonly used per micro-batch to control the number of output files written per partition folder, balancing file count against file size for downstream query efficiency.

---

## 🚀 /optimization — Tuning Levers

| Lever | What it does |
|---|---|
| **Use `coalesce()` before writing output** | Cheaply reduces file count without paying for a full shuffle |
| **Use `repartition()` after reading few large/unsplittable files** | Unlocks real parallelism when input format limits initial partition count |
| **Use `repartition(n, col)` before a skew-prone join/groupBy** | Proactively balances data by the actual key that will be used downstream |
| **Avoid `coalesce(1)` on large datasets** | Forces all data through a single task/partition — extremely slow and memory-risky at scale |
| **Combine with AQE's `coalescePartitions`** | Let AQE automatically right-size post-shuffle partitions at runtime instead of manually guessing a fixed number everywhere |
| **Monitor `df.rdd.getNumPartitions()`** during development | Confirms actual partition count matches expectations at each pipeline stage |
| **Repartition by column only when it helps a downstream operation** | Unnecessary key-based repartitioning adds shuffle cost without benefit if nothing downstream needs that specific grouping |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Using `coalesce()` when you actually need to increase partitions** — it silently becomes a no-op instead of increasing count, leaving parallelism unchanged and confusing engineers who expect it to behave like `repartition()`.
2. **Calling `coalesce(1)` "to get a single output file" on large datasets** — this collapses everything into one partition processed by a single task, often causing severe slowdowns or out-of-memory errors.
3. **Assuming `coalesce()` produces evenly balanced partitions** — it doesn't; it merges whatever was already there, which can preserve or even worsen existing imbalance.
4. **Overusing `repartition()` reflexively** "just in case," paying for a full shuffle when a cheaper `coalesce()` (or no repartitioning at all) would have sufficed.
5. **Repartitioning by a skewed column** — `repartition(n, "hot_key_column")` can concentrate a disproportionate share of data into a few partitions if that column itself is skewed, effectively reintroducing the exact problem repartitioning was meant to solve.
6. **Not verifying actual partition count/size after either operation** — assuming the operation "worked as intended" without checking `df.rdd.getNumPartitions()` or the Spark UI's stage/task metrics.

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: What's the core difference between `repartition()` and `coalesce()`?**
> A: `repartition()` performs a full shuffle to produce evenly balanced partitions at any target count (can increase or decrease); `coalesce()` cheaply merges existing adjacent partitions without a full shuffle, and can only effectively decrease partition count.

**Q2: Can `coalesce()` increase the number of partitions?**
> A: No — calling `coalesce()` with a target higher than the current partition count is effectively a no-op, since it can only merge existing partitions, not split them.

**Q3: Why is `coalesce()` generally faster than `repartition()`?**
> A: Because it avoids a full shuffle — it merges existing partitions (often already co-located) rather than redistributing every row across the network via a `Partitioner`.

**Q4: When would you choose `repartition()` over `coalesce()` even though it's more expensive?**
> A: When you need guaranteed even balance across partitions (e.g., before an expensive downstream join/aggregation), need to increase partition count, or need to repartition by a specific key.

**Q5: Why is `coalesce(1)` risky on large datasets?**
> A: It forces all data into a single partition processed by one task, eliminating parallelism entirely and risking out-of-memory errors or extremely long runtimes.

**Q6: Does `coalesce()` guarantee balanced output partitions?**
> A: No — it merges whatever partitions already exist, so any pre-existing imbalance carries through (or can even worsen) in the merged result.

**Q7: What's a common production use case for `coalesce()`?**
> A: Reducing the number of output files written to storage after a wide transformation left data spread across many small partitions — avoiding the "small files problem" without paying for a full shuffle.

---

## ✅ /keytakeaways

- **`repartition(n)`**: full shuffle, evenly balanced partitions, can increase or decrease count — expensive but guaranteed balance.
- **`coalesce(n)`**: cheap merge of existing adjacent partitions, no full shuffle, can only decrease count — fast but balance not guaranteed.
- `coalesce()` with a target **higher** than the current partition count is a **no-op**.
- Use **`coalesce()`** before writing output to reduce file count cheaply; use **`repartition()`** to fix skew, increase parallelism, or balance data before an expensive downstream operation.
- **Avoid `coalesce(1)`** on large datasets — it destroys parallelism and risks OOM.
- Always verify actual partition count/balance with `df.rdd.getNumPartitions()` and the Spark UI rather than assuming either operation "just worked."