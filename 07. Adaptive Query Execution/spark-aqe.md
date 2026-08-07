# Spark Adaptive Query Execution (AQE) — The Complete Guide (Beginner → Advanced)

## 🧠 /firstprinciples — What Is AQE, Really?

The Catalyst Optimizer picks a physical plan **before execution even starts**, using **estimates** — table statistics, size guesses, cardinality assumptions. But estimates can be wrong: a filter might remove 99% of rows unexpectedly, a join key might be wildly skewed, or table stats might be stale. Once execution starts under the old model, Spark was **stuck** with its original plan, even if reality turned out very different from the estimate.

**Adaptive Query Execution (AQE)**, introduced as a mature feature in Spark 3.0, fixes this by letting Spark **re-optimize the plan mid-query**, using **actual runtime statistics** gathered after each completed stage — not just pre-execution guesses.

**The one-sentence definition:**
> AQE is Spark's ability to look at real, measured data (post-shuffle sizes, actual partition row counts) between stages and adjust the rest of the execution plan accordingly — instead of committing blindly to a plan built on pre-execution estimates.

It works because Spark stages are naturally separated by **materialization points (shuffles)** — perfect checkpoints to stop, look at what actually happened, and replan.

---

## 🧭 Simple Analogy

Imagine a **road trip planned entirely from an old paper map** — before GPS. You plan your route assuming normal traffic and picking the fastest highway. You commit to that route and drive, no matter what you encounter.

Now imagine a **GPS with live traffic** (AQE): at every intersection (stage boundary), it **checks current conditions** — "this highway now has a 45-minute jam, reroute through the side street instead" — and **re-plans the rest of the trip in real time**, using what's *actually happening now*, not what was assumed at the start.

AQE is exactly this: Spark stops at each shuffle boundary, checks the **actual** data it just produced, and **re-plans the next stage** if reality doesn't match the original estimate.

---

## 👁️ /visualizelearning — Static Plan vs Adaptive Plan

```
WITHOUT AQE (plan locked in before execution)

Estimated sizes (pre-execution) → Physical Plan chosen → Executed AS-IS
        │                                                      │
        ▼                                                      ▼
  "Table B looks big" → chose SortMergeJoin → runs slow even if
                                                B turned out tiny at runtime


WITH AQE (plan re-optimized mid-query)

Stage 1 runs → 🔍 ACTUAL stats measured (post-shuffle)
                        │
                        ▼
          "Table B's actual shuffled size = 8MB, not 200MB!"
                        │
                        ▼
     AQE REPLANS: SortMergeJoin ──▶ switched to BroadcastHashJoin
                        │
                        ▼
              Stage 2 executes with the BETTER plan
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

AQE performs three major runtime optimizations, all triggered at **shuffle/materialization boundaries** where Spark has a natural pause point with real statistics available:

### 1. Dynamically Coalescing Shuffle Partitions
- Without AQE, `spark.sql.shuffle.partitions` (default 200) is a **fixed number**, regardless of actual data size — leading to many tiny, wasteful partitions for small data, or too few huge partitions for large data.
- AQE measures the **actual size of each post-shuffle partition** and **merges small adjacent partitions together**, reducing task overhead without needing manual tuning.

```
BEFORE (200 fixed shuffle partitions, many nearly empty):
[P1: 2MB][P2: 1MB][P3: 3MB][P4: 1MB] ... [P200: 2MB]
      → 200 tiny tasks, mostly scheduling overhead

AFTER AQE coalescing:
[Merged Partition A: 40MB][Merged Partition B: 38MB] ... 
      → far fewer, right-sized tasks
```

### 2. Dynamically Switching Join Strategies
- If, after a shuffle, one side of a join turns out to be **smaller than the broadcast threshold** (even though it wasn't estimated that way beforehand), AQE can **convert a `SortMergeJoin` into a `BroadcastHashJoin`** on the fly.
- This captures broadcast join benefits even when static statistics were wrong or unavailable (e.g., after a filter dramatically shrinks a table).

### 3. Dynamically Optimizing Skewed Joins
- AQE detects when a specific shuffle partition is **abnormally large** compared to the median partition size (a classic sign of key skew).
- It automatically **splits that oversized partition into smaller sub-partitions**, processed in parallel, and joins them correctly against the corresponding other side — preventing one straggler task from stalling the entire stage.

```
SKEWED PARTITION BEFORE AQE:
[Partition 1: 5MB][Partition 2: 4MB][Partition 3: 380MB ⚠️][Partition 4: 6MB]
                                        └── one straggler task holds up the whole stage

AFTER AQE SKEW HANDLING:
[Partition 1: 5MB][Partition 2: 4MB][P3a: 95MB][P3b: 95MB][P3c: 95MB][P3d: 95MB][Partition 4: 6MB]
                                        └── split into parallel sub-tasks, no more straggler
```

### How the re-planning loop works internally
```
1. Query starts; Catalyst produces an initial physical plan (based on estimates)
2. Plan is broken into "stages" at materialization points (shuffle exchanges)
3. Stage N executes
4. AQE's "QueryStageExec" collects ACTUAL runtime statistics from Stage N's output
5. AQE re-invokes the optimizer/planner for the REMAINING plan using real stats
6. Adjustments applied: coalesce partitions / switch join strategy / handle skew
7. Stage N+1 executes with the updated plan
8. Repeat until the query completes
```

---

## 🏗️ /architecture — Where AQE Fits

```
┌───────────────────────────────────────────────────────────────┐
│                    Spark SQL / DataFrame API                  │
└───────────────────────────┬───────────────────────────────────┘
                            ▼
┌───────────────────────────────────────────────────────────────┐
│         Catalyst Optimizer (initial, ESTIMATE-based plan)     │
└───────────────────────────┬───────────────────────────────────┘
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                 Execution begins, stage by stage               │
│  ┌────────────────┐   ┌─────────────────────────────────────┐  │
│  │   Stage runs   │──>│  AQE: measure REAL stats, re-plan   │  │
│  │  (materialize) │   │  remaining stages if beneficial     │  │
│  └────────────────┘   └─────────────────────────────────────┘  │
│                (this loop repeats at each shuffle boundary)    │
└────────────────────────────────────────────────────────────────┘
```

AQE doesn't replace Catalyst — it **extends** Catalyst's optimization into runtime, giving it a second (and third, and fourth...) chance to get the plan right using ground truth instead of guesses.

---

## 🔄 /flow — Full Lifecycle of a Query With AQE

```
1. Query planned normally by Catalyst using pre-execution estimates
2. Physical plan divided into stages at shuffle boundaries
3. Stage 1 executes → shuffle output written, REAL sizes now known
4. AQE checks: 
     - Are any post-shuffle partitions too small? → coalesce them
     - Did a join input turn out smaller than the broadcast threshold? → switch join
     - Is any partition abnormally large (skew)? → split it into sub-partitions
5. Remaining plan is updated accordingly
6. Stage 2 executes using the NEW, improved plan
7. Repeat steps 3–6 for each subsequent stage boundary
8. Final result produced — using a plan that reflects REALITY, not just estimates
```

---

## 💻 /examples — Enabling and Observing AQE

```python
# AQE is enabled by default in Spark 3.2+; explicitly ensure it's on:
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Key sub-features (all default true in modern Spark, but tunable):
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

df1 = spark.read.parquet("orders.parquet")
df2 = spark.read.parquet("customers.parquet").filter("region = 'APAC'")  # shrinks a lot

result = df1.join(df2, "customer_id")
result.explain(mode="formatted")
```

```
Look for these markers in the plan output:
- "AdaptiveSparkPlan isFinalPlan=true"   → confirms AQE was used
- "CustomShuffleReaderExec" / "coalesced" → confirms partition coalescing happened
- Join operator switching from SortMergeJoin to BroadcastHashJoin between
  the "before" and "after" plan snapshots (visible in Spark UI SQL tab)
```

```python
# Spark UI: "SQL / DataFrame" tab → click a query →
# toggle between "Initial Plan" and "Final Plan" to SEE what AQE changed
```

---

## 🏭 /production — Real-World Use Cases

1. **ETL pipelines with highly variable filter selectivity**: a filter that removes 5% of rows on one day and 95% on another (e.g., filtering by a promotional campaign flag) benefits enormously from AQE dynamically switching join strategies based on the *actual* post-filter size each run, instead of a single static plan that's wrong half the time.
2. **Ad-tech / event-log skew**: a small number of advertiser or user IDs generate a disproportionate share of events; AQE's automatic skew join handling removes the need for manual salting logic that used to be hand-maintained by engineers.
3. **Multi-tenant data platforms**: shared pipelines processing wildly different data volumes per tenant/customer benefit from AQE's dynamic partition coalescing — avoiding the need to hand-tune `spark.sql.shuffle.partitions` per tenant.
4. **Ad-hoc analytics / BI query layers**: interactive queries with unpredictable filter conditions (user-driven dashboards) benefit from AQE choosing the right join strategy per query, rather than relying on a one-size-fits-all static configuration.
5. **Reducing manual tuning burden across many pipelines**: platform/data engineering teams migrating hundreds of legacy Spark jobs to Spark 3.x have reported significant reductions in job failures and runtime variance simply from **enabling AQE**, without touching individual job logic.

---

## 🚀 /optimization — Tuning Levers

| Lever | What it does |
|---|---|
| `spark.sql.adaptive.enabled` | Master switch for AQE (default: true in Spark 3.2+) |
| `spark.sql.adaptive.coalescePartitions.enabled` | Enables dynamic merging of small post-shuffle partitions |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | Minimum target size per coalesced partition |
| `spark.sql.adaptive.skewJoin.enabled` | Enables automatic detection and splitting of skewed join partitions |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | Threshold multiplier vs median partition size to classify a partition as "skewed" |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | Separate (optional) threshold AQE uses at runtime for dynamic join-strategy switching |
| `spark.sql.adaptive.localShuffleReader.enabled` | Optimizes shuffle reads when no actual repartitioning is needed post-coalesce |
| Monitor **Spark UI's "Initial Plan" vs "Final Plan"** | Confirms exactly what AQE changed for a given query — critical for validating tuning |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Assuming AQE fixes bad code** — AQE optimizes execution *within* the plan Catalyst produced; it cannot fix fundamentally inefficient logic (e.g., unnecessary `.repartition()` calls, avoidable UDFs, or poor filter placement).
2. **Disabling AQE without realizing it** — some legacy configuration templates explicitly set `spark.sql.adaptive.enabled=false` from pre-3.0 tuning guides; carrying that forward silently loses all AQE benefits on modern Spark versions.
3. **Expecting AQE to help streaming continuously** — AQE's adaptive re-planning applies primarily within a single micro-batch's query execution in Structured Streaming, not across batches; it doesn't "learn" across batch boundaries.
4. **Ignoring skew join tuning parameters on extreme skew** — default skew thresholds work for typical cases, but extremely pathological skew (a single key with 90%+ of all data) may still need manual salting alongside AQE.
5. **Not checking the Spark UI's "Final Plan"** — assuming AQE made a beneficial change without confirming it, especially when debugging unexpected performance.
6. **Confusing AQE with Catalyst** — thinking AQE replaces the need for good static optimization (accurate table statistics via `ANALYZE TABLE` still matters for AQE's *initial* plan and coalescing decisions).

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: What is Adaptive Query Execution (AQE)?**
> A: A Spark 3.x feature that re-optimizes a query's physical plan at runtime, using actual statistics gathered after each stage (like post-shuffle partition sizes), instead of relying solely on pre-execution estimates.

**Q2: What are the three main optimizations AQE performs?**
> A: Dynamically coalescing small shuffle partitions, dynamically switching join strategies (e.g., sort-merge to broadcast), and dynamically optimizing skewed joins by splitting oversized partitions.

**Q3: How does AQE detect data skew?**
> A: By comparing each shuffle partition's size to the median partition size; a partition significantly larger than the threshold multiplier is classified as skewed and split into smaller sub-partitions for parallel processing.

**Q4: Why can't AQE re-plan everything from the very start of a query?**
> A: Real statistics are only available after a stage actually materializes (e.g., after a shuffle); AQE re-plans the *remaining* stages using this fresh data, not the whole query retroactively.

**Q5: Does AQE replace the need for Catalyst's cost-based optimizer?**
> A: No — Catalyst still produces the initial plan using available statistics; AQE extends and corrects that plan at runtime using ground-truth data Catalyst couldn't have known beforehand.

**Q6: Is AQE enabled by default?**
> A: Yes, in Spark 3.2 and later it's enabled by default (`spark.sql.adaptive.enabled=true`); earlier 3.x versions required enabling it explicitly.

**Q7: Can AQE convert a SortMergeJoin into a BroadcastHashJoin mid-query?**
> A: Yes — if, after a shuffle, one join input's actual size falls under the broadcast threshold (even if it wasn't estimated that way beforehand), AQE dynamically switches the join strategy for the remaining execution.

---

## ✅ /keytakeaways

- **AQE** lets Spark **re-optimize a query's plan at runtime**, using real post-stage statistics instead of only pre-execution estimates.
- Three core capabilities: **coalescing small shuffle partitions, switching join strategies dynamically, and splitting skewed partitions automatically**.
- It works by pausing at natural **stage/shuffle boundaries**, measuring actual data, and re-planning the *remaining* execution.
- AQE **extends Catalyst**, it doesn't replace it — good statistics and clean query logic still matter.
- Enabled **by default in modern Spark (3.2+)** — verify it's on, and check the Spark UI's "Final Plan" to confirm what it changed.
- AQE significantly reduces the need for **manual shuffle-partition tuning and manual skew-salting** in production pipelines.