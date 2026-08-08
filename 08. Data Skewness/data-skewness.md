# Spark Data Skewness — The Complete Guide (Beginner → Advanced)

## 🧠 /firstprinciples — What Is Data Skew, Really?

Spark's whole performance model is built on **parallelism**: split data into partitions, process them simultaneously across many executors, and the job finishes roughly as fast as its **slowest partition**.

**Data skew** happens when data isn't evenly distributed across partitions — usually because a shuffle groups records by key, and a small number of keys have **far more records** than the rest.

> Data skew is an imbalance in the size of partitions (usually caused by uneven key distribution during a shuffle), where one or a few partitions hold dramatically more data than the others.

The consequence is brutal: **99 tasks finish in 2 minutes, and 1 task takes 45 minutes**, because it's crunching a partition 50x larger than the rest — and the whole job waits on that single straggler.

**The one-sentence definition:**
> Data skew is when a shuffle-based operation distributes work so unevenly across partitions that one or a few tasks become bottlenecks, negating the benefit of parallelism entirely.

---

## 🍽️ Simple Analogy

Imagine a **restaurant kitchen with 10 chefs**, each assigned orders for specific tables.

- Under normal conditions, each chef gets roughly the same number of orders — everyone finishes around the same time, and the kitchen closes on schedule.
- Now imagine **one massive banquet table with 500 guests** gets assigned to a single chef, while the other 9 chefs handle regular tables of 5-10 people each.
- The 9 chefs finish in 20 minutes. The **1 chef with the banquet table is still cooking 3 hours later** — and the kitchen can't close, no matter how fast everyone else worked, because the whole service depends on **every** chef finishing.

That overloaded chef is a **straggler task**. The banquet table is a **skewed key**. And the kitchen waiting on one person despite having 9 idle chefs is exactly what a skewed Spark stage looks like on a cluster dashboard.

---

## 👁️ /visualizelearning — Balanced vs Skewed Partitions

```
BALANCED PARTITIONS (ideal — parallelism works as intended)
┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
│ P1: 2M │ │ P2: 2M │ │ P3: 2M │ │ P4: 2M │ │ P5: 2M │   rows each
└────────┘ └────────┘ └────────┘ └────────┘ └────────┘
   All 5 tasks finish in ~ the same time. Cluster fully utilized.


SKEWED PARTITIONS (one key dominates — straggler task)
┌────────┐ ┌────────┐ ┌──────────────────────────────┐ ┌────────┐ ┌────────┐
│ P1: 2M │ │ P2: 2M │ │      P3: 90M  ⚠️ SKEWED KEY     │ │ P4: 2M │ │ P5: 2M │
└────────┘ └────────┘ └──────────────────────────────┘ └────────┘ └────────┘
   4 tasks finish quickly, then SIT IDLE waiting on P3 — the entire
   stage's completion time = the time it takes to process the biggest partition
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

### Why skew happens
- Skew almost always originates during a **shuffle** (wide dependency), where Spark partitions data using a `HashPartitioner` (or `RangePartitioner`) applied to the **join/groupBy key**.
- If real-world data has a **non-uniform key distribution** — e.g., 40% of e-commerce orders come from one mega-retailer's `customer_id`, or a huge fraction of null/default values share a single key — then whichever partition that key hashes to becomes disproportionately large.

### How it shows up on the cluster
```
Normal stage progress:
[Task 1: ████████████ done] [Task 2: ████████████ done] ...
[Task 3: ████████████████████████████████████████████ still running] ⚠️

- Spark UI "Stages" tab shows huge variance in task duration within one stage
- Shuffle Read Size metric shows one task reading FAR more data than the median
- Executors assigned to other tasks sit idle, waiting for the stage to complete
```

### Detection signals
1. **Spark UI**: in the Stages tab, look at the **task duration distribution** (min/median/max) and **shuffle read size per task** — a huge gap between max and median is the smoking gun.
2. **Event Timeline**: visually shows most executors going idle while one keeps running.
3. **`.explain()` + sampling**: manually checking `df.groupBy(key).count().orderBy(desc("count"))` reveals which specific key(s) dominate.

### How AQE's skew handling works internally (Spark 3.x+)
- AQE compares each shuffle partition's size against the **median partition size**, using a configurable multiplier threshold (`spark.sql.adaptive.skewJoin.skewedPartitionFactor`).
- Any partition exceeding that threshold is **split into multiple sub-partitions**.
- For joins, Spark **duplicates the corresponding partition from the other side** so each sub-partition can still be joined correctly, then processes the sub-partitions **in parallel** across multiple tasks instead of one giant task.

```
BEFORE AQE skew handling:
[P3: 90MB] ── ONE task, ONE executor, 45 minutes

AFTER AQE skew handling:
[P3a: 22MB][P3b: 22MB][P3c: 23MB][P3d: 23MB] ── 4 tasks, 4 executors, ~11 minutes each, IN PARALLEL
```

### The manual technique: Salting
When automatic handling isn't enough (extreme skew, or older Spark versions), engineers manually **salt** the skewed key:
1. Append a random suffix (e.g., 0–9) to the skewed key on the large table side: `customer_id` → `customer_id_3`.
2. **Explode** the corresponding small/dimension table so each original key appears once per salt value (`customer_id_0` through `customer_id_9`).
3. Join on the salted key — this artificially splits the one hot key into 10 separate, evenly-sized partitions.
4. Optionally strip the salt suffix afterward if needed for downstream grouping.

```
BEFORE SALTING:                      AFTER SALTING:
key="A" (90M rows) ──▶ 1 partition   key="A_0" (9M) ──▶ partition 1
                                     key="A_1" (9M) ──▶ partition 2
                                     key="A_2" (9M) ──▶ partition 3
                                     ... (10 evenly-sized partitions instead of 1 giant one)
```

---

## 🏗️ /architecture — Where Skew Lives in the Job

```
┌────────────────────────────────────────────────────────────┐
│                     Stage N (shuffle stage)                │
│  ┌────────┐ ┌────────┐ ┌────────────────────┐ ┌────────┐   │
│  │ Task 1 │ │ Task 2 │ │  Task 3 (SKEWED)   │ │ Task 4 │   │
│  │ fast   │ │ fast   │ │  slow — bottleneck │ │ fast   │   │
│  └────────┘ └────────┘ └────────────────────┘ └────────┘   │
└────────────────────────────────────────────────────────────┘
                              │
                              ▼
          Stage N+1 CANNOT START until ALL of Stage N's
          tasks complete — so the ENTIRE JOB waits on the
          single skewed task, regardless of cluster size
```

Skew is a **stage-level** problem, but its effect is **job-level** — because stages execute in dependency order, one slow task in an early stage delays everything downstream.

---

## 🔄 /flow — Diagnosing and Fixing Skew, End to End

```
1. Job runs unusually slow / one stage takes disproportionately long
2. Open Spark UI → Stages tab → sort by duration, find the slow stage
3. Inspect Task metrics: look at max vs median task duration & shuffle read size
4. Confirm the cause: sample the join/groupBy key distribution
      df.groupBy(key).count().orderBy(desc("count")).show()
5. Choose a fix:
      a. Enable/verify AQE skew join handling is on (often enough alone)
      b. If join with small table involved → try a broadcast join instead
      c. If extreme/manual case → apply salting to the skewed key
6. Re-run job, compare task duration distribution in Spark UI — should be balanced
```

---

## 💻 /examples — Detecting and Fixing Skew

```python
# 1. Detect skew — find dominant keys before running the expensive job
df.groupBy("customer_id").count().orderBy(F.desc("count")).show(10)

# 2. Ensure AQE skew handling is enabled (default true in modern Spark)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 3. Prefer broadcast join if one side is small — sidesteps skew entirely
from pyspark.sql.functions import broadcast
result = orders.join(broadcast(customers), "customer_id")

# 4. Manual salting technique (when AQE isn't enough / extreme skew)
import pyspark.sql.functions as F

SALT_BUCKETS = 10

salted_orders = orders.withColumn(
    "salted_key",
    F.concat(F.col("customer_id"), F.lit("_"), (F.rand() * SALT_BUCKETS).cast("int"))
)

salted_customers = customers.withColumn(
    "salt", F.explode(F.array([F.lit(i) for i in range(SALT_BUCKETS)]))
).withColumn(
    "salted_key", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt"))
)

balanced_result = salted_orders.join(salted_customers, "salted_key")
```

---

## 🏭 /production — Real-World Use Cases

1. **Ad-tech / clickstream analytics**: a small number of "power users" or bot traffic IPs generate a massively disproportionate number of events; joins on `user_id` routinely hit severe skew without salting or broadcast strategies.
2. **Retail/e-commerce order joins**: a handful of enterprise/wholesale accounts generate orders of magnitude more transactions than typical customers, skewing any `customer_id`-based aggregation or join.
3. **Null/default-value skew**: pipelines joining on a foreign key where a large fraction of rows have `NULL` or a placeholder default value (e.g., "unknown region") — all those nulls hash to the same partition, creating accidental skew that's easy to overlook.
4. **Log processing pipelines**: server/application logs where one specific error code or service name dominates the volume; grouping by that field concentrates nearly all data into one partition.
5. **Financial transaction processing**: a small number of high-frequency trading accounts or merchant IDs can dwarf the transaction volume of typical accounts, causing severe skew in daily settlement/aggregation jobs — often requiring dedicated salting logic maintained specifically for those known "hot" IDs.

---

## 🚀 /optimization — Tuning Levers

| Lever | What it does |
|---|---|
| **Enable AQE skew join handling** | Automatically detects and splits oversized shuffle partitions at runtime |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | Threshold multiplier (vs median) for classifying a partition as skewed |
| **Broadcast join for the smaller side** | Eliminates the shuffle (and thus skew) entirely for that join |
| **Manual salting** | Artificially splits a known hot key into multiple sub-keys for even distribution |
| **Filter out/handle nulls or placeholder keys separately** | Prevents accidental skew from default/unknown values sharing one key |
| **Sample key distribution before running expensive jobs** | Early detection via `groupBy(key).count()` avoids surprises in production |
| **Increase `spark.sql.shuffle.partitions` cautiously** | Can help slightly by spreading non-skewed keys further, but does NOT fix a single dominant key on its own |
| **Two-phase aggregation** (partial local aggregation + final aggregation) | Reduces the effective size of what gets shuffled per key, softening (though not eliminating) skew impact |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Assuming more shuffle partitions fixes skew** — increasing `spark.sql.shuffle.partitions` spreads *non-skewed* keys more finely, but a single dominant key still hashes to one partition; it doesn't split that key's data at all.
2. **Not checking for null/default-value skew** — teams often hunt for a "real" business key causing skew and overlook that a huge share of rows simply have `NULL` in the join column, all landing in the same partition.
3. **Salting everything indiscriminately** — salting adds complexity (extra join keys, exploded dimension tables); applying it to keys that aren't actually skewed adds overhead for no benefit.
4. **Ignoring the Spark UI task duration distribution** — diagnosing "slow jobs" by guesswork instead of directly inspecting per-task metrics, which almost always reveal skew immediately if present.
5. **Relying solely on AQE for extreme skew** — AQE's automatic splitting helps a lot, but pathological cases (a single key holding 80%+ of all data) may still need manual salting or business-logic changes (e.g., special-casing known hot IDs).
6. **Forgetting to re-verify after a "fix"** — applying salting or broadcast joins without re-checking the Spark UI task distribution to confirm the fix actually balanced the load.

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: What is data skew in Spark?**
> A: An uneven distribution of data across partitions — typically caused by a shuffle key with disproportionately many records — causing one or a few tasks to take far longer than the rest and become the job's bottleneck.

**Q2: How do you detect data skew?**
> A: Check the Spark UI's Stages tab for large variance in task duration/shuffle read size, or directly sample key distribution with `df.groupBy(key).count().orderBy(desc("count"))`.

**Q3: How does AQE handle data skew automatically?**
> A: It compares each shuffle partition's size to the median; partitions exceeding a configurable threshold are split into smaller sub-partitions, processed in parallel, with the corresponding data from the join's other side duplicated as needed.

**Q4: What is salting and when would you use it?**
> A: Salting appends a random suffix to a skewed key to artificially split it into multiple sub-keys, spreading its data across several partitions; used when a key is extremely skewed and automatic handling (AQE) isn't sufficient.

**Q5: Can increasing the number of shuffle partitions fix skew?**
> A: Not on its own — more partitions help spread out non-skewed keys, but a single dominant key still lands entirely in one partition unless it's specifically salted or split.

**Q6: Why does a skewed task delay the entire job, even with many idle executors?**
> A: Because a stage can't complete (and downstream stages can't begin) until all of its tasks finish; idle executors with nothing left to do don't help the one task still processing a disproportionately large partition.

**Q7: How can a broadcast join help with skew?**
> A: By avoiding the shuffle entirely for the large table — since there's no repartitioning by key, a skewed key on the large side no longer creates an oversized partition or straggler task.

---

## ✅ /keytakeaways

- **Data skew** = uneven partition sizes, usually from an imbalanced shuffle key, causing one or a few **straggler tasks** to bottleneck the whole stage (and job).
- **Detect it** via the Spark UI's task duration/shuffle-read distribution, or by directly sampling key counts.
- **AQE's skew join handling** (Spark 3.x+) automatically splits oversized partitions into parallel sub-partitions — enable it by default.
- For extreme cases, use **manual salting** to artificially spread a hot key across multiple partitions.
- **Broadcast joins sidestep skew entirely** by avoiding the shuffle altogether for the large table.
- More executors or more shuffle partitions **do not fix skew on their own** — the fix must target the imbalanced key directly.
