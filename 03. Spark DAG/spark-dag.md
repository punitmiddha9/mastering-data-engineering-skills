# Spark DAG (Directed Acyclic Graph) — The Complete Guide (Beginner → Advanced)

## 🧠 /firstprinciples — What Is a DAG, Really?

When you write Spark code, you're not actually "running" anything line by line the way a normal script executes. Instead, Spark builds a **map of all the operations and their dependencies** first — and only executes that map when it absolutely has to (i.e., when an **action** is called).

That map is a **DAG — Directed Acyclic Graph**:

- **Directed**: every edge has a direction — data flows from one operation to the next (parent → child).
- **Acyclic**: no cycles — you can never loop back to an earlier operation. Data only flows forward.
- **Graph**: nodes are RDDs/DataFrame stages (transformations), edges are dependencies between them.

Spark builds this DAG lazily as you chain `.filter()`, `.map()`, `.groupBy()`, `.join()`, etc. (all **transformations**, which are *lazy*). Nothing actually runs until you call an **action** (`.count()`, `.collect()`, `.write()`, `.show()`) — at that point, Spark looks at the entire DAG, optimizes it, and executes it as a series of stages and tasks.

**The one-sentence definition:**
> A Spark DAG is the complete, lazily-built execution blueprint of your job — a graph of transformations and dependencies that Spark analyzes as a whole before running anything, enabling global optimization instead of executing operations one at a time.

---

## 🗺️ Simple Analogy

Imagine planning a **road trip with multiple stops**: pick up a friend, get gas, buy groceries, drop off a package, then arrive home.

- If you executed each step the instant you thought of it (**eager execution**), you might drive to get gas, then realize the grocery store was actually right next to the gas station and you should've done both in one stop — too late, you already left.
- Instead, imagine you **plan the entire route first** (build the DAG), looking at all stops together. Now you can **reorder stops** to minimize driving distance, **combine nearby errands**, and skip a stop entirely if it turns out unnecessary — **before turning the key**.

Spark's DAG is exactly this: **a full plan built before any actual "driving" (computation) happens**, which is what allows the Catalyst optimizer and DAG Scheduler to make smart global decisions instead of naive step-by-step execution.

---

## 👁️ /visualizelearning — From Code to DAG to Stages

```
YOUR CODE:
rdd = sc.textFile("logs.txt")
       .map(parse_line)
       .filter(lambda x: x.status == "ERROR")
       .map(lambda x: (x.service, 1))
       .reduceByKey(lambda a, b: a + b)
rdd.collect()   # <-- ACTION triggers DAG execution

LOGICAL DAG (built lazily, nothing executed yet):

  textFile ──▶ map(parse) ──▶ filter(ERROR) ──▶ map(key,1) ──▶ reduceByKey ──▶ collect
   (read)      (narrow)         (narrow)          (narrow)      (WIDE - shuffle!)

STAGES (DAG split at shuffle boundaries):

┌─────────────────────────────────────────┐   SHUFFLE   ┌───────────────────┐
│  STAGE 1 (narrow deps, pipelined)         │ ─────────▶ │  STAGE 2           │
│  textFile → map → filter → map            │             │  reduceByKey →     │
│  (all fused into ONE set of tasks)        │             │  output            │
└─────────────────────────────────────────┘             └───────────────────┘
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

### 1. Lazy DAG construction
- Every transformation (`.map`, `.filter`, `.join`, etc.) just **adds a node** to the logical DAG — no data is touched.
- Spark tracks two kinds of dependencies between nodes:
  - **Narrow dependency**: each parent partition contributes to exactly one child partition (`map`, `filter`) — can be **pipelined** together without moving data.
  - **Wide dependency**: a parent partition may contribute to multiple child partitions (`groupByKey`, `join`, `repartition`) — **requires a shuffle**.

### 2. DAG Scheduler — splits the DAG into Stages
- When an **action** is called, the **DAGScheduler** (running on the Driver) walks the DAG **backwards from the final action**.
- It splits the DAG into **Stages** at every **wide dependency (shuffle boundary)**.
- All narrow-dependency operations within a stage are **pipelined/fused** into a single chain of tasks — no intermediate materialization needed.

### 3. Task Scheduler — turns Stages into Tasks
- Each Stage is split into **Tasks** — one task per partition.
- Tasks are submitted to the **TaskScheduler**, which assigns them to executors based on **data locality** (preferring executors that already hold the relevant data block).
- Stages execute **in dependency order**; a stage can't start until all of its parent stages have completed (for shuffle stages, until shuffle output is available).

```
                     ACTION CALLED (e.g. .collect())
                                │
                                ▼
                    ┌─────────────────────────┐
                    │     DAG Scheduler       │
                    │  (splits DAG → Stages   │
                    │   at shuffle boundaries)│
                    └───────────┬─────────────┘
                                │  Stage graph
                                ▼
                    ┌─────────────────────────┐
                    │    Task Scheduler       │
                    │ (splits Stage → Tasks,  │
                    │  assigns to executors   │
                    │  using data locality)   │
                    └───────────┬─────────────┘
                                │
                                ▼
                     Executors run Tasks in parallel
```

### 4. Fault Tolerance via the DAG (RDD Lineage)
- Because the DAG records **exactly how each partition was derived** (its "lineage"), if a partition is lost (executor crash), Spark can **recompute just that lost partition** by replaying the relevant part of the DAG — no need to restart the whole job. This is the foundation of Spark's resilience (the "R" in RDD).

---

## 🏗️ /architecture — DAG, Stages, and Tasks Hierarchy

```
                             JOB
                    (triggered by one action)
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
          STAGE 1          STAGE 2          STAGE 3
       (narrow deps,     (narrow deps,    (narrow deps,
        pipelined)        pipelined)       pipelined)
              │               │               │
       ┌──────┼──────┐ ┌──────┼──────┐ ┌──────┼──────┐
       ▼      ▼      ▼ ▼      ▼      ▼ ▼      ▼      ▼
     Task1  Task2 Task3  Task1  Task2  Task3   ...  TaskN
   (one task per partition, run in parallel across executors)
```

- **1 Job** ⟶ triggered by 1 action
- **1 Job** ⟶ made of **1+ Stages**, split at shuffle boundaries
- **1 Stage** ⟶ made of **1+ Tasks**, one per partition

---

## 🔄 /flow — Full Lifecycle of a DAG-Driven Job

```
1. Transformations chained → build logical DAG (lazy, nothing runs)
2. Action called (.collect(), .count(), .write()...) → triggers execution
3. DAGScheduler walks the DAG backwards from the action
4. DAG is split into Stages at shuffle (wide-dependency) boundaries
5. Each Stage split into Tasks (1 per partition)
6. TaskScheduler assigns tasks to executors (data-locality aware)
7. Stage 1 tasks run in parallel → output ready (shuffle files, if needed)
8. Stage 2 begins only once Stage 1's shuffle output is available
9. Final stage's output → returned to driver (collect) or written to storage
10. If a task fails → DAG lineage used to recompute ONLY the lost partition
```

---

## 💻 /examples — Seeing the DAG in Practice

```python
df = spark.read.parquet("sales.parquet")

result = (
    df.filter(df.amount > 100)         # narrow — no shuffle
      .withColumn("tax", df.amount * 0.1)  # narrow — no shuffle
      .groupBy("region")                # WIDE — shuffle boundary
      .sum("amount")
      .orderBy("sum(amount)")           # WIDE — another shuffle boundary
)

result.explain()          # shows the physical plan / stages
```

```python
# Visualize the DAG directly in the Spark UI:
# http://<driver-host>:4040 → "Jobs" tab → click a job → "DAG Visualization"
# Shows exactly how stages are split and where shuffle boundaries occur
```

```python
# Narrow-only chain — stays in ONE stage (no shuffle)
df.filter(df.amount > 100).select("region", "amount").withColumn("flag", df.amount > 500)

# Adding .repartition() forces an EXTRA stage boundary even without
# an aggregation — because repartition is itself a wide dependency
df.repartition(50)
```

---

## 🏭 /production — Real-World Use Cases

1. **Debugging slow jobs via the Spark UI DAG visualization**: engineers routinely open the "Jobs" tab to see exactly where a job spends most of its time — often revealing an unexpectedly large or skewed stage caused by a hidden shuffle (e.g., an implicit `repartition` from a config default).
2. **Multi-step ETL pipeline design**: understanding DAG lineage lets teams strategically place `.cache()`/`.persist()` calls at reused branch points in the DAG, avoiding expensive recomputation of the same upstream transformations across multiple downstream actions.
3. **Fault recovery in long-running batch jobs**: on large clusters where spot/preemptible instances are common, Spark's DAG-based lineage recovery (recomputing only lost partitions) is what makes using cheap, interruptible compute practical at all.
4. **Streaming micro-batch pipelines (Structured Streaming)**: each micro-batch builds and executes its own DAG; understanding this helps engineers reason about **why** certain stateful operations (like windowed aggregations) introduce additional shuffle stages per batch.
5. **Cost attribution and capacity planning**: platform teams use stage-level DAG metrics (from the Spark History Server) to identify which specific stages of a shared pipeline are consuming the most cluster resources, informing where to invest optimization effort.

---

## 🚀 /optimization — Tuning Levers

| Lever | What it does |
|---|---|
| **Cache/persist reused DAG branches** (`.cache()`, `.persist()`) | Avoids recomputing the same upstream transformations multiple times when several actions/branches depend on them |
| **Minimize wide dependencies** | Fewer shuffle boundaries = fewer, cheaper stages; prefer `reduceByKey` over `groupByKey`, use broadcast joins where possible |
| **Use the Spark UI DAG visualization** | Directly inspect stage boundaries, task counts, and skew per stage to target optimization effort |
| **Avoid unnecessary `.repartition()`** | Each one is an extra wide-dependency stage boundary — only use when parallelism genuinely needs adjusting |
| **Coalesce narrow transformations together** | Chaining `.filter().map().select()` before any wide op keeps everything pipelined in a single stage — no extra cost from chaining more narrow ops |
| **Enable AQE** (`spark.sql.adaptive.enabled=true`) | Dynamically coalesces small post-shuffle partitions and optimizes skewed stages at runtime |
| **Checkpoint very long DAGs** (`.checkpoint()`) | For iterative algorithms (e.g., ML training loops), truncates an ever-growing lineage graph that would otherwise slow down planning/recovery |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Not realizing transformations are lazy** — expecting `.map()` or `.filter()` alone to "do something," and being confused when nothing appears to happen until an action is called.
2. **Triggering the DAG multiple times unintentionally** — calling multiple actions (`.count()` then `.collect()`) on the same unpersisted DataFrame recomputes the **entire upstream DAG each time**, including expensive joins/reads.
3. **Chaining excessive `.repartition()` calls** "just in case," creating unnecessary extra stages/shuffles that only slow the job down.
4. **Extremely deep DAGs in iterative algorithms** (e.g., a loop that keeps transforming the same DataFrame hundreds of times) without `.checkpoint()` — lineage grows so large that both planning and any potential recovery become very slow, sometimes causing driver stack overflows.
5. **Misreading the Spark UI DAG** — assuming stage order in the UI reflects wall-clock execution order for independent branches, when Spark may actually run independent stages concurrently if resources allow.
6. **Ignoring skewed stages** shown in the DAG/stage view — a single long-running task within a stage (visible as a straggler in the UI) is often a skew problem masquerading as "Spark is just slow."

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: What is a DAG in Spark?**
> A: A Directed Acyclic Graph representing all transformations and their dependencies in a Spark job, built lazily and only executed when an action is triggered.

**Q2: Why does Spark use lazy evaluation with a DAG instead of executing eagerly?**
> A: It lets Spark see the entire computation graph before running anything, enabling global optimizations (like combining/reordering operations, pipelining narrow transformations, and minimizing shuffles) instead of executing step-by-step.

**Q3: How does the DAG get split into stages?**
> A: The DAGScheduler splits the DAG into stages at every wide dependency (shuffle boundary); narrow dependencies within a stage are pipelined into a single set of tasks.

**Q4: What's the difference between a Job, a Stage, and a Task?**
> A: A Job is triggered by one action; a Job consists of one or more Stages, split at shuffle boundaries; each Stage consists of one Task per partition, run in parallel across executors.

**Q5: How does the DAG enable fault tolerance?**
> A: Each RDD/DataFrame partition's lineage (its derivation history in the DAG) is tracked, so if a partition is lost, Spark can recompute just that partition by replaying its lineage instead of restarting the whole job.

**Q6: What's the difference between a narrow and a wide dependency?**
> A: A narrow dependency means each parent partition maps to exactly one child partition (no shuffle, e.g. `map`/`filter`); a wide dependency means a parent partition can contribute to multiple child partitions (requires a shuffle, e.g. `groupByKey`/`join`).

**Q7: Why would you cache a DataFrame in the middle of a DAG?**
> A: To avoid Spark recomputing the same upstream lineage multiple times when multiple actions or downstream branches depend on that same intermediate result.

---

## ✅ /keytakeaways

- Spark builds a **lazy DAG** of all transformations first; nothing executes until an **action** is called.
- **Narrow dependencies** (map/filter) get pipelined together; **wide dependencies** (groupBy/join/repartition) create **new stage boundaries** (shuffles).
- Hierarchy: **1 Job → 1+ Stages (split at shuffles) → 1+ Tasks (1 per partition)**.
- The DAG's **lineage** is the basis of Spark's fault tolerance — lost partitions are recomputed, not the whole job.
- Use the **Spark UI's DAG visualization** to debug slow stages, spot skew, and confirm expected shuffle boundaries.
- **Cache reused branches**, minimize unnecessary wide dependencies, and **checkpoint** very long/iterative DAGs to keep planning and recovery fast.
