# Spark On-Heap Memory — The Complete Guide

## 🧠 /firstprinciples — What Is On-Heap Memory, Really?

Every Spark Executor is a **JVM process**, and every JVM process has a **heap** — the region of memory managed by the Java Virtual Machine, where objects are allocated and periodically cleaned up by the **Garbage Collector (GC)**.

**On-heap memory** in Spark refers to memory that lives **inside this JVM-managed heap** — subject to Java's object model (headers, references, boxing) and, critically, subject to **garbage collection**. This is Spark's **default** memory mode: unless explicitly configured otherwise, everything an Executor does — running tasks, caching data, shuffling — happens within this on-heap space.

> On-heap memory is the portion of an Executor's memory that lives inside the JVM heap, managed and garbage-collected by the JVM itself, and internally subdivided by Spark's Unified Memory Manager into execution and storage regions.

**The one-sentence definition:**
> On-heap memory is Spark's default, JVM-managed memory space for an Executor — where cached data, task execution buffers, and Spark's internal bookkeeping objects all live, subject to Java garbage collection.

This matters because **GC behavior is one of the single biggest, most misunderstood sources of Spark performance problems** — and it all traces back to understanding what's actually happening inside this on-heap space.

---

## 🏠 Simple Analogy

Imagine an **apartment building's shared storage room**, managed by the building superintendent.

- Tenants (Spark tasks) can **store boxes** (data/objects) in the room — but the **superintendent (JVM Garbage Collector)** periodically walks through, checking which boxes are still claimed by someone and which have been abandoned, and **clears out the abandoned ones** to make room for new boxes.
- This inspection process takes **time**, and while the superintendent is actively walking the room checking boxes, **nobody else can use the room** (a GC pause) — the more boxes there are, the longer the inspection takes.
- If tenants store thousands of **tiny, individually labeled boxes** instead of a few large, consolidated ones, the superintendent's inspection becomes dramatically slower — more items to check, even if the total amount of stuff is the same.

**On-heap memory is this shared storage room.** JVM objects are the many tiny boxes. Garbage collection is the superintendent's inspection walk — and it's the reason Spark cares so much about **reducing object overhead** (via Tungsten's binary format) rather than just "having more memory."

---

## 👁️ /visualizelearning — Executor Memory Layout

```
┌────────────────────────────────────────────────────────────────────┐
│                    EXECUTOR CONTAINER (total memory)               │
│                                                                    │
│   ┌────────────────────────────────────────────────────────────┐   │
│   │              JVM HEAP (ON-HEAP MEMORY)                     │   │
│   │        controlled by --executor-memory                     │   │
│   │  ┌─────────────────────────────────────────────────────┐   │   │
│   │  │        Spark Unified Memory (spark.memory.fraction) │   │   │
│   │  │  ┌─────────────────────┐ ┌───────────────────┐      │   │   │
│   │  │  │  EXECUTION MEMORY   │ │  STORAGE MEMORY   │      │   │   │
│   │  │  │ (shuffles, joins,   │ │ (cached RDDs/     │      │   │   │
│   │  │  │  sorts, aggregation)│ │  DataFrames)      │      │   │   │
│   │  │  │      <── can borrow from each other ──>   │      │   │   │
│   │  │  └─────────────────────┘ └───────────────────┘      │   │   │
│   │  └─────────────────────────────────────────────────────┘   │   │
│   │  ┌─────────────────────────────────────────────────────┐   │   │
│   │  │   USER MEMORY (spark.memory.fraction remainder)     │   │   │
│   │  │   — your own data structures, UDF objects, etc.     │   │   │
│   │  └─────────────────────────────────────────────────────┘   │   │
│   │  ┌─────────────────────────────────────────────────────┐   │   │
│   │  │ RESERVED MEMORY (~300MB, fixed, for Spark internals)│   │   │
│   │  └─────────────────────────────────────────────────────┘   │   │
│   └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│   ┌────────────────────────────────────────────────────────────┐   │
│   │       OFF-HEAP MEMORY (optional, outside JVM heap,         │   │
│   │       controlled by spark.memory.offHeap.size)             │   │
│   └────────────────────────────────────────────────────────────┘   │
│                                                                    │
│   ┌────────────────────────────────────────────────────────────┐   │
│   │   OVERHEAD MEMORY (spark.executor.memoryOverhead) —        │   │
│   │   JVM overhead, native libs, PySpark's separate process    │   │
│   └────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

### The Unified Memory Manager (Spark 1.6+)
Within the on-heap space, Spark's **Unified Memory Manager** divides the memory governed by `spark.memory.fraction` (default 0.6, i.e., 60% of `(executor memory - reserved memory)`) into two logical regions:

- **Execution Memory**: used for computation that needs working space — shuffle buffers, hash tables for aggregation, sort buffers for sort-merge joins. This memory is **task-scoped and transient** — released once the task/operation completes.
- **Storage Memory**: used for **caching** (`.cache()`/`.persist()`) RDD/DataFrame partitions and broadcast variables.

**The key innovation of "unified" memory management**: these two regions can **dynamically borrow space from each other** based on demand, governed by `spark.memory.storageFraction` (default 0.5, meaning storage has a *protected minimum* of 50% of the unified pool that execution can't evict). If execution needs more space and storage isn't using its full share, execution can borrow it — and vice versa — rather than each having a rigid, wasteful fixed allocation (as in pre-1.6 Spark).

```
Execution needs more memory, Storage has spare capacity:
┌───────────────────┐┌──────────────────────┐
│   EXECUTION       ││    STORAGE           │
│   (growing) ──────>│   (shrinks,          │
│                   ││    but only down     │
│                   ││    to protected min) │
└───────────────────┘└──────────────────────┘

If Execution needs even MORE and Storage is at its protected minimum:
Storage's cached blocks may be EVICTED (using LRU) to free space for execution —
this is why heavy caching + heavy shuffling in the SAME job can cause cache evictions
```

### Garbage Collection mechanics
- The JVM's heap is further internally divided into **generations**: the **Young Generation** (for newly created, short-lived objects) and the **Old Generation** (for longer-lived objects that survive multiple GC cycles).
- **Minor GC**: cleans the Young Generation frequently, usually fast.
- **Major/Full GC**: cleans the Old Generation, and is **much more expensive** — often causing a **"stop-the-world" pause** where all task threads in that JVM freeze until GC completes.
- Spark's typical workload pattern (millions of short-lived, per-row objects when using RDD-based or non-Tungsten code) can **flood the Young Generation**, causing objects to be promoted to the Old Generation faster than expected, triggering more frequent, expensive Full GCs.

```
   Object created (row processed)
              │
              ▼
     ┌────────────────────┐
     │  Young Generation  │  ── most objects die here quickly (Minor GC, fast)
     └────────┬───────────┘
              │ survives multiple Minor GCs
              ▼
     ┌────────────────────┐
     │   Old Generation   │  ── Full GC here is EXPENSIVE (stop-the-world pause)
     └────────────────────┘
```

### Why Tungsten specifically targets on-heap memory
- Recall from Project Tungsten: Spark's `UnsafeRow` binary format packs row data into compact byte arrays instead of many small JVM objects.
- This directly reduces **on-heap object count**, meaning the GC has **far fewer individual items to track and scan** — a smaller, more efficient Young/Old Generation footprint, fewer and shorter GC pauses.
- This is also why **off-heap memory** exists as an option: data stored off-heap (outside the JVM heap entirely) is **completely invisible to the JVM garbage collector**, sidestepping GC overhead for that data entirely — at the cost of manual memory management via `sun.misc.Unsafe`.

---

## 🏗️ /architecture — On-Heap vs Off-Heap, Side by Side

```
ON-HEAP MEMORY                               OFF-HEAP MEMORY
┌───────────────────────────┐               ┌─────────────────────────┐
│   Inside the JVM heap     │               │   Outside the JVM heap  │
│   Subject to GC           │               │   NOT subject to GC     │
│   Default mode            │               │   Opt-in (spark.memory. │
│                           │               │   offHeap.enabled=true) │
│   Managed automatically   │               │   Managed manually via  │
│   by JVM                  │               │   sun.misc.Unsafe       │
│   Simpler, but GC pauses  │               │   No GC pauses for this │
│   possible                │               │   memory, but requires  │
│                           │               │   careful sizing        │
└───────────────────────────┘               └─────────────────────────┘
```

`spark.executor.memory` controls the **on-heap** JVM size. `spark.memory.offHeap.size` (with `spark.memory.offHeap.enabled=true`) controls an **additional**, separate pool of off-heap memory Spark can use for the same execution/storage purposes — the two are **complementary**, not mutually exclusive.

---

## 🔄 /flow — How Memory Is Used Across a Task's Lifecycle

```
1. Executor JVM launches with a fixed on-heap size (--executor-memory)
2. Reserved memory (~300MB) is carved out first, for Spark internals
3. Remaining memory split: spark.memory.fraction → Unified Memory (exec+storage)
                            remainder → User Memory (your own objects/UDFs)
4. A Task starts running:
     - needs working memory (e.g., a hash map for aggregation)
       → allocated from Execution Memory
     - if the task caches a partition (.cache() triggered)
       → stored in Storage Memory
5. If Execution needs more than its current share and Storage has spare
   capacity → borrows from Storage dynamically
6. If Execution needs even more and Storage is at its protected minimum
   → cached blocks may be EVICTED (LRU) to free space
7. As objects become garbage (task completes, intermediate buffers freed)
   → JVM's Garbage Collector reclaims that on-heap space
     (Minor GC for short-lived objects, Major/Full GC for longer-lived ones)
```

---

## 💻 /examples — Configuring and Observing On-Heap Memory

```bash
spark-submit \
  --executor-memory 16g \                          # total ON-HEAP JVM size
  --conf spark.memory.fraction=0.6 \                # unified memory share (exec+storage)
  --conf spark.memory.storageFraction=0.5 \         # protected minimum for storage
  --conf spark.memory.offHeap.enabled=true \        # ALSO enable off-heap (complementary)
  --conf spark.memory.offHeap.size=4g \
  --conf spark.executor.memoryOverhead=2g \         # separate from heap entirely
  my_job.py
```

```python
# Observing GC behavior via the Spark UI's Tasks tab:
# Look at the "GC Time" column per task — high values relative to
# task duration indicate significant on-heap memory/GC pressure

# Programmatically inspecting JVM GC info requires JVM-level tools
# (e.g., adding -XX:+PrintGCDetails to spark.executor.extraJavaOptions)
spark.conf.set(
    "spark.executor.extraJavaOptions",
    "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
)
```

```python
# A workload pattern that stresses on-heap memory heavily (RDD, many small objects)
rdd = sc.textFile("huge_file.txt").map(lambda line: line.split(","))
# Every parsed row becomes several small JVM objects — heavy GC pressure

# The DataFrame equivalent leverages Tungsten's binary format,
# dramatically reducing on-heap object count for the same logical operation
df = spark.read.csv("huge_file.txt")
```

---

## 🏭 /production — Real-World Use Cases

1. **Diagnosing GC-bound jobs**: teams noticing high "GC Time" in the Spark UI's Tasks tab for RDD-heavy legacy pipelines often migrate that logic to the DataFrame API specifically to benefit from Tungsten's reduced on-heap object footprint, cutting GC pauses significantly.
2. **Balancing cache size vs execution needs in shared workloads**: jobs that both cache large lookup tables AND perform heavy shuffling in the same run tune `spark.memory.storageFraction` carefully to avoid a situation where execution memory pressure repeatedly evicts useful cached data mid-job.
3. **Enabling off-heap memory for large, long-running Structured Streaming jobs**: since GC pauses directly threaten streaming micro-batch SLAs, teams running latency-sensitive streaming pipelines often enable `spark.memory.offHeap.enabled` to move a portion of memory outside GC's reach entirely.
4. **Right-sizing `--executor-memory` for wide vs narrow executor strategies**: teams choosing between "many small executors" and "few large executors" factor in on-heap GC behavior — very large heaps (e.g., 64GB+ per executor) can experience longer Full GC pauses, sometimes making moderately-sized executors a better trade-off despite lower per-executor capacity.
5. **PySpark memory overhead planning**: since PySpark's actual data processing happens partly in a **separate Python process** outside the JVM heap entirely, teams must budget `spark.executor.memoryOverhead` carefully in addition to on-heap sizing — a common source of "why is my on-heap memory fine but my executor still got killed" incidents.

---

## 🚀 /optimization — Tuning Levers

| Lever | What it does |
|---|---|
| `spark.executor.memory` | Sets the total on-heap JVM size for each executor |
| `spark.memory.fraction` | Controls how much of (heap − reserved) goes to Spark's unified execution+storage pool vs user memory |
| `spark.memory.storageFraction` | Sets the protected minimum share of unified memory reserved for cached data (can't be evicted below this by execution pressure) |
| **Prefer DataFrame/SQL over RDDs** | Leverages Tungsten's compact binary format, dramatically reducing on-heap object count and GC pressure |
| `spark.memory.offHeap.enabled` + `.size` | Moves a portion of memory outside the JVM heap entirely, sidestepping GC overhead for that data |
| Avoid excessively large single-executor heaps | Very large heaps can suffer longer Full GC pauses; moderately-sized executors sometimes trade capacity for more predictable GC behavior |
| Monitor **GC Time** in Spark UI Tasks tab | Direct, measurable signal of on-heap memory/GC pressure per task |
| Tune `spark.executor.extraJavaOptions` for GC algorithm choice | e.g., G1GC is commonly recommended over the default for large heaps, reducing pause times |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Assuming "more memory" always fixes GC problems** — a larger heap with the same object-heavy (RDD-based) workload can actually make Full GC pauses *longer*, not shorter, since there's simply more heap to scan.
2. **Not distinguishing on-heap memory issues from off-heap/overhead issues** — an executor killed by the cluster manager (container OOM) despite healthy-looking on-heap GC metrics is very often an **overhead memory** problem (especially in PySpark), not an on-heap one.
3. **Ignoring `spark.memory.storageFraction` interactions** — mixing heavy caching and heavy shuffling in the same job without understanding that execution memory pressure can silently evict useful cached data.
4. **Using RDD-heavy code out of habit** — missing out on Tungsten's on-heap memory efficiency gains that come "for free" simply by using the DataFrame/Dataset API instead.
5. **Never checking GC Time in the Spark UI** — diagnosing "slow tasks" purely by data volume assumptions, missing that a large share of task duration is actually spent in garbage collection.
6. **Overlooking JVM GC algorithm choice** — running with default GC settings on very large executor heaps without considering tuned alternatives (e.g., G1GC) that are often better suited to Spark's allocation patterns.

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: What is on-heap memory in the context of a Spark Executor?**
> A: The portion of an executor's memory that lives inside the JVM heap, managed and garbage-collected by the JVM, and internally divided by Spark's Unified Memory Manager into execution and storage regions.

**Q2: What is the Unified Memory Manager, and why was it introduced?**
> A: Introduced in Spark 1.6, it allows execution memory and storage memory to dynamically borrow space from each other based on demand (subject to a protected minimum for storage), replacing the earlier rigid, fixed-split memory model that often wasted capacity.

**Q3: What's the difference between execution memory and storage memory?**
> A: Execution memory is transient, task-scoped memory used for shuffles, joins, sorts, and aggregations; storage memory holds cached RDD/DataFrame partitions and broadcast variables.

**Q4: Why does high GC Time in the Spark UI matter?**
> A: It signals significant time spent on garbage collection relative to actual task processing — often caused by excessive small object allocation (common in RDD-heavy code), pointing toward memory tuning or a shift to the DataFrame API as a fix.

**Q5: How does off-heap memory relate to on-heap memory?**
> A: They're complementary, separately configured memory pools — on-heap lives inside the JVM heap and is subject to GC; off-heap lives outside the JVM heap (managed manually via `sun.misc.Unsafe`) and is not subject to GC at all.

**Q6: Why can a very large executor heap sometimes hurt performance?**
> A: Full/Major garbage collection pauses generally scale with heap size — a very large heap can experience longer stop-the-world pauses when Full GC eventually runs, sometimes making moderately-sized executors preferable for GC predictability.

**Q7: Why does Project Tungsten's binary row format reduce on-heap memory pressure?**
> A: By packing row data into compact byte arrays instead of many individual small JVM objects, it drastically reduces the number of objects the garbage collector has to track and scan, shrinking both GC frequency and pause duration.

---

## ✅ /keytakeaways

- **On-heap memory** = the portion of an Executor's memory inside the JVM heap, subject to garbage collection — Spark's default memory mode.
- Spark's **Unified Memory Manager** splits on-heap memory into **execution** (transient, task-scoped) and **storage** (cached data) regions, which can **dynamically borrow from each other**, with a protected minimum for storage.
- **GC pauses** (especially Full/Major GC on the Old Generation) are a major, measurable source of Spark performance problems — visible directly as **GC Time** in the Spark UI.
- **Project Tungsten's compact binary format** exists specifically to reduce on-heap object count, cutting GC overhead.
- **Off-heap memory** is a complementary, opt-in pool entirely outside JVM/GC reach — useful for GC-sensitive, long-running jobs.
- Don't assume "more heap" always helps — very large heaps can suffer **longer** Full GC pauses; monitor and tune deliberately.