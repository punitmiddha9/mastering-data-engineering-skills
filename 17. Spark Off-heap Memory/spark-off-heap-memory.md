# Spark Off-Heap Memory — The Complete Guide

## 🧠 /firstprinciples — What Is Off-Heap Memory, Really?

By default, every byte a Spark Executor uses lives **inside the JVM heap** — which means it's visible to, and managed by, the **Java Garbage Collector**. That's convenient, but as covered in on-heap memory: GC has real costs, especially as heaps grow large and object counts climb into the millions.

**Off-heap memory** is Spark's answer: a pool of memory allocated **completely outside the JVM heap**, using low-level direct memory access (`sun.misc.Unsafe`), that the **JVM garbage collector doesn't know exists and never scans**.

> Off-heap memory is a separately configured memory region, outside the JVM's managed heap, that Spark allocates and manages manually — storing data as raw bytes, invisible to (and unaffected by) Java garbage collection.

**The one-sentence definition:**
> Off-heap memory trades the JVM's automatic memory management (and its GC overhead) for manually-managed raw byte buffers that Spark controls directly — eliminating GC pauses for that data, at the cost of Spark (not the JVM) being responsible for allocating and freeing it correctly.

This isn't a competing feature to on-heap memory — it's a **complementary, opt-in pool** that sits alongside it, primarily used by Tungsten's binary row storage and by network/serialization buffers.

---

## 🏚️ Simple Analogy

Continuing the apartment building analogy from on-heap memory: the shared storage room (on-heap) has a superintendent who periodically inspects every box (garbage collection) — helpful, but disruptive while it happens.

**Off-heap memory is like renting a separate storage unit at a facility down the street** — completely outside the building, with **no superintendent, no inspections, ever**. You (Spark) are now **fully responsible** for knowing exactly what's stored there and cleaning it up yourself when you're done — nobody's coming to check on it or clear it out automatically.

The upside: **zero inspection disruptions** for anything stored there. The downside: **if you forget to clean something up, it just sits there forever, taking up space, with no automatic help.**

---

## 👁️ /visualizelearning — On-Heap vs Off-Heap Memory Flow

```
ON-HEAP MEMORY                              OFF-HEAP MEMORY
┌────────────────────────────┐                ┌────────────────────────────┐
│      JVM HEAP              │                │   Direct/Native Memory     │
│  ┌─────────────────────┐   │                │  ┌─────────────────────┐   │
│  │  Java Objects       │   │                │  │  Raw byte buffers   │   │
│  │  (headers, refs,    │   │                │  │  (allocated via     │   │
│  │   boxing overhead)  │   │                │  │   sun.misc.Unsafe)  │   │
│  └─────────────────────┘   │                │  └─────────────────────┘   │
│           │                │                │           │                │
│           ▼                │                │           ▼                │
│  ┌─────────────────────┐   │                │  ┌─────────────────────┐   │
│  │  Garbage Collector  │   │                │  │NO GC — Spark must   │   │
│  │  periodically scans │   │                │  │explicitly free this │   │
│  │  & reclaims memory  │   │                │  │memory itself        │   │
│  │  (STOP-THE-WORLD    │   │                │  │(no scan, no pause)  │   │
│  │   pauses possible)  │   │                │  │                     │   │
│  └─────────────────────┘   │                │  └─────────────────────┘   │
└────────────────────────────┘                └────────────────────────────┘
   Simpler, automatic, but GC pauses            Manual, but zero GC overhead
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

### How off-heap allocation actually works
- Spark uses `sun.misc.Unsafe` (a low-level Java API providing direct, unmanaged memory access, similar in spirit to `malloc`/`free` in C) to allocate a contiguous block of native memory outside the JVM heap.
- Data stored there is represented in **Tungsten's `UnsafeRow` binary format** — the same compact, packed byte layout used for on-heap Tungsten rows, just placed in a different memory region entirely.
- Because it's raw bytes with a fixed, known layout, Spark's generated (Whole-Stage CodeGen) code can read/write this memory **directly by pointer arithmetic and offsets** — no object deserialization step needed, and definitely no GC involvement.

```
   spark.memory.offHeap.enabled = true
   spark.memory.offHeap.size = 4g
              │
              ▼
┌────────────────────────────────────────────────────┐
│         Off-Heap Memory Pool (4GB)                 │
│  ┌───────────────┐┌───────────────┐┌────────────┐  │
│  │ UnsafeRow     ││ UnsafeRow     ││ UnsafeRow  │  │
│  │ (packed bytes)││ (packed bytes)││ (packed... │  │
│  └───────────────┘└───────────────┘└────────────┘  │
│      allocated/freed manually by Spark's code,     │
│      using sun.misc.Unsafe — NEVER touched by      │
│      the JVM's garbage collector                   │
└────────────────────────────────────────────────────┘
```

### Where off-heap memory gets used in Spark
1. **Execution & Storage memory (Tungsten)**: when `spark.memory.offHeap.enabled=true`, Spark's Unified Memory Manager can allocate **execution memory (shuffle/sort/aggregation buffers)** and **storage memory (cached data)** from this off-heap pool instead of (or in addition to) the on-heap pool — governed by `spark.memory.offHeap.size`.
2. **Network/shuffle transfer buffers**: Spark's shuffle subsystem (built on Netty) commonly uses **direct ByteBuffers** for network I/O — these are inherently off-heap-style buffers, more efficient for transferring data across the network since the OS can write directly to/from them without an extra JVM-heap copy step.
3. **PySpark's Arrow-based data exchange**: when using Apache Arrow for efficient Python↔JVM data transfer (e.g., Pandas UDFs), data is represented in a **columnar, off-heap-friendly format**, avoiding costly row-by-row serialization overhead between the two processes.

### The trade-off, concretely
- **Benefit**: zero GC scanning/pause cost for this memory — critical for **latency-sensitive** workloads (streaming) or workloads with **very large heaps** where Full GC pauses would otherwise be long.
- **Cost**: Spark (not the JVM) is now responsible for correctly freeing this memory; bugs or misconfiguration here can lead to **native memory leaks** that don't show up in typical JVM heap monitoring at all — a different, less familiar class of problem for most JVM-focused engineers.
- **Cost**: off-heap memory must be sized **explicitly and separately** (`spark.memory.offHeap.size`) — it doesn't share the same pool as `--executor-memory`, so under- or over-sizing it is a distinct configuration mistake from on-heap sizing.

---

## 🏗️ /architecture — Where Off-Heap Fits in the Executor's Total Footprint

```
┌───────────────────────────────────────────────────────────────────────┐
│                     EXECUTOR CONTAINER (total)                        │
│                                                                       │
│  ┌───────────────────────────────┐   ┌────────────────────────────┐   │
│  │      ON-HEAP (JVM heap)       │   │       OFF-HEAP MEMORY      │   │
│  │  --executor-memory            │   │  spark.memory.offHeap.size │   │
│  │  Unified Memory + User Memory │   │  (only used if             │   │
│  │  + Reserved Memory            │   │   offHeap.enabled=true)    │   │
│  │  Subject to GC                │   │  NOT subject to GC         │   │
│  └───────────────────────────────┘   └────────────────────────────┘   │
│                                                                       │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │       OVERHEAD MEMORY (spark.executor.memoryOverhead)          │   │
│  │       JVM/native library overhead, PySpark's separate          │   │
│  │       Python process memory (also technically "off-heap"       │   │
│  │       relative to the JVM, but tracked/configured separately)  │   │
│  └────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────┘

Total container memory the Cluster Manager must reserve ≈
  executor-memory (on-heap) + offHeap.size (if enabled) + memoryOverhead
```

**Important distinction:** `spark.memory.offHeap.size` (Spark-managed Tungsten off-heap) and `spark.executor.memoryOverhead` (general native/JVM/PySpark overhead) are **two separate configuration knobs** — both live "outside the JVM heap" conceptually, but serve very different purposes and are sized independently.

---

## 🔄 /flow — How a Task Uses Off-Heap Memory (When Enabled)

```
1. spark.memory.offHeap.enabled=true, spark.memory.offHeap.size=4g set at launch
2. Executor JVM starts; separately allocates the off-heap pool via sun.misc.Unsafe
3. A Task begins a shuffle/sort/aggregation operation:
     - Unified Memory Manager decides whether to serve this request
       from ON-heap or OFF-heap execution memory (based on config/availability)
4. If using off-heap: Tungsten allocates UnsafeRow buffers directly in
     the off-heap pool — no JVM object created, no GC visibility
5. Task completes: Spark explicitly frees the off-heap buffers it allocated
     (manual lifecycle management, NOT JVM garbage collection)
6. If a bug/leak causes off-heap memory to not be freed properly:
     the off-heap pool can fill up over time — invisible to on-heap
     GC/heap monitoring, requiring separate observability
```

---

## 💻 /examples — Enabling and Configuring Off-Heap Memory

```bash
spark-submit \
  --executor-memory 8g \                        # on-heap JVM size
  --conf spark.memory.offHeap.enabled=true \     # turn ON off-heap usage
  --conf spark.memory.offHeap.size=4g \          # off-heap pool size (separate!)
  --conf spark.executor.memoryOverhead=2g \      # separate again — general overhead
  my_job.py
```

```python
# Off-heap is transparent to your DataFrame code — no code changes needed
# once enabled; Tungsten decides how to use available on/off-heap space.

df = spark.read.parquet("large_dataset.parquet")
df.groupBy("customer_id").sum("amount").collect()
# Internally, shuffle/aggregation buffers MAY be allocated off-heap
# if spark.memory.offHeap.enabled=true — invisible at the API level

# Checking configured off-heap settings
print(spark.conf.get("spark.memory.offHeap.enabled"))
print(spark.conf.get("spark.memory.offHeap.size"))
```

```
# Off-heap usage isn't directly visible in the standard on-heap GC metrics —
# monitoring typically requires OS-level tools (e.g., checking total process
# RSS memory vs reported JVM heap usage) or JVM Native Memory Tracking (NMT):
#   -XX:NativeMemoryTracking=summary
```

---

## 🏭 /production — Real-World Use Cases

1. **Structured Streaming pipelines with strict micro-batch SLAs**: since GC pauses directly threaten a streaming job's ability to keep up with its trigger interval, teams enable off-heap memory to remove a meaningful source of latency variance from the processing path.
2. **Very large executor heaps where Full GC pauses are problematic**: rather than shrinking heap size (losing capacity) or accepting long Full GC pauses, teams move a substantial portion of memory off-heap, keeping the on-heap portion smaller and more GC-friendly while retaining total working memory.
3. **High-throughput shuffle-heavy ETL pipelines**: since Spark's shuffle network layer already leverages off-heap-style direct buffers for efficient network I/O, jobs with very large shuffle volumes benefit from enabling broader off-heap usage to reduce on-heap pressure from shuffle-related object allocation.
4. **PySpark pipelines using Pandas UDFs / Apache Arrow**: teams processing large volumes of data through vectorized Pandas UDFs rely on Arrow's off-heap columnar format for efficient JVM↔Python data exchange, avoiding the GC and serialization overhead of row-by-row conversion.
5. **Multi-tenant clusters with tight container memory limits**: platform teams carefully partition total container memory between on-heap, off-heap, and overhead allocations to maximize usable memory within a fixed container size, since off-heap memory can sometimes be used more space-efficiently than equivalent on-heap JVM object storage.

---

## 🚀 /optimization — Tuning Levers

| Lever | What it does |
|---|---|
| `spark.memory.offHeap.enabled` | Master switch to allow Spark to use off-heap memory for execution/storage (default: false) |
| `spark.memory.offHeap.size` | Explicit size of the off-heap pool — must be set deliberately; not automatically derived from `--executor-memory` |
| **Reduce `--executor-memory` while increasing off-heap size** | Shrinks the GC-managed heap (fewer/shorter GC pauses) while preserving total working memory via off-heap |
| `spark.executor.memoryOverhead` | Separate lever for general native/JVM/PySpark process overhead — don't conflate with `offHeap.size` |
| **Enable JVM Native Memory Tracking** for observability | Off-heap usage isn't visible in standard heap metrics; NMT (`-XX:NativeMemoryTracking`) provides visibility |
| **Use Arrow-based Pandas UDFs in PySpark** | Leverages off-heap columnar data exchange, reducing serialization overhead vs standard row-based UDFs |
| Monitor **container-level (OS) memory usage**, not just JVM heap usage | Off-heap leaks or misconfiguration show up here first, often before any JVM-level symptom appears |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Enabling `offHeap.enabled` without setting `offHeap.size`** — the feature has no effect without an explicit, non-trivial size configured; a common "I turned it on but nothing changed" confusion.
2. **Confusing `spark.memory.offHeap.size` with `spark.executor.memoryOverhead`** — they are separate pools serving different purposes; sizing one thinking it covers the other leads to unexpected container OOM kills.
3. **Not accounting for off-heap size in total container memory requests** — under-provisioning the container/pod's total memory limit relative to `on-heap + off-heap + overhead`, causing the cluster manager to kill the executor for exceeding its allocated container size.
4. **Assuming off-heap memory eliminates ALL memory problems** — it eliminates *GC-related* overhead for that specific memory, but doesn't prevent out-of-memory conditions if the off-heap pool itself is undersized for the actual working set.
5. **Not monitoring off-heap usage separately** — relying solely on standard JVM heap/GC dashboards, which are blind to off-heap memory entirely, and missing native memory leaks or pressure building up invisibly.
6. **Enabling off-heap for workloads that don't actually benefit** — small jobs or jobs with light GC pressure gain little from the added configuration complexity; it's most valuable for GC-sensitive, large-scale, or latency-critical workloads specifically.

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: What is off-heap memory in Spark?**
> A: A separately configured memory pool allocated outside the JVM heap (via `sun.misc.Unsafe`), used by Tungsten's execution/storage engine, that is completely invisible to and unaffected by Java garbage collection.

**Q2: What's the main benefit of off-heap memory?**
> A: It eliminates garbage collection overhead (scanning and stop-the-world pauses) for the data stored there — valuable for GC-sensitive, latency-critical, or very large-heap workloads.

**Q3: What's the main cost or risk of off-heap memory?**
> A: Spark must manually manage its allocation and deallocation (no automatic JVM cleanup); misconfiguration or bugs can cause native memory leaks that are invisible to standard JVM heap monitoring.

**Q4: How do you enable off-heap memory in Spark?**
> A: Set `spark.memory.offHeap.enabled=true` and explicitly configure `spark.memory.offHeap.size` — both are required; enabling the flag alone without a size has no effect.

**Q5: Is `spark.memory.offHeap.size` the same as `spark.executor.memoryOverhead`?**
> A: No — they're separate configuration knobs. `offHeap.size` is Spark-managed Tungsten memory for execution/storage; `memoryOverhead` covers general JVM/native/PySpark process overhead. Both are conceptually "outside the heap" but serve different purposes.

**Q6: Why is off-heap memory particularly valuable for streaming workloads?**
> A: Because GC pauses directly threaten a streaming job's ability to process micro-batches within its trigger interval; removing GC overhead for a portion of memory reduces latency variance.

**Q7: How would you monitor off-heap memory usage, since it's invisible to standard JVM heap dashboards?**
> A: Via OS-level process memory monitoring (e.g., total RSS vs reported JVM heap usage) or JVM Native Memory Tracking (`-XX:NativeMemoryTracking=summary`).

---

## ✅ /keytakeaways

- **Off-heap memory** is a Spark-managed pool allocated outside the JVM heap (via `sun.misc.Unsafe`), storing Tungsten's binary row data completely outside garbage collection's reach.
- It's **opt-in and complementary** to on-heap memory — requires both `spark.memory.offHeap.enabled=true` AND an explicit `spark.memory.offHeap.size`.
- The trade-off: **zero GC pause cost** for that memory, in exchange for **manual memory management** by Spark (not the JVM) — a different class of risk (native leaks) than typical heap issues.
- `spark.memory.offHeap.size` and `spark.executor.memoryOverhead` are **separate, independently-sized** configuration knobs — don't conflate them.
- Most valuable for **GC-sensitive workloads**: streaming pipelines with tight SLAs, very large executor heaps, and PySpark pipelines using Arrow-based Pandas UDFs.
- Off-heap usage requires **separate monitoring** (OS-level or JVM Native Memory Tracking) since it's invisible to standard JVM heap/GC dashboards.