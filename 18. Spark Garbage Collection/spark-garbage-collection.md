# Garbage Collection in Spark — The Complete Guide

## 🧠 /firstprinciples — What Is Garbage Collection, and Why Does Spark Care So Much?

Every Spark Executor is a **JVM process**. As tasks run, they constantly create objects — rows, intermediate buffers, hash map entries, closures. The JVM's **Garbage Collector (GC)** is the background system responsible for finding objects that are no longer referenced by any running code and **reclaiming their memory** so it can be reused.

This is automatic and mostly invisible — until it isn't. GC work isn't free: to safely reclaim memory, most GC algorithms need to (at least briefly) **pause application threads** so nothing changes underneath them while memory is being scanned and compacted. These are called **"stop-the-world" pauses**, and at Spark's scale — millions of objects created per second, heaps spanning tens of gigabytes — GC pauses become a **first-class performance concern**, not an obscure JVM internals detail.

> Garbage Collection in Spark is the JVM's automatic memory reclamation process running inside every Executor; because Spark workloads generate enormous numbers of objects at scale, GC behavior — pause frequency and duration — directly and measurably affects job throughput and latency.

**The one-sentence definition:**
> GC in Spark matters because Spark's entire performance model assumes executors are actively computing — every millisecond spent in a GC pause is a millisecond every task thread on that executor is frozen, doing nothing.

---

## 🚦 Simple Analogy

Imagine a **busy toll booth plaza** on a highway, processing cars (tasks) continuously.

- Periodically, the plaza needs to **do maintenance** — sweep away debris, count discarded toll tickets, clear space. This is garbage collection.
- A **quick, targeted sweep of just the entry lanes** (Minor GC on the Young Generation) barely slows traffic — cars queue for a second, then keep moving.
- But occasionally, the **entire plaza needs a full shutdown for deep cleaning** (Major/Full GC on the Old Generation) — every lane closes, every car stops, for a noticeably longer stretch, before traffic resumes.

If the plaza is well-designed (efficient object lifecycle, right-sized generations), quick sweeps happen often and deep cleanings happen rarely. If it's poorly designed (too many long-lived objects, undersized generations), **full shutdowns happen more often than they should** — and that's exactly what a GC-bound Spark job looks like from the outside: mysteriously "slow," with no obvious code-level cause.

---

## 👁️ /visualizelearning — Generational GC and Pause Types

```
JVM HEAP (on-heap memory)
┌──────────────────────────────────────────────────────────────────┐
│                     YOUNG GENERATION                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │
│  │   Eden      │  │ Survivor 0  │  │ Survivor 1  │               │
│  │ (new objects│  │             │  │             │               │
│  │  created    │  │             │  │             │               │
│  │  here)      │  │             │  │             │               │
│  └─────────────┘  └─────────────┘  └─────────────┘               │
│         │ most objects DIE here quickly (short-lived)            │
│         │ MINOR GC — fast, frequent, small pause                 │
│         ▼                                                        │
│  (survivors promoted after surviving enough Minor GC cycles)     │
└──────────────────────────┬───────────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                      OLD GENERATION                              │
│         (long-lived objects: cached data, broadcast vars,        │
│          objects that survived many young-gen collections)       │
│                                                                  │
│         MAJOR / FULL GC — slow, infrequent, LARGE pause          │
│         (this is the one that actually hurts job performance)    │
└──────────────────────────────────────────────────────────────────┘

Timeline view of GC impact on a task:
[Task running] [Minor GC: 20ms] [Task running] [Minor GC: 15ms]
[Task running] ... [FULL GC: 8 SECONDS ⚠️] [Task running]
                     └── every thread in this executor was FROZEN
```

---

## ⚙️ /internals — What Actually Happens Behind the Scenes

### The object lifecycle that drives GC pressure
1. A task processes a row → creates one or more Java objects (unless using Tungsten's binary `UnsafeRow` format).
2. New objects are allocated in the **Eden space** of the Young Generation.
3. When Eden fills up, a **Minor GC** runs: live objects are copied to a **Survivor space**; dead objects are simply left behind (their space reclaimed instantly — no explicit "delete" needed, since nothing referenced it).
4. Objects that survive **several** Minor GC cycles get **promoted** to the **Old Generation** (the JVM assumes if it's lived this long, it'll keep living).
5. The Old Generation only gets cleaned during a **Major/Full GC** — which, for most collectors, requires scanning (and often compacting) the *entire* Old Generation, hence the long pause.

### Why Spark workloads are especially GC-heavy (without Tungsten)
- RDD-based or non-columnar processing creates **enormous numbers of small, short-lived objects** per row (a tuple here, a boxed integer there, a case class instance) — flooding the Young Generation constantly.
- If object churn is high enough, **promotion happens faster than intended** (a phenomenon called "premature promotion"), filling the Old Generation with objects that should have died young — directly increasing Full GC frequency.
- **Project Tungsten's binary row format** (`UnsafeRow`) exists specifically to counteract this: representing rows as packed byte arrays means far fewer actual JVM objects are created in the first place, starving the GC of work rather than trying to make GC itself faster.

### Common GC algorithms in the JVM (relevant to Spark tuning)
| Collector | Behavior | When it's chosen |
|---|---|---|
| **Parallel GC** (older JVM default) | Stop-the-world for both minor and major GC, multi-threaded | Throughput-focused batch jobs where occasional long pauses are tolerable |
| **G1GC (Garbage-First)** | Divides heap into regions, prioritizes collecting regions with the most garbage first, aims for predictable pause targets | Default (and generally recommended) for most modern Spark executors, especially with large heaps |
| **ZGC / Shenandoah** (newer, low-latency collectors) | Designed for sub-millisecond pause times even on very large heaps, using concurrent collection techniques | Latency-critical Spark Structured Streaming jobs on modern JVMs (11+/17+), where even G1GC's pauses are too disruptive |

```
spark.executor.extraJavaOptions = "-XX:+UseG1GC -XX:MaxGCPauseMillis=200"
                    │
                    ▼
   G1GC attempts to keep EACH pause under ~200ms by choosing
   which heap regions to collect, rather than always doing a
   full sweep — trading some collection efficiency for predictability
```

### Reading GC signals in the Spark UI
- The **Tasks tab**'s "GC Time" column shows time spent in garbage collection **per task** — the single most direct, in-product signal of GC pressure.
- A task with **GC Time approaching or exceeding its total duration's 10-15%** is a strong signal that memory/GC tuning (not just "add more executors") is the right lever to pull.

---

## 🏗️ /architecture — Where GC Fits in the Executor's Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│                       EXECUTOR JVM PROCESS                          │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │                   ON-HEAP MEMORY                            │   │
│   │     (subject to GC — Young + Old Generation, as above)      │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                            │                                        │
│                            ▼                                        │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │       JVM GARBAGE COLLECTOR (Parallel / G1GC / ZGC)         │   │
│   │   runs concurrently with, or pauses, task execution         │   │
│   │   depending on collector and phase                          │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                            │                                        │
│                            ▼                                        │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │            OFF-HEAP MEMORY (Tungsten, if enabled)           │   │
│   │        completely OUTSIDE this GC's reach entirely          │   │
│   └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

GC is purely an **on-heap** concern — this is precisely why off-heap memory exists as an escape hatch for GC-sensitive workloads, and why Tungsten's binary format (which can live on either side) reduces GC pressure even when kept on-heap.

---

## 🔄 /flow — Diagnosing and Tuning GC, End to End

```
1. Job is slower than expected, or has inconsistent task durations
2. Open Spark UI → Stages → Tasks tab → check "GC Time" column
3. High GC Time relative to task duration? → GC IS a real contributor
4. Enable detailed GC logging for the next run:
     spark.executor.extraJavaOptions =
       "-Xlog:gc*:file=/tmp/gc.log:time,uptime:filecount=5,filesize=50M"
5. Inspect GC logs: how often do Minor GCs run? How long do Full GCs take?
   Is promotion happening faster than expected (young gen filling fast)?
6. Apply a targeted change:
     a. Prefer DataFrame/SQL API over RDDs (reduces object churn — Tungsten)
     b. Switch to G1GC (or tune G1GC's pause target) if not already using it
     c. Adjust Young Generation size (-Xmn) if premature promotion is evident
     d. Consider off-heap memory for the most GC-sensitive portion of the workload
     e. Reduce executor heap size (paradoxically) if Full GC pauses on a very
        large heap are the dominant cost, trading capacity for predictability
7. Re-run → compare GC Time and task duration metrics to confirm improvement
```

---

## 💻 /examples — Enabling GC Logging and Tuning

```bash
spark-submit \
  --executor-memory 16g \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Xlog:gc*:file=/tmp/executor-gc.log:time,uptime" \
  --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -Xlog:gc*:file=/tmp/driver-gc.log:time,uptime" \
  my_job.py
```

```python
# No code-level API for GC directly — it's purely a JVM/config-level concern.
# But you CAN observe its effect via task metrics:
# Spark UI → Stages → click a stage → Tasks table → "GC Time" column

# Comparing RDD-heavy vs DataFrame code's GC footprint (conceptually):
rdd_version = sc.textFile("data.txt").map(lambda l: l.split(","))  # heavy object churn
df_version = spark.read.csv("data.txt")                             # Tungsten binary rows
# Run both, compare GC Time in the Spark UI for an equivalent operation
```

```
# Example line from a G1GC log, annotated:
[12.345s][info][gc] GC(42) Pause Young (G1 Evacuation Pause) 512M->128M(1024M) 18.2ms
#                         ^type of pause      ^before  ^after  ^heap size  ^duration
# A healthy pattern: many small (10-30ms) young pauses, few/no Full GCs.
# A problem pattern: increasingly long pauses, or "Pause Full" entries appearing.
```

---

## 🏭 /production — Real-World Use Cases

1. **Streaming pipelines with strict micro-batch SLAs**: teams running Structured Streaming jobs actively tune GC (G1GC pause targets, or off-heap memory) because a single multi-second Full GC pause can directly cause a missed batch trigger, cascading into growing backlog.
2. **Migrating legacy RDD pipelines to DataFrames specifically for GC relief**: platform teams have measured significant GC Time reduction (and corresponding runtime improvement) purely from migrating object-heavy RDD transformation chains to the DataFrame API, with no other logic changes.
3. **Right-sizing executor heap size on large clusters**: teams running very large executors (32GB+) sometimes deliberately choose **more, smaller executors** instead, specifically to keep individual Full GC pause durations manageable, even at the cost of some per-executor overhead.
4. **Long-running Spark applications (notebooks, thrift servers)**: applications that stay alive for hours/days accumulate cached data and long-lived objects across many jobs; GC tuning (and periodic `.unpersist()` discipline) becomes essential to avoid gradually worsening Full GC frequency over the application's lifetime.
5. **Diagnosing "randomly slow" tasks in an otherwise-healthy cluster**: when task duration variance can't be explained by data skew or resource contention, GC Time is frequently the overlooked culprit, found only by actually checking that specific Tasks tab column.

---

## 🚀 /optimization — Tuning Levers

| Lever | What it does |
|---|---|
| **Prefer DataFrame/SQL API over RDDs** | Reduces raw object creation via Tungsten's binary format — the single highest-leverage GC fix available |
| **Switch to G1GC** (`-XX:+UseG1GC`) | Modern default, generally better pause predictability than the older Parallel GC for large heaps |
| `-XX:MaxGCPauseMillis` | Sets a target (not a hard guarantee) for G1GC's maximum pause duration, influencing its collection strategy |
| **Consider ZGC/Shenandoah** for extreme latency sensitivity | Sub-millisecond pause targets on modern JVMs, for the most GC-sensitive streaming workloads |
| **Enable off-heap memory** (`spark.memory.offHeap`) | Removes a portion of memory from GC's scope entirely |
| **Reduce executor heap size, add more executors** | Smaller heaps generally mean shorter (though possibly more frequent) GC pauses — a real trade-off, not a free win |
| **Enable and review GC logs** | The only way to move from "guessing" to actually confirming GC behavior and the impact of tuning changes |
| Avoid excessive caching of large, rarely-reused datasets | Reduces long-lived Old Generation pressure from unnecessary cached objects |

---

## ⚠️ /mistakes — Common Pitfalls

1. **Never checking GC Time at all** — treating every slow job as a data volume, skew, or shuffle problem, missing GC as a real and measurable contributor that's visible directly in the Spark UI.
2. **Assuming a bigger heap always helps** — a larger heap with the same object-heavy workload can make Full GC pauses *longer*, not shorter, since more heap means more to scan/compact when Full GC eventually runs.
3. **Sticking with default/older GC algorithms on large modern heaps** — not evaluating G1GC (or newer collectors) despite running on large executor memory sizes where pause predictability matters most.
4. **Using RDD-heavy code out of habit or legacy inertia** — missing the highest-leverage GC fix available (migrating to DataFrame/SQL API) in favor of low-level JVM flag tweaking that yields smaller gains.
5. **Tuning GC flags blindly without enabling GC logs first** — changing `-XX` flags based on generic advice without actually confirming, via logs or Spark UI metrics, whether the change helped this specific workload's actual object lifecycle pattern.
6. **Ignoring cumulative GC pressure in long-running applications** — not periodically unpersisting stale cached data in notebooks/streaming apps, letting Old Generation pressure (and Full GC frequency) creep up silently over the application's lifetime.

---

## 🎤 /interview — Common Questions & Short Answers

**Q1: Why does garbage collection matter so much for Spark performance?**
> A: Spark workloads create enormous numbers of objects at scale; GC pauses freeze all task threads on an executor during collection, so pause frequency and duration directly and measurably impact job throughput and latency.

**Q2: What's the difference between a Minor GC and a Major/Full GC?**
> A: Minor GC cleans the Young Generation (frequent, fast, small pauses); Major/Full GC cleans the Old Generation (infrequent, but much slower, larger pauses) — Full GC is the one that most noticeably hurts performance.

**Q3: How does Project Tungsten reduce GC pressure?**
> A: By representing row data as compact binary byte arrays (`UnsafeRow`) instead of many individual JVM objects, drastically reducing the number of objects the garbage collector has to track, scan, and potentially promote.

**Q4: What is "premature promotion" and why does it matter?**
> A: When short-lived objects survive enough Minor GC cycles to get promoted to the Old Generation before they should — often due to high object churn rates — filling the Old Generation faster and increasing Full GC frequency.

**Q5: Why might reducing executor heap size actually improve performance?**
> A: A smaller heap generally means shorter (though sometimes more frequent) GC pauses, since there's less memory to scan/compact per collection cycle — a real trade-off between per-executor capacity and GC pause predictability.

**Q6: How would you check whether GC is a significant factor in a slow Spark job?**
> A: Check the "GC Time" column in the Spark UI's Tasks tab for the relevant stage; a high ratio of GC Time to total task duration is a direct, measurable signal.

**Q7: What GC algorithm is generally recommended for modern Spark executors, and why?**
> A: G1GC, because it divides the heap into regions and prioritizes collecting the most garbage-dense regions first, generally offering more predictable pause times than the older Parallel GC, especially on large heaps.

---

## ✅ /keytakeaways

- **Garbage Collection** reclaims memory from objects no longer in use — but doing so requires **pausing** application threads, and Spark's massive object creation rate makes GC a first-class, measurable performance factor.
- **Minor GC** (Young Generation) is fast and frequent; **Major/Full GC** (Old Generation) is slow and infrequent — Full GC pauses are usually the real performance killer.
- **Project Tungsten's binary row format** reduces GC pressure at the source, by creating far fewer JVM objects in the first place — often the highest-leverage fix available.
- **G1GC** is the generally recommended collector for modern, large-heap Spark executors; **ZGC/Shenandoah** suit the most latency-critical streaming workloads.
- **Check "GC Time" in the Spark UI's Tasks tab** before assuming a slow job is purely a data/skew/shuffle problem.
- **Bigger heaps aren't automatically better** — they can increase Full GC pause duration; deliberate sizing and collector choice matter more than raw capacity.