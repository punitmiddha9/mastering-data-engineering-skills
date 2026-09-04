# 🗑️ Delta VACUUM — From First Principles to Production

## 1. What Is `VACUUM`?

**`VACUUM`** is the Delta Lake command that **physically deletes data files that are no longer needed** — files that were marked "removed" by previous operations (UPDATE, DELETE, MERGE, `OPTIMIZE`) but are still sitting on disk.

### 🧠 Simple Analogy

Think of a Delta table like your **computer's Recycle Bin / Trash**.

- When you "delete" a file on your laptop, it doesn't vanish — it moves to Trash.
- You can still restore it for a while.
- Eventually you **"Empty Trash"** — and *then* it's gone for good, freeing up disk space.

```
Laptop analogy:     Delete file  →  Trash  →  Empty Trash (permanent)
Delta analogy:      DELETE/UPDATE →  "removed" in log  →  VACUUM (permanent)
```

`VACUUM` is literally "Empty Trash" for your Delta table. Until you run it, the old files are just **hidden from current reads**, not gone.

---

## 2. First Principles — Why Do Deleted Files Stick Around?

Recall from Delta's core design (see Time Travel deep dive): **Delta never mutates Parquet files.** Every write operation only:

1. **Adds** new Parquet files, and/or
2. **Marks** existing Parquet files as **removed** in the transaction log.

> The `remove` action is a *logical* delete — a metadata flag in `_delta_log`. The physical bytes on disk are untouched.

This is *intentional* — it's exactly what makes **Time Travel** possible. If Delta deleted files immediately, `VERSION AS OF` would break instantly. `VACUUM` is the deliberate, explicit step that trades away time-travel-ability for storage savings.

```
        WITHOUT VACUUM                      AFTER VACUUM
   (files kept for time travel)        (old files purged)

   disk: [f1] [f2] [f3] [f4]           disk: [f3] [f4]
   log:  f1 removed (v2)               log:  f1 removed (v2)
         f2 removed (v3)                     f2 removed (v3)
                                        (v0, v1 time-travel now BROKEN
                                         if they depended on f1/f2)
```

---

## 3. Architecture — What VACUUM Actually Touches

```
my_table/
│
├── _delta_log/
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json   ← "remove: f1.parquet" recorded here
│   ├── 00000000000000000002.json
│   └── ...
│
├── f1.parquet   ← marked "removed" in log, but PHYSICALLY still here
├── f2.parquet   ← currently ACTIVE (referenced by latest snapshot)
├── f3.parquet   ← currently ACTIVE
└── f4.parquet   ← orphan file (e.g., failed/aborted write, never committed)
```

`VACUUM` scans two things and computes a difference:

1. **All files physically present** on storage (a full directory listing).
2. **All files referenced** by the transaction log within the retention window (i.e., files needed by any version still "in scope").

```
   Files on disk           Files needed by log
   (physical listing)      (within retention window)
        │                          │
        └──────────► DIFF ◄────────┘
                       │
                       ▼
         Files eligible for deletion
      (not referenced + older than retention)
```

Anything physically present but **not referenced and older than the retention threshold** → deleted. This also cleans up:
- Files from **failed/aborted transactions** that never got committed (orphans).
- Files superseded by `OPTIMIZE` (small-file compaction leaves the old small files behind until VACUUM).

---

## 4. Behind the Scenes — Step-by-Step Execution

```
┌─────────────────────────────────────────────────────────┐
│  VACUUM my_table RETAIN 168 HOURS;                      │
└─────────────────────────────────────────────────────────┘
                       │
                       ▼
   1. Safety check: is retention < 168h (7 days)?
      → By default, Delta REFUSES this (safety guard)
      → Requires spark.databricks.delta.retentionDurationCheck.enabled = false
                       │
                       ▼
   2. List ALL files currently on disk (full directory scan)
                       │
                       ▼
   3. Determine the set of files referenced by:
      - the current table state, AND
      - every version still within the retention window
                       │
                       ▼
   4. Compute: files_to_delete = 
        (files on disk) - (files still referenced) 
        WHERE file.modificationTime < now - retention
                       │
                       ▼
   5. DRY RUN mode (optional): list candidates without deleting
      VACUUM my_table RETAIN 168 HOURS DRY RUN;
                       │
                       ▼
   6. Physically delete files_to_delete from storage
      (irreversible — no transaction log entry created for this!)
```

⚠️ **Critical internal detail:** the physical deletion in step 6 is **not** itself logged as a Delta transaction the way `add`/`remove` are. It's a storage-layer operation. This is precisely why it's irreversible — there's no log entry to "replay back."

---

## 5. Flow Diagram — The Full Lifecycle of a File

```
 WRITE          UPDATE/DELETE         VACUUM (after retention)
   │                  │                        │
   ▼                  ▼                        ▼
[created]  ──►  [marked "removed"]  ──►  [physically deleted]
   │                  │                        │
   active in       still on disk,          gone forever;
   current +      time-travel-able        time travel to any
   past versions   until retention          version needing
                   expires                  this file now fails
```

---

## 6. Examples

```sql
-- Standard cleanup (safe default: 7 days / 168 hours)
VACUUM my_table;

-- Explicit retention
VACUUM my_table RETAIN 168 HOURS;

-- Preview what WOULD be deleted (always do this first in prod)
VACUUM my_table RETAIN 168 HOURS DRY RUN;

-- Dangerous: aggressive cleanup, disables safety check
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM my_table RETAIN 0 HOURS;   -- deletes ALL unreferenced files immediately
```

```python
# PySpark / Delta Python API
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/path/my_table")
dt.vacuum()            # default 7-day retention
dt.vacuum(168)          # explicit hours
```

```sql
-- Check current retention settings on a table
DESCRIBE DETAIL my_table;

-- Set table-level retention policy
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = '7 days',
  'delta.logRetentionDuration' = '30 days'
);
```

---

## 7. Production Use Cases

| Use Case | How VACUUM Helps |
|---|---|
| **Storage cost control** | Tables with heavy UPDATE/DELETE/MERGE traffic (e.g., CDC/SCD Type-2 tables) accumulate huge numbers of orphaned files; scheduled VACUUM keeps cloud storage bills in check. |
| **Post-`OPTIMIZE` cleanup** | `OPTIMIZE` compacts small files into large ones but leaves old small files behind — VACUUM removes them. |
| **Cleaning failed job artifacts** | Aborted/failed Spark jobs can leave orphaned Parquet files never referenced by any commit — VACUUM sweeps these too. |
| **Compliance-driven purges** | Some data-retention regulations require **actual physical deletion**, not just logical deletion (e.g., "right to be forgotten" under GDPR) — VACUUM (with short retention) is the only way to truly purge bytes. |
| **CI/CD & test environments** | Ephemeral tables recreated often — aggressive `RETAIN 0 HOURS` VACUUM keeps test storage minimal (never do this in prod). |

---

## 8. Optimization

**Scheduling:**
- Run `VACUUM` on a **regular cadence** (e.g., nightly or weekly) as part of table maintenance jobs — don't let it pile up on high-churn tables.
- Run it **after** `OPTIMIZE`, not before — you want compaction's leftover small files included in the cleanup pass.

**Retention tuning:**
- Default 7 days balances "reasonable rollback window" vs. storage cost. Match it to your actual operational recovery SLA, not arbitrarily.
- Coordinate `deletedFileRetentionDuration` with any concurrent long-running readers/streaming jobs — a stream reading an old snapshot can break if files it needs get vacuumed mid-read.

**Performance:**
- On object stores (S3/ADLS/GCS), the file-listing step (step 2 above) can be slow on tables with millions of files — this is often the real bottleneck, not the deletion itself.
- Some platforms support **parallelized/optimized VACUUM** (e.g., Databricks' improved vacuum with faster listing) — check your platform's specific implementation for large tables.

**Always dry-run first:**
```sql
VACUUM my_table DRY RUN;  -- inspect the candidate list before committing
```

---

## 9. Common Mistakes (Real-World Pain Points)

1. **Running `VACUUM RETAIN 0 HOURS` in production** — instantly kills time-travel and can break concurrent long-running readers/streams mid-flight.
2. **Disabling the retention safety check as a habit** (`retentionDurationCheck.enabled = false`) instead of understanding *why* the 7-day guard exists — it's protecting against exactly this mistake.
3. **Vacuuming before a scheduled `RESTORE`/rollback plan is confirmed safe** — teams sometimes run cleanup jobs on a timer without checking if a rollback might still be needed.
4. **Forgetting concurrent streaming readers** — a structured streaming job reading an older Delta version can fail with `FileNotFoundException` if VACUUM removes files it still needs.
5. **Assuming VACUUM shrinks the transaction log** — it doesn't; that's controlled separately by `logRetentionDuration` / log checkpoint cleanup. VACUUM only removes *data* files.
6. **Not dry-running first** — in an unfamiliar or high-stakes table, skipping `DRY RUN` means no chance to sanity-check the blast radius before deletion.
7. **Running VACUUM too rarely on high-churn tables** — orphaned files silently balloon storage costs for months before anyone notices the bill.

---

## 10. Interview Questions & Short Answers

**Q1: What does `VACUUM` actually do in Delta Lake?**
> It physically deletes data files that are no longer referenced by the table's current state or by any version within the retention window — files that were only *logically* removed (marked in the transaction log) until this point.

**Q2: Why does Delta Lake keep "deleted" files around instead of removing them immediately?**
> To support Time Travel and rollback (`RESTORE`) — old files must persist so historical versions remain queryable until an explicit VACUUM purges them.

**Q3: What's the default retention period, and why does Delta prevent you from going lower?**
> 7 days (168 hours) by default. Delta blocks shorter retention via a safety check because aggressive VACUUM can silently break time travel, concurrent readers, and in-flight streaming jobs.

**Q4: Does VACUUM affect the transaction log?**
> No — VACUUM only deletes data (Parquet) files. Log retention/cleanup (`logRetentionDuration`) is a separate, independent setting.

**Q5: How would you safely test what VACUUM will delete before running it for real?**
> Use `VACUUM table_name DRY RUN` — it lists candidate files without deleting anything.

**Q6: What happens if a streaming job is reading an old version when VACUUM runs?**
> It can fail with a file-not-found error if the files it depends on get physically deleted — retention windows must account for the longest-running concurrent reader.

**Q7: Where do orphaned files come from besides UPDATE/DELETE?**
> Failed or aborted write transactions can leave Parquet files on disk that were never referenced by a committed log entry, and `OPTIMIZE` leaves old pre-compaction files behind — VACUUM cleans up both.

---

## 11. Key Takeaways

- 🗑️ `VACUUM` = **"Empty Trash"** — it physically deletes files that were only *logically* removed in the transaction log.
- 🔗 It exists because Delta keeps old files around specifically to support **Time Travel**; VACUUM is the deliberate trade-off point where you give that up for storage savings.
- 🧮 Internally, it's a **diff**: files on disk **minus** files still referenced within the retention window, filtered by age.
- ⚠️ The default 7-day safety guard exists for a reason — lowering retention (especially to 0) is a common, expensive production mistake.
- 🚫 VACUUM is **irreversible** and **not logged as a transaction** — always run `DRY RUN` first on unfamiliar or critical tables.
- 📅 Schedule it regularly, run it **after** `OPTIMIZE`, and coordinate retention with your longest-running concurrent readers/streams.