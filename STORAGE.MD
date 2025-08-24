## This file describes the architecture of storage layer of this framework

### Postgres
Metastore table

Because i just want to learn how delta lake works, this schema will reflect what similar to the delta log. Meaning
each row reflect a modification/action of the system, and stored in an immutable/append only fashion, instead of keeping the latest snapshot of all active directory/parquet file locations.

They have good reasons for organizing it this way (The followings are listed by LLM when i asked it, let's consider if it make sense with our current architecture or not)
- Object stores aren’t databases 
=> Delta lake store data on remote files, not traditional ACID aware DB

- S3/ADLS/GCS don’t support multi-object transactions or atomic renames across many files 
=> Same as above

- Delta turns each commit into one atomic write: a new _delta_log/<version>.json (plus later a checkpoint). Multiple writers use optimistic concurrency; if two try the same version, one loses and retries.
=> "Writers" in the context of delta can be multiple thing, such as ETL frameworks, data streaming/batching frameworks such as Spark,Flink... In our project "writer" is something that write Postgres replication stream into managed parquet file, and commit the metadata into this delta table. Concurrency is also a problem here

No external two-phase commit is needed between “data files” and “metastore row”.
=>  

Snapshot isolation without downtime
=> This makes sense!

Readers pin a version; writers publish the next version. Reads never see partial updates and don’t block writes. With a mutable “latest row” you’d need locks or fancy MVCC in a DB.
=> Same as aboved

O(changed files) metadata cost

A commit records only the delta (adds/removes, stats). Updating a single “latest snapshot” row would still require reconstructing and rewriting large blobs of metadata under concurrency. The log lets you append tiny records.
=> This is true, let's say there is a DML command that needs to record update action on multiple parquet file (the update filter spans across multiple partition values, or even the whole data), in this case, concurrency can be a problem, as it may need to lock the a large portion of metastore. But how does "append only" approach improve this? 


Time travel, audit, and reproducibility (for free)

The log is a full history: query “as of version/timestamp”, diff two versions, rebuild any past state, explain where data came from. A single latest row throws that away or forces a separate audit trail.

Streaming + batch unification

The same log powers exactly-once streaming (the txn actions) and batch reads. Sinks can “tail” new versions; sources can resume from a known version.

Robust recovery

If a writer fails mid-commit, no new version appears → table is consistent. If metadata gets corrupted, you can replay from the last good checkpoint + actions to rebuild state.

Engine- and cloud-agnostic

All compute engines understand a filesystem + JSON/Parquet. No dependency on a specific RDBMS, no single point of failure, no cross-region DB replication to keep tables queryable.

Still fast to read: checkpoints

Delta does store a latest-ish snapshot—as periodic Parquet checkpoints of the log. Readers load the latest checkpoint, then apply only the newer actions. You get fast startup and full history.

Why not “just a Postgres metastore”?

You’d need distributed transactions to keep Postgres state in lockstep with object-store file mutations. That’s slow, fragile, and introduces a central bottleneck + SPOF.

It weakens portability (every engine must talk to your DB) and complicates zero-downtime reads/writes without MVCC gymnastics.

Rule of thumb

Event-sourced log + periodic checkpoints is the right fit for lake-stored tables: cheap to write, easy to scale, strongly consistent at version boundaries, and rich in lineage/history—things a single “latest row” can’t deliver.
