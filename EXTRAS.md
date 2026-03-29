# SurrealDB Extras for `rocksdb`

This document describes the changes in the `cloud/11.0.0` branch compared to
upstream `main` (`facebook/rocksdb`). These additions turn stock RocksDB into a
cloud-native storage engine suitable for SurrealDB's requirements.

---

## Cloud object storage addon

The foundational change: a full cloud storage layer that syncs SST files,
manifests, and WAL to S3-compatible object stores. Includes a pluggable
`CloudFileSystem` abstraction, `CloudStorageProvider` interface, and
`DBCloud` wrapper that transparently maps local file operations to remote
object storage.

Key components:
- `CloudFileSystem` / `CloudFileSystemImpl` — intercepts file I/O and
  routes SST and manifest operations to cloud storage
- `CloudStorageProvider` — abstraction for upload, download, list, delete,
  and existence checks against a remote bucket
- `CloudManifest` — epoch-based manifest tracking for cloud consistency
- `DBCloud` — thin wrapper over `DB` adding cloud-aware open, checkpoint,
  and savepoint operations
- WAL background sync to cloud storage
- Cloud-aware `ldb` tool commands

## Kafka and Kinesis WAL syncing removal

Removed the Kafka and Kinesis WAL replication backends that were part of the
original RocksDB-Cloud project. These are replaced by simpler cloud storage
WAL sync which better fits SurrealDB's architecture.

## Bucket and storage defaults

Adjusted default `CloudFileSystemOptions` to use sensible defaults for
bucket configuration, storage paths, and boolean flags, reducing the amount
of boilerplate needed to open a cloud-backed database.

## BlobDB value separation in object storage

Extended the cloud layer to handle BlobDB `.blob` files alongside SST files.
Blob files are now uploaded to and downloaded from object storage, enabling
value separation (large values stored in blob files) in cloud deployments.

## CloudTransactionDB and CloudOptimisticTransactionDB

Two new database types that combine cloud storage with RocksDB's transaction
APIs:
- `CloudTransactionDB` — pessimistic transactions over a cloud-backed DB
- `CloudOptimisticTransactionDB` — optimistic transactions over a cloud-backed DB

Both support the full transaction lifecycle (begin, commit, rollback) and
cloud operations (checkpoint, savepoint) through a unified API. Includes
full C binding coverage and test suites.

## C binding completeness

Comprehensive C bindings for all cloud types and operations:
- `CloudFileSystemOptions` — all configuration getters/setters
- `CloudBucketOptions` — bucket name, region, prefix, object path
- `CloudFileSystem` — creation, env wrapping
- `DBCloud`, `CloudTransactionDB`, `CloudOptimisticTransactionDB` — open,
  close, column family management, checkpoint, savepoint
- Cloud credential configuration (access key, config file, access type)

Also ensured all pre-existing RocksDB types referenced in the cloud layer
have proper C bindings.

## Object storage file purging

Reworked the cloud file purge logic to reliably delete obsolete SST and blob
files from object storage. The purger now correctly tracks which files are
still referenced by any live version and only deletes truly orphaned remote
objects.

## `DB::resume()` in C bindings

Exposed `DB::resume()` through the C API, allowing callers to resume database
operations after a background error (e.g. I/O failure) has been resolved.

## GCS (Google Cloud Storage) support

Added a second cloud storage backend targeting Google Cloud Storage alongside
the existing S3 backend:
- `GcpFileSystem` / `GcpStorageProvider` — upload, download, list, delete,
  and existence checks using the GCS JSON API
- Retry logic with exponential backoff for transient GCS errors
- Dedicated test suite exercising the full GCS code path
- Build system integration via CMake with `google-cloud-cpp` dependency

## Encryption key management

A pluggable encryption key management module for data-at-rest encryption:
- `KeyManager` interface — create, rotate, delete, and retrieve encryption
  keys identified by string key IDs
- `InMemoryKeyManager` — reference implementation for testing
- `EncryptedEnv` — environment wrapper that transparently encrypts/decrypts
  data using AES-CTR via the configured key manager
- Full C bindings and test coverage

## User-defined timestamps in optimistic transactions

Extended `OptimisticTransactionDB` to support RocksDB's user-defined
timestamp (UDT) feature. Optimistic transactions can now read and write with
explicit timestamps, with conflict detection correctly accounting for
timestamp ordering. Includes validation that timestamps are consistent
within a transaction and across the commit-time conflict check.

## Bounded local SST file cache with LRU eviction

A new `LocalSstCache` that keeps recently-accessed SST files on local disk
after they are uploaded to cloud storage:
- Configurable maximum cache size via `local_sst_cache_size`
- LRU eviction — least-recently-accessed files are deleted when the cache
  exceeds its size limit
- Avoids the write-upload-delete-redownload round-trip of
  `keep_local_sst_files=false` without the unbounded disk growth of
  `keep_local_sst_files=true`
- Evicted files remain in cloud storage and are transparently re-downloaded
  on demand

Also includes WAL cloud controller and Kafka WAL sync infrastructure as
supporting components.

## Fallback object paths for cloud SST resolution

Added an ordered list of fallback `BucketOptions` that are searched when an
SST file is not found in the primary destination or source buckets. This is
the storage primitive for zero-copy database branching: child branches can
read parent SST files from ancestor object paths without copying them.

## Fork point snapshot API

`CaptureForkPoint()` on `DBCloud`, `CloudTransactionDB`, and
`CloudOptimisticTransactionDB` captures a lightweight metadata-only snapshot
containing:
- Current `CloudManifest` epoch
- Next file number
- `CLOUDMANIFEST` cookie

This snapshot can be stored externally and used to create zero-copy database
branches that share the parent's SST files for all file numbers below the
fork point.

## Per-instance cloud bandwidth throttling

New `cloud_upload_rate_limiter` and `cloud_download_rate_limiter` options on
`CloudFileSystemOptions` that throttle S3/GCS bandwidth per database
instance using RocksDB's existing `RateLimiter` infrastructure. Both the S3
and GCS backends apply rate limiting to upload and download streams.

## Serverless cold start optimizations

Several optimizations to reduce cloud DB open latency:

- **Parallel manifest bootstrap:** `SanitizeLocalDirectory` now downloads
  IDENTITY and CLOUDMANIFEST concurrently on the cold-start (reinit) path,
  saving one full cloud round-trip.
- **`skip_cloud_listing_on_open`:** new `CloudFileSystemOptions` boolean
  that suppresses expensive `ListCloudObjects` calls during `DB::Open`.
  After open completes, `GetChildren` resumes normal cloud listing.
- **`warm_connection_pool_size`:** new `CloudFileSystemOptions` int that
  pre-establishes N TLS connections to the cloud storage endpoint during
  initialization, removing TLS handshake latency from the download path.
- **`initial_table_load_limit`:** new `DBOptions` int (default 16) that
  controls how many SST files have their metadata loaded during
  `DB::Open`. Set to 0 to load all tables, eliminating first-query
  latency spikes.

## Kafka WAL recovery on startup

Wired up the `KafkaWALTailer` so that WAL records published to Kafka are
consumed and written to the local DB directory during
`SanitizeLocalDirectory`, before `DB::Recover()` replays them. This ensures
unflushed data is not lost when a node restarts with Kafka WAL sync enabled.

The recovery runs after cloud WAL download and before the background WAL
uploader starts. It is a no-op when Kafka sync is not configured
(`kafka_wal_sync_mode == kNone`) or when RocksDB is built without Kafka
support.

## Incremental WAL cloud upload with delta mode

Added size-tracking to `BackgroundWALUploader` to skip re-uploading
unchanged WAL files, and an opt-in `use_wal_delta_upload` mode that uploads
only new bytes as separate delta objects rather than re-uploading the entire
file.

Key components:
- Per-file uploaded-size tracking — skips upload when local size matches
  the last uploaded size
- `UploadWALDelta()` — reads only new bytes from the WAL file and uploads
  them as `.delta.<offset>` objects in cloud storage
- Recovery reassembly — `RecoverWALFromCloud()` detects delta chunks,
  groups them by base WAL file name, sorts by offset, and concatenates
  them into complete local WAL files
- Cleanup of obsolete delta objects when the corresponding local WAL file
  is deleted
- New `use_wal_delta_upload` option on `CloudFileSystemOptions` with C API
  bindings
