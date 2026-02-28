## RocksDB Cloud Storage Addon

This directory contains extensions for RocksDB to sync SST files, MANIFEST,
and other database files to cloud object storage (currently AWS S3).

Originally derived from [Rockset's rocksdb-cloud](https://github.com/rockset/rocksdb-cloud),
based on RocksDB v9.1.1, and ported to the current RocksDB version.

### Building

Enable cloud support with CMake:

```
cmake -DWITH_CLOUD=ON -DWITH_AWS=ON ..
cmake --build .
```

This requires the AWS C++ SDK to be installed. Follow the instructions at
https://github.com/aws/aws-sdk-cpp to install it (version 1.7.325 or later).

### Configuration

Cloud storage is configured through `CloudFileSystemOptions`. At minimum you
need to set the source and destination buckets:

```cpp
CloudFileSystemOptions cloud_opts;
cloud_opts.src_bucket.SetBucketName("mybucket");
cloud_opts.src_bucket.SetObjectPath("db/prod");
cloud_opts.src_bucket.SetRegion("us-west-2");
cloud_opts.dest_bucket.SetBucketName("mybucket");
cloud_opts.dest_bucket.SetObjectPath("db/prod");
cloud_opts.dest_bucket.SetRegion("us-west-2");
```

AWS credentials can be provided via:
- Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- Programmatically: `cloud_opts.credentials.InitializeSimple(key_id, secret)`
- AWS SDK default credential chain (instance roles, config files, etc.)

See `include/rocksdb/cloud/cloud_file_system.h` for the full set of options.

### Running Tests

Cloud tests require AWS credentials and a writable S3 bucket. Set:

```
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-west-2
```

### Examples

See `cloud/examples/` for sample programs demonstrating cloud-backed databases.
