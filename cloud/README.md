## RocksDB Cloud Storage Addon

This directory contains extensions for RocksDB to sync SST files, MANIFEST,
and other database files to cloud object storage (AWS S3 and/or Google Cloud
Storage).

Originally derived from [Rockset's rocksdb-cloud](https://github.com/rockset/rocksdb-cloud),
based on RocksDB v9.1.1, and ported to the current RocksDB version. GCS support
adapted from [rockset/rocksdb-cloud PR #300](https://github.com/rockset/rocksdb-cloud/pull/300).

### Building

Cloud support is enabled with CMake. You can enable AWS, GCS, or both:

```bash
# AWS only
cmake -B build -DWITH_CLOUD=ON -DWITH_AWS=ON

# GCS only (requires google-cloud-cpp, see below)
cmake -B build -DWITH_CLOUD=ON -DWITH_GCS=ON

# Both providers
cmake -B build -DWITH_CLOUD=ON -DWITH_AWS=ON -DWITH_GCS=ON

cmake --build build
```

#### AWS SDK

On macOS, the AWS C++ SDK can be installed via Homebrew:

```bash
brew install aws-sdk-cpp
```

Alternatively, follow the build instructions at
https://github.com/aws/aws-sdk-cpp (version 1.7.325 or later).

#### Installing dependencies with vcpkg

Both SDKs can also be installed via [vcpkg](https://github.com/microsoft/vcpkg)
(this is the only option for the Google Cloud C++ SDK, which is not in Homebrew):

```bash
git clone https://github.com/microsoft/vcpkg.git ~/vcpkg
~/vcpkg/bootstrap-vcpkg.sh

# AWS SDK (s3 + transfer components)
~/vcpkg/vcpkg install 'aws-sdk-cpp[core,s3,transfer]'

# Google Cloud C++ SDK (storage component)
~/vcpkg/vcpkg install 'google-cloud-cpp[core,storage]'
```

Then pass the vcpkg toolchain to CMake:

```bash
cmake -B build \
  -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake \
  -DWITH_CLOUD=ON -DWITH_AWS=ON -DWITH_GCS=ON
```

Note: the first vcpkg install builds all dependencies from source, which can
take 10-40 minutes depending on your machine.

### Verifying Build Configurations

All of the following configurations should build and link cleanly. The provider
source files always compile (using `#ifdef` stubs when the SDK is absent), so
any combination works:

```bash
# No cloud (baseline)
cmake -B build-none -DWITH_CLOUD=OFF -DWITH_TESTS=ON
cmake --build build-none --target cloud_file_system_test

# Cloud framework only (no provider SDKs)
cmake -B build-cloud -DWITH_CLOUD=ON -DWITH_TESTS=ON
cmake --build build-cloud --target cloud_file_system_test

# Cloud + AWS
cmake -B build-aws -DWITH_CLOUD=ON -DWITH_AWS=ON -DWITH_TESTS=ON
cmake --build build-aws --target cloud_file_system_test

# Cloud + GCS (needs vcpkg toolchain)
cmake -B build-gcs -DWITH_CLOUD=ON -DWITH_GCS=ON -DWITH_TESTS=ON \
  -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build build-gcs --target cloud_file_system_test

# Cloud + AWS + GCS
cmake -B build-all -DWITH_CLOUD=ON -DWITH_AWS=ON -DWITH_GCS=ON -DWITH_TESTS=ON \
  -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build build-all --target cloud_file_system_test
```

The `cloud_file_system_test` target tests provider registration and factory
methods for all configurations (providers return `NotSupported` when their SDK
is not compiled in).

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

#### AWS credentials

- Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- Programmatically: `cloud_opts.credentials.InitializeSimple(key_id, secret)`
- AWS SDK default credential chain (instance roles, config files, etc.)

#### GCS credentials

GCS uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
(ADC). No explicit credential configuration is needed in code. Authenticate via:

```bash
gcloud auth application-default login
```

Or set `GOOGLE_APPLICATION_CREDENTIALS` to a service account key file path.

For custom client creation, set `cloud_opts.gcs_client_factory`.

See `include/rocksdb/cloud/cloud_file_system.h` for the full set of options.

### Running Tests

#### Unit tests (no credentials needed)

The `cloud_file_system_test` runs without any cloud credentials and tests
provider registration, factory methods, and configuration:

```bash
./build/cloud_file_system_test
```

#### AWS integration tests (requires credentials)

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-west-2
export ROCKSDB_CLOUD_TEST_BUCKET_PREFIX=myprefix.
export ROCKSDB_CLOUD_TEST_BUCKET_NAME=mytestbucket

./build/db_cloud_test
```

#### GCS integration tests with emulator (no credentials needed)

GCS integration tests can run locally using
[fake-gcs-server](https://github.com/fsouza/fake-gcs-server) in Docker:

```bash
# Start the emulator
docker run -d --name fake-gcs -p 4443:4443 \
  fsouza/fake-gcs-server -scheme http -port 4443

# Run the tests
CLOUD_STORAGE_EMULATOR_ENDPOINT=http://localhost:4443 \
GOOGLE_CLOUD_PROJECT=test-project \
ROCKSDB_CLOUD_TEST_BUCKET_PREFIX=rockset- \
ROCKSDB_CLOUD_TEST_BUCKET_NAME=gcpcloudtest \
./build/gcp_db_cloud_test

# Clean up
docker rm -f fake-gcs
```

Note: GCS does not support dots in bucket names. Use a prefix like `rockset-`
instead of `rockset.` when configuring test buckets.

#### GCS integration tests with real GCS (requires credentials)

```bash
# Authenticate
gcloud auth application-default login

# Set project and bucket
export GOOGLE_CLOUD_PROJECT=your-gcp-project
export ROCKSDB_CLOUD_TEST_BUCKET_PREFIX=myprefix-
export ROCKSDB_CLOUD_TEST_BUCKET_NAME=mytestbucket

./build/gcp_db_cloud_test
```

### Examples

See `cloud/examples/` for sample programs demonstrating cloud-backed databases.
