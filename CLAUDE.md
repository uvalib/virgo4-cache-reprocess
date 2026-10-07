# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- `make` / `make darwin` — build macOS binary (with `-race`) to `bin/virgo4-cache-reprocess.darwin`
- `make linux` — static Linux build to `bin/virgo4-cache-reprocess.linux` (used by the Dockerfile)
- `make fmt`, `make vet` — gofmt / go vet on `cmd/virgo4-cache-reprocess`
- `make check` — installs and runs `staticcheck` (checks `all,-S1002,-ST1003`) and the `shadow` vet analyzer
- `make dep` — `go get -u`, `go mod tidy`, `go mod verify`
- `docker build -f package/Dockerfile .` — container build (run from repo root)

There are no tests in this repo.

## Architecture

Single `package main` Go service in `cmd/virgo4-cache-reprocess/`. It re-publishes previously cached Virgo4 records to an SQS queue so they can be reprocessed downstream.

Flow (`main.go`):
1. Long-poll the inbound SQS queue for an S3 event notification (`inbound.go`, structs in `inbound_struct.go`). Object keys are URL-unescaped; zero-length objects are skipped.
2. Download each referenced S3 object to a temp file in `DownloadDir`. Each file is a newline-delimited list of record IDs (`record_loader.go`).
3. **Validate** every file first: all IDs must exist in the Postgres cache table (`CacheProxy.Exists`, batched 500 at a time). If any file in the notification fails, the whole batch's local files are deleted and the inbound message is *not* deleted (so SQS will redeliver it).
4. On success, delete the inbound message, then stream IDs from each file into `inboundRecordsChan`.
5. `cache_worker` goroutines batch IDs (100 per `CacheProxy.Get`), fetch `id/type/source/payload` from Postgres, and build `awssqs.Message`s with record id/type/source attributes and operation=update (`cache_proxy.go`).
6. `send_worker` goroutines batch messages (`awssqs.MAX_SQS_BLOCK_COUNT`) and `BatchMessagePut` to the outbound queue, retrying partial failures.

Both worker types flush partial batches after `flushTimeout` (5s, in `main.go`) of inactivity. Cache lookups are filtered by `source IN (...)` using the space-separated `VIRGO4_CACHE_REPROCESS_DATA_SOURCE` value.

Error handling is fail-fast: most unexpected errors call `fatalIfError` (`helpers.go`) and exit the process, relying on the container being restarted. `ErrNotInCache` during validation is the one tolerated error.

## Configuration

All config comes from required environment variables loaded in `config.go` (`VIRGO4_CACHE_REPROCESS_*` plus `VIRGO4_SQS_MESSAGE_BUCKET` for large SQS message payloads). A missing/empty variable is fatal at startup. When adding a config value, add it to `ServiceConfig`, `LoadConfiguration`, and the `[CONFIG]` log lines.

Key dependencies are UVA libraries: `github.com/uvalib/virgo4-sqs-sdk/awssqs` (SQS + oversized-message S3 offload) and `github.com/uvalib/uva-aws-s3-sdk/uva-s3`. Postgres access uses `ozzo-dbx` with `lib/pq`.

## Build & Deploy

- `Version()` reads a `buildtag.*` file in the working directory (created by the Dockerfile from the `BUILD_TAG` build arg); locally it reports `unknown`.
- `pipeline/buildspec.yml` (AWS CodeBuild) builds and pushes the image to ECR and records the build tag in SSM.
- `pipeline/deployspec.yml` applies Terraform from `github.com/uvalib/terraform-infrastructure` for three staging ECS tasks that all run this same image with different config: `virgo4-sirsi-cache-reprocess`, `virgo4-hathi-cache-reprocess`, `virgo4-dynamic-cache-reprocess`.
