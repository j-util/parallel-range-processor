# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2026-08-11

### Added

- Storage-neutral `RangeSource` API and JDK-only `FileRangeSource`.
- Parallel processing of non-overlapping ranges using a caller-owned `Executor`
  whose lifecycle remains with the caller.
- Concurrent `process(...)` calls on one processor instance, provided shared
  caller-supplied collaborators tolerate the resulting concurrent use.
- Independent parser creation for each actual range and, when needed, final
  boundary reconstruction.
- Single-byte delimiter framing with newline convenience configuration.
- Ordered, segmented reconstruction of records crossing any number of ranges.
- Integration with `inputstream-processor-core` 1.0.0.
- Configurable per-worker framing read-buffer size, with a 64 KiB default.
- A direct single-range processing path when range division produces one actual
  range.
- Deterministic failure propagation after all submitted tasks finish, including
  interruption handling without leaving consumer calls running after return.
