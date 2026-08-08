# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - Unreleased

### Added

- Storage-neutral `RangeSource` API and JDK-only `FileRangeSource`.
- Parallel processing of non-overlapping ranges using a caller-owned `Executor`.
- Single-byte delimiter framing with newline convenience configuration.
- Ordered, segmented reconstruction of records crossing any number of ranges.
- Integration with `inputstream-processor-core` 1.0.0.
- Java 8 and Java 25 CI verification.
