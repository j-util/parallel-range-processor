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

### Changed

- Reused framing state directly for complete in-range records, avoiding four
  framing allocations per ordinary record while retaining boundary fragments.
- Bypassed delimiter framing when range division produces one actual range.
- Submitted reconstructed boundary records as one final caller-executor task.
- Clarified boundary-record memory growth and stream-global parser metadata
  constraints.
- Removed unused boundary-shape framing state and added deterministic
  interruption coverage.
