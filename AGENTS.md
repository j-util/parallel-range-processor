# AGENTS.md

## Repository purpose

This repository publishes `io.github.j-util:parallel-range-processor`, a small
Java 8 library that orchestrates concurrent delimiter-framed processing over a
known-size, range-addressable byte source.

## j-util library standards

- Preserve Java 8 compatibility and use `maven.compiler.release=8`.
- Use `./mvnw verify` as the primary local quality gate.
- Keep public APIs minimal and fully documented.
- Add tests for every behavior or regression change; update `README.md` and
  `CHANGELOG.md` with user-visible changes.
- Preserve Maven Central metadata, Apache-2.0 licensing, and attached source and
  Javadoc artifacts.
- Never publish, sign, tag, or create a release unless explicitly requested.

## Library boundaries

- Keep storage adapters out of the core except for the JDK-only local-file
  implementation.
- Never create or shut down executors or thread pools.
- Never read outside an assigned half-open range.
- Retain only framing buffers and boundary fragments, not the complete source.
- Keep delimiter framing independent from parser results and counts.
- Compose with `inputstream-processor-core`; never copy or reimplement it.
- Preserve original worker failures and wait for submitted work to finish before
  returning, including on failure.
