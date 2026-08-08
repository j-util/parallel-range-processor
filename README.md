# Parallel Range Processor

[![CI](https://github.com/j-util/parallel-range-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/j-util/parallel-range-processor/actions/workflows/ci.yml)

A small Java 8 library for concurrently processing delimiter-framed records
from one large, range-addressable byte source while normally retaining only
reusable framing buffers and range-boundary fragments. It is an orchestration
layer over
[`inputstream-processor-core`](https://github.com/j-util/inputstream-processor-core),
which remains responsible for parsing and consumer-call counting.

## Requirements and installation

Parallel Range Processor requires Java 8 or later.

```xml
<dependency>
    <groupId>io.github.j-util</groupId>
    <artifactId>parallel-range-processor</artifactId>
    <version>1.0.0</version>
</dependency>
```

The artifact has one compile-time dependency:
`io.github.j-util:inputstream-processor-core:1.0.0`. There are no storage SDK or
concurrency-framework dependencies.

## Why a range source is required

An ordinary `InputStream` has one cursor. Splitting byte offsets does not by
itself establish record boundaries: a range can start or end in the middle of a
record. `RangeSource` adds the two capabilities needed by this algorithm:

- a known total byte size; and
- independent streams for exact half-open ranges such as `[1000, 2000)`.

The source must remain logically stable for one processing operation. Each
`openRange` caller owns and closes the returned stream. The processor closes all
streams it opens and also enforces the requested upper bound locally.

Unknown-size sources are outside V1 because they require a different scheduling
strategy based on chunk creation and EOF discovery.

## Processing model

```text
RangeSource
    |
    +-- [0, a) ---- worker parser --+
    +-- [a, b) ---- worker parser --+--> concurrent consumer calls
    +-- [b, S) ---- worker parser --+
             |
             +-- ordered boundary fragments
                         |
                         +--> final core parser --> consumer
```

For source size `S` and requested parallelism `N`, the processor creates up to
`N` contiguous, non-empty ranges that cover `[0, S)` exactly once. Ranges never
overlap, workers never read outside their assigned ranges, and there is one
top-level task per actual range—not one task per record.

If division produces exactly one actual range, the processor opens `[0, S)` and
submits one direct `InputStreamProcessor` task. Delimiter framing, fragment
retention, and boundary reconstruction are bypassed because no range boundary
exists.

Within a multi-range worker, a segmented framing stream copies the current
record into reusable accumulator storage until it encounters the configured
delimiter. Complete records are read directly from that reusable state;
persistent fragment snapshots are created only for retained boundaries. The
framing stream:

1. retains an ambiguous leading fragment for every range except the first;
2. exposes complete delimiter-terminated records to that worker's independent
   `InputStreamProcessor`;
3. retains an ambiguous trailing fragment for every range except the last; and
4. treats bytes at source EOF as a complete final record even without a trailing
   delimiter.

An intermediate range with no delimiter is represented as one middle fragment,
not inferred from parser results or consumer-call counts. This is what permits a
single record to span three, four, or more ranges. After all workers complete,
only boundary fragments are concatenated in source/range order and processed by
one additional core parser in one final task submitted to the caller-owned
executor. Fragments are streamed from fixed-size segments; the reconstructed
record is not first copied into one giant contiguous `byte[]`.

Complete records are processed during the parallel phase and are not retained
after the parser consumes them. Temporary memory is proportional to reusable
per-worker framing buffers plus retained boundary-spanning record data. In the
pathological case where one logical record spans essentially the entire source,
the retained boundary data can approach the source size.

## Local-file example

```java
import io.github.jutil.inputstreamprocessor.core.InputParser;
import io.github.jutil.parallelrangeprocessor.FileRangeSource;
import io.github.jutil.parallelrangeprocessor.ParallelProcessingResult;
import io.github.jutil.parallelrangeprocessor.ParallelRangeProcessor;
import io.github.jutil.parallelrangeprocessor.RecordDelimiter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

Supplier<InputParser<String>> lineParserFactory = () -> (input, emit) -> {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, StandardCharsets.UTF_8)
        );
        String line;
        while ((line = reader.readLine()) != null) {
            emit.accept(line);
        }
        // Do not close reader: the processor owns the underlying range stream.
    };

ExecutorService executor = Executors.newFixedThreadPool(4);
Queue<String> records = new ConcurrentLinkedQueue<>();
try {
    ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
            4,
            executor,
            lineParserFactory,
            RecordDelimiter.newline()
    );

    ParallelProcessingResult result = processor.process(
            new FileRangeSource(Paths.get("records.ndjson")),
            records::add
    );
    System.out.println("Processed items: " + result.getProcessedCount());
} finally {
    // The application owns executor lifecycle; the library never shuts it down.
    executor.shutdown();
}
```

The parser factory is called once per actual range and, when boundary data
exists, once for final reconstruction. Every returned parser must be non-null,
independently usable, synchronous as required by `inputstream-processor-core`,
and must consume its supplied complete-record stream through EOF. A parser that
returns while complete record bytes remain causes processing to fail rather
than silently losing records.

Every worker parser receives an independent subsequence of complete records,
not a replay of the source beginning. A parser therefore cannot independently
discover stream-global initialization such as a CSV header, preamble, schema,
or other first-record metadata in every worker. Configure such metadata
externally, share it immutably among parser instances, or handle its discovery
outside this V1 orchestration model.

## Concurrency and ordering

- The caller supplies explicit parallelism and an `Executor`. Parallelism is
  never inferred from the executor.
- The library creates no threads or pools and never shuts down the executor.
- The same consumer can be called concurrently by several worker threads. For
  effective parallelism greater than one, the consumer must be thread-safe or
  otherwise tolerate concurrent invocation. Calls are not synchronized by the
  library.
- Global consumer invocation order is unspecified. Complete in-range records
  can be consumed in any worker completion order.
- Boundary reconstruction order is deterministic source order. Reconstructed
  records are consumed only after the parallel worker phase, in one final task
  submitted to the same executor.
- A request for parallelism greater than the byte size creates fewer ranges;
  zero-length worker ranges are never submitted.

## Framing semantics and limitations

V1 recognizes one byte delimiter. `RecordDelimiter.newline()` selects line feed
(`0x0A`); `RecordDelimiter.singleByte(...)` supports another independently
recognizable byte. Delimiter bytes are preserved in parser input. With a normal
line parser, consecutive line feeds represent empty records, while a final
trailing line feed does not invent an extra record after EOF.

This layer establishes byte boundaries; it is not a CSV, JSON, or XML parser.
It is suitable only when every separator byte unambiguously terminates a logical
record. Unsupported inputs include:

- CSV with multiline quoted fields;
- JSON arrays or other stateful structured documents;
- XML;
- formats in which delimiter bytes can occur inside a record without locally
  detectable framing state;
- multi-byte delimiters in V1; and
- non-splittable compressed content such as a normal single gzip stream.

CRLF text can use the line-feed delimiter: the carriage return remains in the
original bytes immediately before the line feed, and a standard line reader
handles it normally. This library makes no claim of support for arbitrary
structured or compressed formats.

## S3 and HTTP adapters

The core stays storage-neutral. A custom `RangeSource` for S3 or HTTP should:

1. obtain and return a stable content length from `size()`;
2. translate `[fromInclusive, toExclusive)` into an inclusive wire range such
   as `Range: bytes=fromInclusive-(toExclusive - 1)`;
3. require a partial-content response when appropriate and verify the returned
   range and length;
4. return an independent response-body stream for every call; and
5. keep the same object/version or validator stable for the complete processing
   operation.

SDK clients, credentials, retries, consistency policy, and HTTP response
validation belong in the application adapter or a separate adapter artifact,
not in this library.

## Results and failures

`ParallelProcessingResult.getProcessedCount()` sums the
`inputstream-processor-core` counts from all normally completed worker streams
and the final boundary stream. It counts parser-emitted items whose consumer
calls returned normally; it is never used to infer record framing.

Source, parser, consumer, and executor failures do not disappear. Submitted
tasks are awaited before `process` returns or throws, so no worker is left
calling the consumer after the operation has reported completion. A generic
`Executor` has no reliable cancellation API, so already submitted tasks are
allowed to finish. If several workers fail, the original failure from the
lowest source-range ordinal is propagated regardless of completion order.

The final boundary task is submitted only after every range worker completes
normally. A final parser-factory or executor-submission failure is propagated
unchanged; rejected boundary work is not run. Once the final task is accepted,
its parser or consumer failure is propagated unchanged after that task finishes.

If the waiting thread is interrupted, the processor still waits for submitted
tasks and then throws `InterruptedException`; a selected task failure is
attached as a suppressed exception. An interruption during the range phase
prevents final-boundary submission. Completed consumer side effects are not
rolled back, and no transactional guarantee is made. No result is returned
after any failure.
