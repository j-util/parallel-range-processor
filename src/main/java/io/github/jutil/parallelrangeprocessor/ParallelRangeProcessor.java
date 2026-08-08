package io.github.jutil.parallelrangeprocessor;

import io.github.jutil.inputstreamprocessor.core.InputParser;
import io.github.jutil.inputstreamprocessor.core.InputStreamProcessor;
import io.github.jutil.inputstreamprocessor.core.ProcessingResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Processes delimiter-framed records concurrently across non-overlapping byte
 * ranges.
 *
 * <p>The processor divides a known-size {@link RangeSource} into up to the
 * configured number of contiguous, non-empty, half-open ranges. Each worker
 * scans and reads only its assigned range. Complete records wholly classifiable
 * inside a range are passed to an independent
 * {@link InputStreamProcessor} during the worker phase. Only boundary fragments
 * are retained. After all workers finish normally, those fragments are joined
 * in deterministic source order and passed to one additional processor in a
 * final task submitted to the same caller-owned executor. When range division
 * produces one actual range, framing is unnecessary and one executor task
 * passes the complete bounded source directly to its processor.</p>
 *
 * <p>Parser input preserves the original bytes, including every delimiter byte.
 * A final record at source EOF is processed even without a trailing delimiter.
 * Consecutive delimiters therefore remain consecutive in parser input; whether
 * they emit empty items is defined by the supplied parser.</p>
 *
 * <p>The supplied consumer can be invoked concurrently by several executor
 * workers. When effective parallelism is greater than one, it must tolerate
 * concurrent invocation. Consumer calls are deliberately not synchronized.
 * Global consumer invocation order is unspecified. Only reconstruction of
 * retained boundary fragments is source-ordered.</p>
 *
 * <p>This class creates no threads, owns no thread pool, and never shuts down or
 * otherwise manages the supplied executor. Because the generic {@link Executor}
 * contract has no cancellation facility, all successfully submitted tasks are
 * allowed to finish and are awaited before this method returns or throws.
 * If multiple workers fail, the failure belonging to the lowest source-range
 * ordinal is propagated, regardless of worker completion order.</p>
 *
 * <p>Source, parser, and consumer exceptions are never interpreted as framing
 * state. Successful consumer side effects are not rolled back. On any failure,
 * no {@link ParallelProcessingResult} is returned. The original selected
 * {@link IOException}, runtime exception, or error is propagated unchanged.</p>
 *
 * <p>Additional memory is proportional to reusable per-worker record-framing
 * buffers and retained boundary-spanning record data. A record spanning ranges
 * necessarily requires temporary storage proportional to that record, split
 * into fixed-size segments; it is streamed to the parser without first being
 * copied into one contiguous byte array. In the pathological case where one
 * logical record spans essentially the complete source, retained data can
 * approach the source size.</p>
 *
 * @param <T> the parser-emitted item type
 */
public final class ParallelRangeProcessor<T> {

    private static final int DEFAULT_READ_BUFFER_SIZE = 64 * 1024;

    private final int parallelism;
    private final Executor executor;
    private final Supplier<? extends InputParser<T>> parserFactory;
    private final RecordDelimiter delimiter;
    private final int readBufferSize;

    /**
     * Creates a processor with explicit concurrency and framing configuration,
     * using a 64 KiB framing read buffer for each worker.
     *
     * <p>The parser factory is invoked once per actual non-empty range before
     * worker submission and once more after the worker phase if reconstructed
     * boundary data exists. Each invocation must return a non-null parser that
     * is independently usable from parsers returned by other invocations. Range
     * parsers receive independent subsequences of complete records; they cannot
     * each discover stream-global metadata such as a header, preamble, or schema
     * from the source's first record. Such metadata must be supplied externally
     * or shared immutably. Each parser must consume its supplied stream through
     * end-of-stream before returning; returning while complete record bytes
     * remain is treated as a parser contract failure.</p>
     *
     * @param parallelism the maximum number of source ranges and worker tasks
     * @param executor the caller-owned executor used to run range and final
     *        boundary tasks
     * @param parserFactory factory for independent core parser instances
     * @param delimiter the single-byte record framing delimiter
     * @throws IllegalArgumentException if {@code parallelism} is not positive
     * @throws NullPointerException if any other argument is {@code null}
     */
    public ParallelRangeProcessor(
            int parallelism,
            Executor executor,
            Supplier<? extends InputParser<T>> parserFactory,
            RecordDelimiter delimiter
    ) {
        this(
                parallelism,
                executor,
                parserFactory,
                delimiter,
                DEFAULT_READ_BUFFER_SIZE
        );
    }

    /**
     * Creates a processor with explicit concurrency and framing configuration.
     *
     * <p>The parser factory is invoked once per actual non-empty range before
     * worker submission and once more after the worker phase if reconstructed
     * boundary data exists. Each invocation must return a non-null parser that
     * is independently usable from parsers returned by other invocations. Range
     * parsers receive independent subsequences of complete records; they cannot
     * each discover stream-global metadata such as a header, preamble, or schema
     * from the source's first record. Such metadata must be supplied externally
     * or shared immutably. Each parser must consume its supplied stream through
     * end-of-stream before returning; returning while complete record bytes
     * remain is treated as a parser contract failure.</p>
     *
     * <p>Each worker has its own framing read buffer of {@code readBufferSize}
     * bytes. This setting does not change the fixed-size segmentation used for
     * retained boundary fragments.</p>
     *
     * @param parallelism the maximum number of source ranges and worker tasks
     * @param executor the caller-owned executor used to run range and final
     *        boundary tasks
     * @param parserFactory factory for independent core parser instances
     * @param delimiter the single-byte record framing delimiter
     * @param readBufferSize the positive per-worker framing read-buffer size in
     *        bytes
     * @throws IllegalArgumentException if {@code parallelism} or
     *         {@code readBufferSize} is not positive
     * @throws NullPointerException if any other argument is {@code null}
     */
    public ParallelRangeProcessor(
            int parallelism,
            Executor executor,
            Supplier<? extends InputParser<T>> parserFactory,
            RecordDelimiter delimiter,
            int readBufferSize
    ) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be positive");
        }
        if (readBufferSize <= 0) {
            throw new IllegalArgumentException("readBufferSize must be positive");
        }
        this.parallelism = parallelism;
        this.executor = Objects.requireNonNull(executor, "executor");
        this.parserFactory = Objects.requireNonNull(parserFactory, "parserFactory");
        this.delimiter = Objects.requireNonNull(delimiter, "delimiter");
        this.readBufferSize = readBufferSize;
    }

    /**
     * Processes all delimiter-framed source records.
     *
     * <p>The source size is read once. An empty source opens no ranges, creates
     * no parser, invokes no consumer, and returns a zero count. Otherwise, the
     * actual range count is {@code min(parallelism, source size in bytes)}, so
     * no zero-length worker ranges are created.</p>
     *
     * <p>If range-worker submission fails, no later ranges or final boundary
     * task are submitted. Already submitted workers are still awaited. The
     * final boundary task is submitted only after all range workers finish
     * normally; a final parser-factory, submission, parser, or consumer failure
     * is propagated unchanged.</p>
     *
     * <p>If the waiting thread is interrupted, this method continues waiting
     * until already submitted tasks have finished and then throws
     * {@link InterruptedException}; any selected task failure is attached as a
     * suppressed exception. Interruption during the range-worker phase prevents
     * final-boundary submission.</p>
     *
     * @param source the stable, range-addressable source to process
     * @param consumer the potentially concurrently invoked item consumer
     * @return the successful consumer-call count, only after normal completion
     * @throws IOException if source access or a parser propagates an I/O failure
     * @throws InterruptedException if interrupted while awaiting submitted
     *         tasks, after all submitted tasks have finished
     * @throws RuntimeException if the executor, parser factory, parser, consumer,
     *         or count accumulation propagates a runtime failure
     * @throws NullPointerException if {@code source} or {@code consumer} is null,
     *         or if the parser factory or source returns null
     */
    public ParallelProcessingResult process(
            RangeSource source,
            Consumer<? super T> consumer
    ) throws IOException, InterruptedException {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(consumer, "consumer");

        long sourceSize = source.size();
        if (sourceSize < 0L) {
            throw new IOException("RangeSource returned a negative size: " + sourceSize);
        }
        if (sourceSize == 0L) {
            return new ParallelProcessingResult(0L);
        }

        List<SourceRange> ranges = divideRanges(sourceSize, parallelism);
        if (ranges.size() == 1) {
            return processSingleActualRange(source, ranges.get(0), consumer);
        }

        List<InputStreamProcessor<T>> processors = createProcessors(ranges.size());
        List<CompletableFuture<TaskOutcome>> submitted = new ArrayList<>(ranges.size());

        Throwable submissionFailure = null;
        for (int index = 0; index < ranges.size(); index++) {
            SourceRange range = ranges.get(index);
            InputStreamProcessor<T> processor = processors.get(index);
            CompletableFuture<TaskOutcome> completion = new CompletableFuture<>();
            try {
                executor.execute(() -> runWorker(
                        completion,
                        source,
                        range,
                        processor,
                        consumer
                ));
                submitted.add(completion);
            } catch (RuntimeException | Error failure) {
                submissionFailure = failure;
                break;
            }
        }

        AwaitedTasks awaited = awaitTasks(submitted);
        throwIfFailed(awaited, submissionFailure);

        long processedCount = 0L;
        List<ByteFragments> boundaryFragments = new ArrayList<>();
        for (TaskOutcome outcome : awaited.outcomes) {
            processedCount = Math.addExact(processedCount, outcome.result.processedCount);
            boundaryFragments.addAll(outcome.result.boundaryFragments);
        }

        if (!boundaryFragments.isEmpty()) {
            processedCount = Math.addExact(
                    processedCount,
                    processBoundaryFragments(boundaryFragments, consumer)
            );
        }

        return new ParallelProcessingResult(processedCount);
    }

    private ParallelProcessingResult processSingleActualRange(
            RangeSource source,
            SourceRange range,
        Consumer<? super T> consumer
    ) throws IOException, InterruptedException {
        InputStreamProcessor<T> processor = createProcessor();
        CompletableFuture<TaskOutcome> completion = new CompletableFuture<>();
        executor.execute(() -> runSingleRangeTask(
                completion,
                source,
                range,
                processor,
                consumer
        ));

        AwaitedTasks awaited = awaitTasks(Collections.singletonList(completion));
        throwIfFailed(awaited, null);
        return new ParallelProcessingResult(awaited.outcomes.get(0).result.processedCount);
    }

    private long processBoundaryFragments(
            List<ByteFragments> boundaryFragments,
            Consumer<? super T> consumer
    ) throws IOException, InterruptedException {
        InputStreamProcessor<T> processor = createProcessor();
        CompletableFuture<TaskOutcome> completion = new CompletableFuture<>();
        executor.execute(() -> runBoundaryTask(
                completion,
                boundaryFragments,
                processor,
                consumer
        ));

        AwaitedTasks awaited = awaitTasks(Collections.singletonList(completion));
        throwIfFailed(awaited, null);
        return awaited.outcomes.get(0).result.processedCount;
    }

    private List<InputStreamProcessor<T>> createProcessors(int count) {
        List<InputStreamProcessor<T>> processors = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            processors.add(createProcessor());
        }
        return processors;
    }

    private InputStreamProcessor<T> createProcessor() {
        InputParser<T> parser = Objects.requireNonNull(
                parserFactory.get(),
                "parserFactory returned null"
        );
        return new InputStreamProcessor<>(parser);
    }

    private void runWorker(
            CompletableFuture<TaskOutcome> completion,
            RangeSource source,
            SourceRange range,
            InputStreamProcessor<T> processor,
            Consumer<? super T> consumer
    ) {
        try {
            long count = 0L;
            List<ByteFragments> boundaryFragments;
            try (FramedRangeInputStream input = new FramedRangeInputStream(
                    openBounded(source, range.fromInclusive, range.toExclusive),
                    range.fromInclusive,
                    range.toExclusive,
                    range.first,
                    range.last,
                    delimiter.value(),
                    readBufferSize
            )) {
                if (input.prepare()) {
                    count = processor.process(input, consumer).getProcessedCount();
                    requireExhausted(input);
                }
                boundaryFragments = input.boundaryFragments();
            }
            completion.complete(TaskOutcome.success(
                    new TaskResult(count, boundaryFragments)
            ));
        } catch (Throwable failure) {
            completion.complete(TaskOutcome.failure(failure));
        }
    }

    private void runSingleRangeTask(
            CompletableFuture<TaskOutcome> completion,
            RangeSource source,
            SourceRange range,
            InputStreamProcessor<T> processor,
            Consumer<? super T> consumer
    ) {
        try (InputStream input = openBounded(
                source,
                range.fromInclusive,
                range.toExclusive
        )) {
            ProcessingResult result = processor.process(input, consumer);
            requireExhausted(input);
            completion.complete(TaskOutcome.success(new TaskResult(
                    result.getProcessedCount(),
                    Collections.emptyList()
            )));
        } catch (Throwable failure) {
            completion.complete(TaskOutcome.failure(failure));
        }
    }

    private void runBoundaryTask(
            CompletableFuture<TaskOutcome> completion,
            List<ByteFragments> boundaryFragments,
            InputStreamProcessor<T> processor,
            Consumer<? super T> consumer
    ) {
        try (InputStream input = new FragmentSequenceInputStream(boundaryFragments)) {
            ProcessingResult result = processor.process(input, consumer);
            requireExhausted(input);
            completion.complete(TaskOutcome.success(new TaskResult(
                    result.getProcessedCount(),
                    Collections.emptyList()
            )));
        } catch (Throwable failure) {
            completion.complete(TaskOutcome.failure(failure));
        }
    }

    private static void requireExhausted(InputStream input) throws IOException {
        if (input.read() >= 0) {
            throw new IllegalStateException(
                    "InputParser returned before consuming its complete-record stream"
            );
        }
    }

    private static InputStream openBounded(
            RangeSource source,
            long fromInclusive,
            long toExclusive
    ) throws IOException {
        InputStream input = Objects.requireNonNull(
                source.openRange(fromInclusive, toExclusive),
                "RangeSource returned null"
        );
        return new BoundedInputStream(input, toExclusive - fromInclusive);
    }

    private static List<SourceRange> divideRanges(long size, int requestedParallelism) {
        int count = (int) Math.min((long) requestedParallelism, size);
        long baseLength = size / count;
        long longerRangeCount = size % count;
        List<SourceRange> ranges = new ArrayList<>(count);
        long from = 0L;
        for (int ordinal = 0; ordinal < count; ordinal++) {
            long length = baseLength + (ordinal < longerRangeCount ? 1L : 0L);
            long to = from + length;
            ranges.add(new SourceRange(from, to, ordinal == 0, ordinal == count - 1));
            from = to;
        }
        return ranges;
    }

    private static AwaitedTasks awaitTasks(
            List<CompletableFuture<TaskOutcome>> submitted
    ) {
        List<TaskOutcome> outcomes = new ArrayList<>(submitted.size());
        InterruptedException interruption = null;
        for (CompletableFuture<TaskOutcome> completion : submitted) {
            while (true) {
                try {
                    outcomes.add(completion.get());
                    break;
                } catch (InterruptedException interrupted) {
                    if (interruption == null) {
                        interruption = interrupted;
                    } else {
                        interruption.addSuppressed(interrupted);
                    }
                } catch (ExecutionException impossible) {
                    outcomes.add(TaskOutcome.failure(impossible.getCause()));
                    break;
                }
            }
        }
        if (interruption == null && Thread.interrupted()) {
            interruption = new InterruptedException("Interrupted while awaiting processing tasks");
        }
        return new AwaitedTasks(outcomes, interruption);
    }

    private static void throwIfFailed(
            AwaitedTasks awaited,
            Throwable submissionFailure
    ) throws IOException, InterruptedException {
        Throwable terminalFailure = firstTaskFailure(awaited.outcomes);
        if (terminalFailure == null) {
            terminalFailure = submissionFailure;
        }
        if (awaited.interruption != null) {
            if (terminalFailure != null) {
                awaited.interruption.addSuppressed(terminalFailure);
            }
            throw awaited.interruption;
        }
        if (terminalFailure != null) {
            rethrow(terminalFailure);
        }
    }

    private static Throwable firstTaskFailure(List<TaskOutcome> outcomes) {
        for (TaskOutcome outcome : outcomes) {
            if (outcome.failure != null) {
                return outcome.failure;
            }
        }
        return null;
    }

    private static void rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException("Unexpected checked processing-task failure", failure);
    }

    private static final class SourceRange {

        private final long fromInclusive;
        private final long toExclusive;
        private final boolean first;
        private final boolean last;

        private SourceRange(
                long fromInclusive,
                long toExclusive,
                boolean first,
                boolean last
        ) {
            this.fromInclusive = fromInclusive;
            this.toExclusive = toExclusive;
            this.first = first;
            this.last = last;
        }
    }

    private static final class TaskResult {

        private final long processedCount;
        private final List<ByteFragments> boundaryFragments;

        private TaskResult(long processedCount, List<ByteFragments> boundaryFragments) {
            this.processedCount = processedCount;
            this.boundaryFragments = boundaryFragments;
        }
    }

    private static final class TaskOutcome {

        private final TaskResult result;
        private final Throwable failure;

        private TaskOutcome(TaskResult result, Throwable failure) {
            this.result = result;
            this.failure = failure;
        }

        private static TaskOutcome success(TaskResult result) {
            return new TaskOutcome(result, null);
        }

        private static TaskOutcome failure(Throwable failure) {
            return new TaskOutcome(null, failure);
        }
    }

    private static final class AwaitedTasks {

        private final List<TaskOutcome> outcomes;
        private final InterruptedException interruption;

        private AwaitedTasks(
                List<TaskOutcome> outcomes,
                InterruptedException interruption
        ) {
            this.outcomes = outcomes;
            this.interruption = interruption;
        }
    }
}
