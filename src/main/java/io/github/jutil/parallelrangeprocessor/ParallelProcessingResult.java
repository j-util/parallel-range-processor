package io.github.jutil.parallelrangeprocessor;

/**
 * Immutable summary of a normally completed parallel processing operation.
 *
 * <p>The processed count is the total number of parser-emitted items whose
 * caller-supplied consumer invocation returned normally. It is a consumer-call
 * count, not framing metadata or a count inferred from source delimiters. No
 * result is returned when source, parser, consumer, executor, or orchestration
 * processing fails.</p>
 */
public final class ParallelProcessingResult {

    private final long processedCount;

    ParallelProcessingResult(long processedCount) {
        this.processedCount = processedCount;
    }

    /**
     * Returns the total number of successful consumer calls.
     *
     * @return the successful consumer-call count
     */
    public long getProcessedCount() {
        return processedCount;
    }
}
