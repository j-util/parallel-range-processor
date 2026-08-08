package io.github.jutil.parallelrangeprocessor;

import java.io.IOException;
import java.io.InputStream;

/**
 * A known-size byte source that supports independent reads of exact ranges.
 *
 * <p>The size and range coordinates are byte offsets. Source content must
 * remain logically stable from the call to {@link #size()} until every stream
 * opened for the same processing operation has been closed. Implementations
 * must support multiple independent range streams being read concurrently.</p>
 *
 * <p>The caller of {@link #openRange(long, long)} owns the returned stream and
 * must close it. {@link ParallelRangeProcessor} therefore closes every stream
 * it opens, on both normal and exceptional completion.</p>
 */
public interface RangeSource {

    /**
     * Returns the non-negative total size of this source in bytes.
     *
     * @return the total byte size
     * @throws IOException if the size cannot be obtained
     */
    long size() throws IOException;

    /**
     * Opens a stream representing exactly the half-open byte interval
     * {@code [fromInclusive, toExclusive)}.
     *
     * <p>The returned stream must start at {@code fromInclusive}, must reach
     * end-of-stream after exactly {@code toExclusive - fromInclusive} bytes,
     * and must be independent of streams returned by other calls. A zero-length
     * range is valid and returns an empty stream.</p>
     *
     * @param fromInclusive the first byte offset, inclusive
     * @param toExclusive the ending byte offset, exclusive
     * @return a newly opened caller-owned stream for only that range
     * @throws IllegalArgumentException if either offset is negative,
     *         {@code fromInclusive > toExclusive}, or {@code toExclusive} is
     *         greater than the current source size
     * @throws IOException if the range cannot be opened
     */
    InputStream openRange(long fromInclusive, long toExclusive) throws IOException;
}
