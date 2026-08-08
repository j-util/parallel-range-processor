package io.github.jutil.parallelrangeprocessor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * A {@link RangeSource} backed by a local file.
 *
 * <p>Each opened range uses an independent read-only file channel. The file
 * must not be replaced, resized, or modified during a processing operation.</p>
 */
public final class FileRangeSource implements RangeSource {

    private final Path path;

    /**
     * Creates a range source for {@code path}.
     *
     * @param path the file to read
     * @throws NullPointerException if {@code path} is {@code null}
     */
    public FileRangeSource(Path path) {
        this.path = Objects.requireNonNull(path, "path");
    }

    /**
     * Returns the current file size.
     *
     * @return the file size in bytes
     * @throws IOException if file metadata cannot be read
     */
    @Override
    public long size() throws IOException {
        return Files.size(path);
    }

    /**
     * Opens an independently positioned, length-limited file stream.
     *
     * @param fromInclusive the first byte offset, inclusive
     * @param toExclusive the ending byte offset, exclusive
     * @return a caller-owned stream limited to the requested range
     * @throws IllegalArgumentException if the range is invalid for the current
     *         file size
     * @throws IOException if the file cannot be inspected or opened
     */
    @Override
    public InputStream openRange(long fromInclusive, long toExclusive) throws IOException {
        long currentSize = size();
        validateRange(fromInclusive, toExclusive, currentSize);

        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        boolean success = false;
        try {
            channel.position(fromInclusive);
            InputStream result = new BoundedInputStream(
                    Channels.newInputStream(channel),
                    toExclusive - fromInclusive
            );
            success = true;
            return result;
        } finally {
            if (!success) {
                channel.close();
            }
        }
    }

    private static void validateRange(long fromInclusive, long toExclusive, long size) {
        if (fromInclusive < 0L) {
            throw new IllegalArgumentException("fromInclusive must not be negative");
        }
        if (toExclusive < 0L) {
            throw new IllegalArgumentException("toExclusive must not be negative");
        }
        if (fromInclusive > toExclusive) {
            throw new IllegalArgumentException("fromInclusive must not exceed toExclusive");
        }
        if (toExclusive > size) {
            throw new IllegalArgumentException("toExclusive must not exceed the file size");
        }
    }
}
