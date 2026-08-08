package io.github.jutil.parallelrangeprocessor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

final class BoundedInputStream extends InputStream {

    private final InputStream delegate;
    private long remaining;

    BoundedInputStream(InputStream delegate, long length) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (length < 0L) {
            throw new IllegalArgumentException("length must not be negative");
        }
        this.remaining = length;
    }

    @Override
    public int read() throws IOException {
        if (remaining == 0L) {
            return -1;
        }

        int value = delegate.read();
        if (value >= 0) {
            remaining--;
        }
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        if (offset < 0 || length < 0 || length > bytes.length - offset) {
            throw new IndexOutOfBoundsException();
        }
        if (length == 0) {
            return 0;
        }
        if (remaining == 0L) {
            return -1;
        }

        int requested = (int) Math.min((long) length, remaining);
        int read = delegate.read(bytes, offset, requested);
        if (read > 0) {
            remaining -= read;
        }
        return read;
    }

    @Override
    public long skip(long count) throws IOException {
        if (count <= 0L || remaining == 0L) {
            return 0L;
        }
        long skipped = delegate.skip(Math.min(count, remaining));
        if (skipped > 0L) {
            remaining -= skipped;
        }
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return (int) Math.min((long) delegate.available(), Math.min(remaining, Integer.MAX_VALUE));
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
