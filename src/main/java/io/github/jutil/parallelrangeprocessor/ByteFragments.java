package io.github.jutil.parallelrangeprocessor;

final class ByteFragments {

    private static final ByteFragments EMPTY = new ByteFragments(new byte[0][], 0L);

    private final byte[][] segments;
    private final long length;

    ByteFragments(byte[][] segments, long length) {
        this.segments = segments;
        this.length = length;
    }

    static ByteFragments empty() {
        return EMPTY;
    }

    boolean isEmpty() {
        return length == 0L;
    }

    byte[][] segments() {
        return segments;
    }

    long length() {
        return length;
    }
}
