package io.github.jutil.parallelrangeprocessor;

final class ByteFragments {

    private final byte[][] segments;
    private final long length;

    ByteFragments(byte[][] segments, long length) {
        this.segments = segments;
        this.length = length;
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
