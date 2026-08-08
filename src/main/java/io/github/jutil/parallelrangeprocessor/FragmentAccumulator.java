package io.github.jutil.parallelrangeprocessor;

import java.util.ArrayList;
import java.util.List;

final class FragmentAccumulator {

    static final int SEGMENT_SIZE = 8192;

    private final List<byte[]> segments = new ArrayList<>();
    private int usedSegments;
    private int writeOffset;
    private long size;

    void append(byte[] source, int offset, int length) {
        if (length == 0) {
            return;
        }

        int remaining = length;
        int sourceOffset = offset;
        while (remaining > 0) {
            byte[] segment = writableSegment();
            int copied = Math.min(remaining, segment.length - writeOffset);
            System.arraycopy(source, sourceOffset, segment, writeOffset, copied);
            writeOffset += copied;
            sourceOffset += copied;
            remaining -= copied;
            size += copied;
        }
    }

    void clear() {
        usedSegments = 0;
        writeOffset = 0;
        size = 0L;
    }

    boolean isEmpty() {
        return size == 0L;
    }

    ByteFragments toFragments() {
        if (size == 0L) {
            return ByteFragments.empty();
        }
        byte[][] retained = new byte[usedSegments][];
        for (int index = 0; index < usedSegments; index++) {
            retained[index] = segments.get(index);
        }
        return new ByteFragments(retained, size);
    }

    private byte[] writableSegment() {
        if (usedSegments == 0) {
            usedSegments = 1;
            writeOffset = 0;
            return segmentAt(0);
        }
        if (writeOffset == SEGMENT_SIZE) {
            int nextIndex = usedSegments;
            usedSegments++;
            writeOffset = 0;
            return segmentAt(nextIndex);
        }
        return segments.get(usedSegments - 1);
    }

    private byte[] segmentAt(int index) {
        if (index == segments.size()) {
            segments.add(new byte[SEGMENT_SIZE]);
        }
        return segments.get(index);
    }
}
