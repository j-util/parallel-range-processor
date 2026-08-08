package io.github.jutil.parallelrangeprocessor;

import java.util.ArrayList;
import java.util.List;

final class FragmentAccumulator {

    static final int SEGMENT_SIZE = 8192;

    private final List<byte[]> segments = new ArrayList<>();
    private int usedSegments;
    private int writeOffset;
    private long size;
    private int readSegmentIndex;
    private int readOffset;
    private long readRemaining;

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
        readSegmentIndex = 0;
        readOffset = 0;
        readRemaining = 0L;
    }

    boolean isEmpty() {
        return size == 0L;
    }

    void beginReading() {
        readSegmentIndex = 0;
        readOffset = 0;
        readRemaining = size;
    }

    int read() {
        if (readRemaining == 0L) {
            return -1;
        }

        byte[] segment = segments.get(readSegmentIndex);
        int value = segment[readOffset] & 0xff;
        advanceRead(1);
        return value;
    }

    int read(byte[] destination, int offset, int length) {
        if (readRemaining == 0L) {
            return -1;
        }

        int total = 0;
        while (total < length && readRemaining > 0L) {
            byte[] segment = segments.get(readSegmentIndex);
            int available = segment.length - readOffset;
            int copied = (int) Math.min(
                    (long) Math.min(length - total, available),
                    readRemaining
            );
            System.arraycopy(segment, readOffset, destination, offset + total, copied);
            advanceRead(copied);
            total += copied;
        }
        return total;
    }

    ByteFragments toFragments() {
        byte[][] retained = new byte[usedSegments][];
        for (int index = 0; index < usedSegments; index++) {
            retained[index] = segments.get(index);
        }
        return new ByteFragments(retained, size);
    }

    private void advanceRead(int count) {
        readOffset += count;
        readRemaining -= count;
        if (readRemaining > 0L && readOffset == SEGMENT_SIZE) {
            readSegmentIndex++;
            readOffset = 0;
        }
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
