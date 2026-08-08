package io.github.jutil.parallelrangeprocessor;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;

final class FragmentSequenceInputStream extends InputStream {

    private final List<ByteFragments> fragments;
    private int fragmentIndex;
    private int segmentIndex;
    private int segmentOffset;
    private long fragmentRemaining;

    FragmentSequenceInputStream(List<ByteFragments> fragments) {
        this.fragments = Objects.requireNonNull(fragments, "fragments");
        moveToNextNonEmptyFragment();
    }

    @Override
    public int read() {
        if (fragmentIndex >= fragments.size()) {
            return -1;
        }

        byte[] segment = currentSegment();
        int value = segment[segmentOffset] & 0xff;
        advance(1);
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        Objects.requireNonNull(bytes, "bytes");
        if (offset < 0 || length < 0 || length > bytes.length - offset) {
            throw new IndexOutOfBoundsException();
        }
        if (length == 0) {
            return 0;
        }
        if (fragmentIndex >= fragments.size()) {
            return -1;
        }

        int total = 0;
        while (total < length && fragmentIndex < fragments.size()) {
            byte[] segment = currentSegment();
            int availableInSegment = segment.length - segmentOffset;
            int copied = (int) Math.min(
                    (long) Math.min(length - total, availableInSegment),
                    fragmentRemaining
            );
            System.arraycopy(segment, segmentOffset, bytes, offset + total, copied);
            advance(copied);
            total += copied;
        }
        return total;
    }

    private byte[] currentSegment() {
        return fragments.get(fragmentIndex).segments()[segmentIndex];
    }

    private void advance(int count) {
        segmentOffset += count;
        fragmentRemaining -= count;

        if (fragmentRemaining == 0L) {
            fragmentIndex++;
            segmentIndex = 0;
            segmentOffset = 0;
            moveToNextNonEmptyFragment();
        } else if (segmentOffset == currentSegment().length) {
            segmentIndex++;
            segmentOffset = 0;
        }
    }

    private void moveToNextNonEmptyFragment() {
        while (fragmentIndex < fragments.size()) {
            ByteFragments fragment = fragments.get(fragmentIndex);
            if (!fragment.isEmpty()) {
                fragmentRemaining = fragment.length();
                return;
            }
            fragmentIndex++;
        }
        fragmentRemaining = 0L;
    }
}
