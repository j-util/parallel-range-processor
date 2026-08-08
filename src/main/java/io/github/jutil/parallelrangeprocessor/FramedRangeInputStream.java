package io.github.jutil.parallelrangeprocessor;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class FramedRangeInputStream extends InputStream {

    private final InputStream source;
    private final long fromInclusive;
    private final long toExclusive;
    private final boolean firstRange;
    private final boolean lastRange;
    private final byte delimiter;
    private final byte[] readBuffer;
    private final List<ByteFragments> boundaryFragments = new ArrayList<>(2);

    private FragmentAccumulator current = new FragmentAccumulator();
    private int readOffset;
    private int readLimit;
    private long sourceBytesRead;
    private boolean prepared;
    private boolean sourceEnded;
    private boolean recordExposed;
    private boolean finalRecordExposed;
    private boolean finished;

    FramedRangeInputStream(
            InputStream source,
            long fromInclusive,
            long toExclusive,
            boolean firstRange,
            boolean lastRange,
            byte delimiter,
            int readBufferSize
    ) {
        this.source = Objects.requireNonNull(source, "source");
        this.fromInclusive = fromInclusive;
        this.toExclusive = toExclusive;
        this.firstRange = firstRange;
        this.lastRange = lastRange;
        this.delimiter = delimiter;
        this.readBuffer = new byte[readBufferSize];
    }

    boolean prepare() throws IOException {
        if (!prepared) {
            prepared = true;
            if (!firstRange) {
                if (!readThroughDelimiter()) {
                    if (!current.isEmpty()) {
                        boundaryFragments.add(current.toFragments());
                    }
                    finished = true;
                    return false;
                }
                boundaryFragments.add(current.toFragments());
                current = new FragmentAccumulator();
            }
            loadNextRecord();
        }
        return recordExposed;
    }

    List<ByteFragments> boundaryFragments() {
        if (!finished) {
            throw new IllegalStateException("framed range has not been consumed to completion");
        }
        return Collections.unmodifiableList(new ArrayList<>(boundaryFragments));
    }

    @Override
    public int read() throws IOException {
        if (!ensureExposedRecord()) {
            return -1;
        }

        int value = current.read();
        if (value >= 0) {
            return value;
        }
        finishExposedRecord();
        return read();
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
        if (!ensureExposedRecord()) {
            return -1;
        }

        int read = current.read(bytes, offset, length);
        if (read >= 0) {
            return read;
        }
        finishExposedRecord();
        return read(bytes, offset, length);
    }

    @Override
    public void close() throws IOException {
        source.close();
    }

    private boolean ensureExposedRecord() throws IOException {
        if (!prepared) {
            prepare();
        }
        while (!recordExposed && !finished) {
            loadNextRecord();
        }
        return recordExposed;
    }

    private void finishExposedRecord() throws IOException {
        recordExposed = false;
        current.clear();
        if (finalRecordExposed) {
            finalRecordExposed = false;
            finished = true;
            return;
        }
        loadNextRecord();
    }

    private void loadNextRecord() throws IOException {
        if (finished || recordExposed) {
            return;
        }

        if (readThroughDelimiter()) {
            exposeCurrentRecord();
            return;
        }

        if (lastRange && !current.isEmpty()) {
            exposeCurrentRecord();
            finalRecordExposed = true;
        } else {
            if (!lastRange && !current.isEmpty()) {
                boundaryFragments.add(current.toFragments());
            }
            finished = true;
        }
    }

    private void exposeCurrentRecord() {
        current.beginReading();
        recordExposed = true;
    }

    private boolean readThroughDelimiter() throws IOException {
        while (true) {
            if (readOffset == readLimit) {
                if (!refill()) {
                    return false;
                }
            }

            int delimiterIndex = -1;
            for (int index = readOffset; index < readLimit; index++) {
                if (readBuffer[index] == delimiter) {
                    delimiterIndex = index;
                    break;
                }
            }

            if (delimiterIndex >= 0) {
                int length = delimiterIndex - readOffset + 1;
                current.append(readBuffer, readOffset, length);
                readOffset = delimiterIndex + 1;
                return true;
            }

            current.append(readBuffer, readOffset, readLimit - readOffset);
            readOffset = readLimit;
        }
    }

    private boolean refill() throws IOException {
        if (sourceEnded) {
            return false;
        }

        int maximum = (int) Math.min(
                (long) readBuffer.length,
                toExclusive - fromInclusive - sourceBytesRead
        );
        if (maximum == 0) {
            sourceEnded = true;
            return false;
        }

        int read = source.read(readBuffer, 0, maximum);
        if (read < 0) {
            throw new EOFException(
                    "RangeSource ended at byte " + (fromInclusive + sourceBytesRead)
                            + " before assigned range [" + fromInclusive
                            + ", " + toExclusive + ") was complete"
            );
        }
        if (read == 0) {
            int value = source.read();
            if (value < 0) {
                throw new EOFException(
                        "RangeSource ended at byte " + (fromInclusive + sourceBytesRead)
                                + " before assigned range [" + fromInclusive
                                + ", " + toExclusive + ") was complete"
                );
            }
            readBuffer[0] = (byte) value;
            read = 1;
        }

        sourceBytesRead += read;
        readOffset = 0;
        readLimit = read;
        if (sourceBytesRead == toExclusive - fromInclusive) {
            sourceEnded = true;
        }
        return true;
    }

}
