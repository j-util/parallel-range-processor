package io.github.jutil.parallelrangeprocessor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jutil.inputstreamprocessor.core.InputParser;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

class ParallelRangeProcessorTest {

    private static final Executor DIRECT_EXECUTOR = Runnable::run;

    @Test
    void emptySourceProducesZeroWithoutOpeningRangesOrCreatingParsers() throws Exception {
        ByteArrayRangeSource source = source("");
        AtomicInteger parserCreations = new AtomicInteger();
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                8,
                DIRECT_EXECUTOR,
                () -> {
                    parserCreations.incrementAndGet();
                    return lineParser();
                },
                RecordDelimiter.newline()
        );

        ParallelProcessingResult result = processor.process(source, item -> {
        });

        assertEquals(0L, result.getProcessedCount());
        assertEquals(0, parserCreations.get());
        assertEquals(Collections.emptyList(), source.openedRanges());
    }

    @Test
    void oneRecordWithTrailingDelimiterIsProcessed() throws Exception {
        assertRecords("only\n", 4, "only");
    }

    @Test
    void oneRecordWithoutTrailingDelimiterIsProcessed() throws Exception {
        assertRecords("only", 4, "only");
    }

    @Test
    void recordsExactlyAlignedToRangesAreProcessedOnce() throws Exception {
        assertRecords("a\nb\nc\nd\n", 4, "a", "b", "c", "d");
    }

    @Test
    void splitImmediatelyAfterDelimiterLosesNothing() throws Exception {
        assertRecords("one\ntwo\n", 2, "one", "two");
    }

    @Test
    void splitInsideFirstRecordReconstructsIt() throws Exception {
        assertRecords("abcdef\nx\n", 3, "abcdef", "x");
    }

    @Test
    void intermediateRangeWithPrefixCompleteRecordAndSuffixIsCorrect() throws Exception {
        String content = "r0\nA0\nxxxx"
                + "yyyy\nA1\nzz"
                + "zzzz\nA2\nQ";

        assertRecords(
                content,
                3,
                "r0",
                "A0",
                "xxxxyyyy",
                "A1",
                "zzzzzz",
                "A2",
                "Q"
        );
    }

    @Test
    void intermediateRangeWithPrefixAndSuffixButNoCompleteRecordIsCorrect() throws Exception {
        String content = "a\nb\nxxxxxx"
                + "yyyy\nzzzzz"
                + "wwwww\nc\nd!";

        assertRecords(
                content,
                3,
                "a",
                "b",
                "xxxxxxyyyy",
                "zzzzzwwwww",
                "c",
                "d!"
        );
    }

    @Test
    void delimiterFreeIntermediateRangeIsAnExplicitMiddleFragment() throws Exception {
        String content = "a\nxxxxxxxx"
                + "yyyyyyyyyy"
                + "zzzzzz\nb\nc";

        assertRecords(
                content,
                3,
                "a",
                "xxxxxxxxyyyyyyyyyyzzzzzz",
                "b",
                "c"
        );
    }

    @Test
    void oneRecordCanSpanMoreThanThreeRanges() throws Exception {
        String oversized = repeated('q', 50);
        assertRecords(oversized + "\nend\n", 8, oversized, "end");
    }

    @Test
    void recordLargerThanInternalReadAndFragmentBuffersIsProcessed() throws Exception {
        String oversized = repeated('x', 25000);
        assertRecords("first\n" + oversized + "\nlast", 7, "first", oversized, "last");
    }

    @Test
    void multiSegmentOrdinaryRecordIsReadFromReusableFramingState() throws Exception {
        String ordinary = repeated('x', 9000);
        String boundary = repeated('z', 10000);
        List<String> consumed = synchronizedList();
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                2,
                DIRECT_EXECUTOR,
                () -> byteDelimiterParser((byte) '|'),
                RecordDelimiter.singleByte((byte) '|')
        );

        ParallelProcessingResult result = processor.process(
                source(ordinary + '|' + boundary),
                consumed::add
        );

        assertEquals(2L, result.getProcessedCount());
        assertSameElements(Arrays.asList(ordinary, boundary), consumed);
    }

    @Test
    void sourceSmallerThanParallelismCreatesNoZeroLengthRanges() throws Exception {
        ByteArrayRangeSource source = source("a\n");
        List<String> consumed = synchronizedList();
        ParallelRangeProcessor<String> processor = processor(20, DIRECT_EXECUTOR);

        ParallelProcessingResult result = processor.process(source, consumed::add);

        assertEquals(1L, result.getProcessedCount());
        assertEquals(Collections.singletonList("a"), consumed);
        assertEquals(
                Arrays.asList(new RangeCall(0, 1), new RangeCall(1, 2)),
                source.openedRanges()
        );
    }

    @Test
    void parallelismOneBypassesFramingAndUsesTheSuppliedExecutor() throws Exception {
        ByteArrayRangeSource source = source("a\nb\nc");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger submissions = new AtomicInteger();
        AtomicReference<Thread> parserThread = new AtomicReference<>();
        Thread callingThread = Thread.currentThread();
        List<String> consumed = new ArrayList<>();
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                1,
                command -> {
                    submissions.incrementAndGet();
                    executor.execute(command);
                },
                () -> (input, emitter) -> {
                    assertFalse(input instanceof FramedRangeInputStream);
                    parserThread.set(Thread.currentThread());
                    lineParser().parse(input, emitter);
                },
                RecordDelimiter.newline()
        );

        try {
            ParallelProcessingResult result = processor.process(source, consumed::add);

            assertEquals(3L, result.getProcessedCount());
            assertEquals(Arrays.asList("a", "b", "c"), consumed);
            assertEquals(1, submissions.get());
            assertNotSame(callingThread, parserThread.get());
            assertEquals(Collections.singletonList(new RangeCall(0, 5)), source.openedRanges());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void sourceSizeCanReduceRangeDivisionToTheSingleRangeFastPath() throws Exception {
        AtomicInteger submissions = new AtomicInteger();
        AtomicBoolean framingObserved = new AtomicBoolean();
        List<String> consumed = new ArrayList<>();
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                8,
                command -> {
                    submissions.incrementAndGet();
                    command.run();
                },
                () -> (input, emitter) -> {
                    framingObserved.set(input instanceof FramedRangeInputStream);
                    lineParser().parse(input, emitter);
                },
                RecordDelimiter.newline()
        );

        ParallelProcessingResult result = processor.process(source("x"), consumed::add);

        assertEquals(1L, result.getProcessedCount());
        assertEquals(Collections.singletonList("x"), consumed);
        assertEquals(1, submissions.get());
        assertFalse(framingObserved.get());
    }

    @Test
    void nonPositiveParallelismIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> processor(0, DIRECT_EXECUTOR)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> processor(-1, DIRECT_EXECUTOR)
        );
    }

    @Test
    void nullConstructorAndProcessArgumentsAreRejected() {
        assertThrows(
                NullPointerException.class,
                () -> new ParallelRangeProcessor<String>(
                        1,
                        null,
                        ParallelRangeProcessorTest::lineParser,
                        RecordDelimiter.newline()
                )
        );
        assertThrows(
                NullPointerException.class,
                () -> new ParallelRangeProcessor<String>(
                        1,
                        DIRECT_EXECUTOR,
                        null,
                        RecordDelimiter.newline()
                )
        );
        assertThrows(
                NullPointerException.class,
                () -> new ParallelRangeProcessor<String>(
                        1,
                        DIRECT_EXECUTOR,
                        ParallelRangeProcessorTest::lineParser,
                        null
                )
        );

        ParallelRangeProcessor<String> processor = processor(1, DIRECT_EXECUTOR);
        assertThrows(NullPointerException.class, () -> processor.process(null, item -> {
        }));
        assertThrows(NullPointerException.class, () -> processor.process(source("a"), null));
    }

    @Test
    void negativeSourceSizeIsRejectedAsAnInputFailure() {
        RangeSource source = new RangeSource() {
            @Override
            public long size() {
                return -1L;
            }

            @Override
            public InputStream openRange(long fromInclusive, long toExclusive) {
                throw new AssertionError("must not open a range");
            }
        };

        assertThrows(IOException.class, () -> processor(2, DIRECT_EXECUTOR).process(
                source,
                item -> {
                }
        ));
    }

    @Test
    void sourceSizeFailurePropagatesUnchanged() {
        IOException failure = new IOException("size failed");
        RangeSource source = new RangeSource() {
            @Override
            public long size() throws IOException {
                throw failure;
            }

            @Override
            public InputStream openRange(long fromInclusive, long toExclusive) {
                throw new AssertionError("must not open a range");
            }
        };

        IOException thrown = assertThrows(
                IOException.class,
                () -> processor(2, DIRECT_EXECUTOR).process(source, item -> {
                })
        );

        assertSame(failure, thrown);
    }

    @Test
    void sourceRangeFailurePropagatesUnchanged() {
        IOException failure = new IOException("range failed");
        RangeSource source = new RangeSource() {
            @Override
            public long size() {
                return 1L;
            }

            @Override
            public InputStream openRange(long fromInclusive, long toExclusive)
                    throws IOException {
                throw failure;
            }
        };

        IOException thrown = assertThrows(
                IOException.class,
                () -> processor(1, DIRECT_EXECUTOR).process(source, item -> {
                })
        );

        assertSame(failure, thrown);
    }

    @Test
    void prematureRangeEndIsReported() {
        RangeSource source = new RangeSource() {
            @Override
            public long size() {
                return 4L;
            }

            @Override
            public InputStream openRange(long fromInclusive, long toExclusive) {
                return new ByteArrayInputStream(new byte[]{'a'});
            }
        };

        assertThrows(
                EOFException.class,
                () -> processor(1, DIRECT_EXECUTOR).process(source, item -> {
                })
        );
    }

    @Test
    void parserFailurePropagatesUnchanged() {
        IOException failure = new IOException("parser failed");
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                1,
                DIRECT_EXECUTOR,
                () -> (input, emitter) -> {
                    throw failure;
                },
                RecordDelimiter.newline()
        );

        IOException thrown = assertThrows(
                IOException.class,
                () -> processor.process(source("record\n"), item -> {
                })
        );

        assertSame(failure, thrown);
    }

    @Test
    void parserReturningBeforeEndOfStreamIsRejected() {
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                1,
                DIRECT_EXECUTOR,
                () -> (input, emitter) -> emitter.accept("invented"),
                RecordDelimiter.newline()
        );

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> processor.process(source("record\n"), item -> {
                })
        );

        assertTrue(failure.getMessage().contains("before consuming"));
    }

    @Test
    void consumerFailurePropagatesUnchangedAndCompletedCallsRemain() {
        RuntimeException failure = new IllegalStateException("consumer failed");
        List<String> attempted = new ArrayList<>();

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> processor(1, DIRECT_EXECUTOR).process(source("first\nsecond\nthird\n"), item -> {
                    attempted.add(item);
                    if ("second".equals(item)) {
                        throw failure;
                    }
                })
        );

        assertSame(failure, thrown);
        assertEquals(Arrays.asList("first", "second"), attempted);
    }

    @Test
    void resultCountsSuccessfulConsumerCallsRatherThanFramingBoundaries() throws Exception {
        InputParser<String> twicePerLine = (input, emitter) -> {
            BufferedReader reader = reader(input);
            String line;
            while ((line = reader.readLine()) != null) {
                emitter.accept(line + "-1");
                emitter.accept(line + "-2");
            }
        };
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                4,
                DIRECT_EXECUTOR,
                () -> twicePerLine,
                RecordDelimiter.newline()
        );
        List<String> consumed = new ArrayList<>();

        ParallelProcessingResult result = processor.process(source("a\nb\nc\n"), consumed::add);

        assertEquals(6L, result.getProcessedCount());
        assertEquals(6, consumed.size());
    }

    @Test
    void reverseWorkerCompletionStillReconstructsFragmentsInSourceOrder() throws Exception {
        String content = "abcdefghijklmnopqrst";
        ByteArrayRangeSource source = source(content);
        ReverseBatchExecutor executor = new ReverseBatchExecutor(4);
        List<String> consumed = new ArrayList<>();

        ParallelProcessingResult result = processor(4, executor).process(source, consumed::add);

        assertEquals(1L, result.getProcessedCount());
        assertEquals(Collections.singletonList(content), consumed);
        assertEquals(
                Arrays.asList(
                        new RangeCall(15, 20),
                        new RangeCall(10, 15),
                        new RangeCall(5, 10),
                        new RangeCall(0, 5)
                ),
                source.openedRanges()
        );
    }

    @Test
    void reconstructedBoundariesRunAsOneFinalExecutorTask() throws Exception {
        ExecutorService workerThread = Executors.newSingleThreadExecutor();
        AtomicInteger submissions = new AtomicInteger();
        AtomicReference<Thread> consumerThread = new AtomicReference<>();
        Thread callingThread = Thread.currentThread();
        List<String> consumed = new ArrayList<>();
        Executor executor = command -> {
            submissions.incrementAndGet();
            workerThread.execute(command);
        };

        try {
            ParallelProcessingResult result = processor(4, executor).process(
                    source("abcdefgh"),
                    item -> {
                        consumerThread.set(Thread.currentThread());
                        consumed.add(item);
                    }
            );

            assertEquals(1L, result.getProcessedCount());
            assertEquals(Collections.singletonList("abcdefgh"), consumed);
            assertEquals(5, submissions.get());
            assertNotSame(callingThread, consumerThread.get());
        } finally {
            workerThread.shutdownNow();
            assertTrue(workerThread.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void assignedRangesCoverEverySourceByteExactlyOnce() throws Exception {
        ByteArrayRangeSource source = source("abcdefghijklmnopq");

        processor(4, DIRECT_EXECUTOR).process(source, item -> {
        });

        assertEquals(
                Arrays.asList(
                        new RangeCall(0, 5),
                        new RangeCall(5, 9),
                        new RangeCall(9, 13),
                        new RangeCall(13, 17)
                ),
                source.openedRanges()
        );
    }

    @Test
    void manyRecordsAreNeitherDuplicatedNorLost() throws Exception {
        StringBuilder content = new StringBuilder();
        List<String> expected = new ArrayList<>();
        for (int index = 0; index < 200; index++) {
            String record = "record-" + index;
            expected.add(record);
            content.append(record).append('\n');
        }
        List<String> consumed = synchronizedList();
        ExecutorService executor = Executors.newFixedThreadPool(7);
        try {
            ParallelProcessingResult result = processor(11, executor).process(
                    source(content.toString()),
                    consumed::add
            );

            assertEquals(expected.size(), result.getProcessedCount());
            assertSameElements(expected, consumed);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void finalRecordWithoutDelimiterAtEofIsProcessedOnce() throws Exception {
        assertRecords("a\nb\nfinal", 5, "a", "b", "final");
    }

    @Test
    void consecutiveDelimitersRemainVisibleToParserAsEmptyRecords() throws Exception {
        assertRecords("\n\nvalue\n\n", 4, "", "", "value", "");
    }

    @Test
    void genericSingleByteDelimiterFramesRecords() throws Exception {
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                5,
                DIRECT_EXECUTOR,
                () -> byteDelimiterParser((byte) '|'),
                RecordDelimiter.singleByte((byte) '|')
        );
        List<String> consumed = new ArrayList<>();

        ParallelProcessingResult result = processor.process(source("a||bbb|last"), consumed::add);

        assertEquals(4L, result.getProcessedCount());
        assertSameElements(Arrays.asList("a", "", "bbb", "last"), consumed);
    }

    @Test
    void consumerCanBeInvokedConcurrently() throws Exception {
        String content = "a\none\nxxxx" + "yyyy\ntwo\n?";
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CyclicBarrier concurrentCalls = new CyclicBarrier(2);
        AtomicBoolean observed = new AtomicBoolean();
        List<String> consumed = synchronizedList();
        Consumer<String> consumer = item -> {
            consumed.add(item);
            if ("a".equals(item) || "two".equals(item)) {
                try {
                    int arrival = concurrentCalls.await(5, TimeUnit.SECONDS);
                    if (arrival == 0) {
                        observed.set(true);
                    }
                } catch (Exception failure) {
                    throw new IllegalStateException(failure);
                }
            }
        };

        try {
            ParallelProcessingResult result = processor(2, executor).process(
                    source(content),
                    consumer
            );

            assertEquals(5L, result.getProcessedCount());
            assertTrue(observed.get());
            assertSameElements(
                    Arrays.asList("a", "one", "xxxxyyyy", "two", "?"),
                    consumed
            );
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void suppliedExecutorIsNotShutDown() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            processor(2, executor).process(source("a\nb\n"), item -> {
            });

            assertFalse(executor.isShutdown());
            assertEquals("still usable", executor.submit(() -> "still usable").get());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void allOpenedRangeStreamsCloseNormallyAndOnParserFailure() throws Exception {
        ByteArrayRangeSource normal = source("a\nb\nc\n");
        processor(3, DIRECT_EXECUTOR).process(normal, item -> {
        });
        assertEquals(normal.openedStreamCount(), normal.closedStreamCount());

        ByteArrayRangeSource failing = source("record\n");
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                1,
                DIRECT_EXECUTOR,
                () -> (input, emitter) -> {
                    throw new IOException("failed");
                },
                RecordDelimiter.newline()
        );
        assertThrows(IOException.class, () -> processor.process(failing, item -> {
        }));
        assertEquals(failing.openedStreamCount(), failing.closedStreamCount());
    }

    @Test
    void processorBoundsAnOverlongSourceStreamToItsAssignedRange() throws Exception {
        ByteArrayRangeSource source = new ByteArrayRangeSource(bytes("a\nb\nc\nd\n")) {
            @Override
            protected InputStream newRangeStream(int fromInclusive, int toExclusive) {
                return new ByteArrayInputStream(content, fromInclusive, content.length - fromInclusive);
            }
        };
        List<String> consumed = synchronizedList();

        ParallelProcessingResult result = processor(4, DIRECT_EXECUTOR).process(
                source,
                consumed::add
        );

        assertEquals(4L, result.getProcessedCount());
        assertSameElements(Arrays.asList("a", "b", "c", "d"), consumed);
    }

    @Test
    void parserFactoryCreatesIndependentWorkerAndBoundaryProcessors() throws Exception {
        AtomicInteger parserCreations = new AtomicInteger();
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                4,
                DIRECT_EXECUTOR,
                () -> {
                    parserCreations.incrementAndGet();
                    return lineParser();
                },
                RecordDelimiter.newline()
        );

        processor.process(source("abcdefgh"), item -> {
        });

        assertEquals(5, parserCreations.get());
    }

    @Test
    void nullParserFromFactoryFailsBeforeWorkerSubmission() {
        AtomicBoolean executorUsed = new AtomicBoolean();
        ParallelRangeProcessor<String> processor = new ParallelRangeProcessor<>(
                2,
                command -> executorUsed.set(true),
                () -> null,
                RecordDelimiter.newline()
        );

        assertThrows(NullPointerException.class, () -> processor.process(source("ab"), item -> {
        }));
        assertFalse(executorUsed.get());
    }

    @Test
    void lowestRangeFailureWinsRegardlessOfCompletionOrder() {
        IOException first = new IOException("first range");
        IOException second = new IOException("second range");
        RangeSource source = new RangeSource() {
            @Override
            public long size() {
                return 4L;
            }

            @Override
            public InputStream openRange(long fromInclusive, long toExclusive)
                    throws IOException {
                throw fromInclusive == 0L ? first : second;
            }
        };

        IOException thrown = assertThrows(
                IOException.class,
                () -> processor(4, new ReverseBatchExecutor(4)).process(source, item -> {
                })
        );

        assertSame(first, thrown);
    }

    @Test
    void executorRejectionPropagatesAfterSubmittedWorkersFinish() {
        RejectedExecutionException failure = new RejectedExecutionException("rejected");
        AtomicInteger submissions = new AtomicInteger();
        Executor executor = command -> {
            if (submissions.getAndIncrement() == 0) {
                command.run();
            } else {
                throw failure;
            }
        };

        RejectedExecutionException thrown = assertThrows(
                RejectedExecutionException.class,
                () -> processor(4, executor).process(source("abcd"), item -> {
                })
        );

        assertSame(failure, thrown);
    }

    @Test
    void finalBoundarySubmissionFailurePropagatesUnchanged() {
        RejectedExecutionException failure = new RejectedExecutionException("final rejected");
        AtomicInteger submissions = new AtomicInteger();
        Executor executor = command -> {
            if (submissions.incrementAndGet() == 5) {
                throw failure;
            }
            command.run();
        };
        List<String> consumed = new ArrayList<>();

        RejectedExecutionException thrown = assertThrows(
                RejectedExecutionException.class,
                () -> processor(4, executor).process(source("abcdefgh"), consumed::add)
        );

        assertSame(failure, thrown);
        assertEquals(5, submissions.get());
        assertTrue(consumed.isEmpty());
    }

    @Test
    void finalBoundaryConsumerFailurePropagatesUnchanged() {
        RuntimeException failure = new IllegalStateException("final consumer failed");
        AtomicInteger submissions = new AtomicInteger();
        Executor executor = command -> {
            submissions.incrementAndGet();
            command.run();
        };

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> processor(4, executor).process(source("abcdefgh"), item -> {
                    throw failure;
                })
        );

        assertSame(failure, thrown);
        assertEquals(5, submissions.get());
    }

    @Test
    void workerFailureWaitsForEveryOtherSubmittedWorkerToFinish() throws Exception {
        IOException failure = new IOException("first range failed");
        CountDownLatch secondWorkerEntered = new CountDownLatch(1);
        CountDownLatch releaseSecondWorker = new CountDownLatch(1);
        RangeSource source = new RangeSource() {
            @Override
            public long size() {
                return 2L;
            }

            @Override
            public InputStream openRange(long fromInclusive, long toExclusive)
                    throws IOException {
                if (fromInclusive == 0L) {
                    throw failure;
                }
                return new InputStream() {
                    private boolean read;

                    @Override
                    public int read() throws IOException {
                        if (read) {
                            return -1;
                        }
                        secondWorkerEntered.countDown();
                        try {
                            if (!releaseSecondWorker.await(5, TimeUnit.SECONDS)) {
                                throw new IOException("timed out awaiting test release");
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IOException(interrupted);
                        }
                        read = true;
                        return 'b';
                    }

                    @Override
                    public int read(byte[] bytes, int offset, int length) throws IOException {
                        int value = read();
                        if (value < 0) {
                            return -1;
                        }
                        bytes[offset] = (byte) value;
                        return 1;
                    }
                };
            }
        };
        ExecutorService workers = Executors.newFixedThreadPool(2);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<IOException> operation = caller.submit(() -> assertThrows(
                    IOException.class,
                    () -> processor(2, workers).process(source, item -> {
                    })
            ));

            assertTrue(secondWorkerEntered.await(5, TimeUnit.SECONDS));
            assertFalse(operation.isDone());
            releaseSecondWorker.countDown();
            assertSame(failure, operation.get(5, TimeUnit.SECONDS));
        } finally {
            releaseSecondWorker.countDown();
            workers.shutdownNow();
            caller.shutdownNow();
            assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(caller.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void interruptionWhileAwaitingWorkersStillAwaitsEveryConsumerCall() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch consumersEntered = new CountDownLatch(2);
        CountDownLatch releaseConsumers = new CountDownLatch(1);
        CountDownLatch consumersExited = new CountDownLatch(2);
        CountDownLatch processReturned = new CountDownLatch(1);
        AtomicInteger activeConsumers = new AtomicInteger();
        AtomicReference<Throwable> terminalFailure = new AtomicReference<>();
        ParallelRangeProcessor<String> processor = processor(2, workers);
        Thread caller = new Thread(() -> {
            try {
                processor.process(source("a\nb\nc\n"), item -> {
                    activeConsumers.incrementAndGet();
                    consumersEntered.countDown();
                    try {
                        if (!releaseConsumers.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("timed out awaiting consumer release");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    } finally {
                        activeConsumers.decrementAndGet();
                        consumersExited.countDown();
                    }
                });
                terminalFailure.set(new AssertionError("process completed normally"));
            } catch (Throwable failure) {
                terminalFailure.set(failure);
            } finally {
                processReturned.countDown();
            }
        }, "parallel-range-process-caller");

        try {
            caller.start();
            assertTrue(consumersEntered.await(5, TimeUnit.SECONDS));

            caller.interrupt();
            assertFalse(processReturned.await(100, TimeUnit.MILLISECONDS));

            releaseConsumers.countDown();
            assertTrue(consumersExited.await(5, TimeUnit.SECONDS));
            assertTrue(processReturned.await(5, TimeUnit.SECONDS));
            caller.join(5000L);

            assertTrue(terminalFailure.get() instanceof InterruptedException);
            assertEquals(0, activeConsumers.get());
        } finally {
            releaseConsumers.countDown();
            caller.interrupt();
            caller.join(5000L);
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static void assertRecords(
            String content,
            int parallelism,
            String... expected
    ) throws Exception {
        List<String> consumed = synchronizedList();

        ParallelProcessingResult result = processor(parallelism, DIRECT_EXECUTOR).process(
                source(content),
                consumed::add
        );

        assertEquals(expected.length, result.getProcessedCount());
        assertSameElements(Arrays.asList(expected), consumed);
    }

    private static ParallelRangeProcessor<String> processor(
            int parallelism,
            Executor executor
    ) {
        return new ParallelRangeProcessor<>(
                parallelism,
                executor,
                ParallelRangeProcessorTest::lineParser,
                RecordDelimiter.newline()
        );
    }

    private static InputParser<String> lineParser() {
        return (input, emitter) -> {
            BufferedReader reader = reader(input);
            String line;
            while ((line = reader.readLine()) != null) {
                emitter.accept(line);
            }
        };
    }

    private static InputParser<String> byteDelimiterParser(byte delimiter) {
        return (input, emitter) -> {
            StringBuilder item = new StringBuilder();
            int value;
            while ((value = input.read()) >= 0) {
                if ((byte) value == delimiter) {
                    emitter.accept(item.toString());
                    item.setLength(0);
                } else {
                    item.append((char) value);
                }
            }
            if (item.length() > 0) {
                emitter.accept(item.toString());
            }
        };
    }

    private static BufferedReader reader(InputStream input) {
        return new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
    }

    private static ByteArrayRangeSource source(String content) {
        return new ByteArrayRangeSource(bytes(content));
    }

    private static byte[] bytes(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }

    private static String repeated(char value, int count) {
        char[] characters = new char[count];
        Arrays.fill(characters, value);
        return new String(characters);
    }

    private static List<String> synchronizedList() {
        return Collections.synchronizedList(new ArrayList<>());
    }

    private static void assertSameElements(List<String> expected, List<String> actual) {
        assertEquals(frequencies(expected), frequencies(actual));
    }

    private static Map<String, Integer> frequencies(List<String> values) {
        Map<String, Integer> frequencies = new HashMap<>();
        synchronized (values) {
            for (String value : values) {
                frequencies.put(value, frequencies.getOrDefault(value, 0) + 1);
            }
        }
        return frequencies;
    }

    private static class ByteArrayRangeSource implements RangeSource {

        protected final byte[] content;
        private final List<RangeCall> openedRanges = Collections.synchronizedList(
                new ArrayList<>()
        );
        private final AtomicInteger openedStreams = new AtomicInteger();
        private final AtomicInteger closedStreams = new AtomicInteger();

        private ByteArrayRangeSource(byte[] content) {
            this.content = content;
        }

        @Override
        public long size() {
            return content.length;
        }

        @Override
        public InputStream openRange(long fromInclusive, long toExclusive) {
            if (fromInclusive < 0L
                    || toExclusive < fromInclusive
                    || toExclusive > content.length) {
                throw new IllegalArgumentException("invalid range");
            }
            int from = (int) fromInclusive;
            int to = (int) toExclusive;
            openedRanges.add(new RangeCall(from, to));
            openedStreams.incrementAndGet();
            return new CloseCountingInputStream(
                    newRangeStream(from, to),
                    closedStreams
            );
        }

        protected InputStream newRangeStream(int fromInclusive, int toExclusive) {
            return new ByteArrayInputStream(
                    content,
                    fromInclusive,
                    toExclusive - fromInclusive
            );
        }

        private List<RangeCall> openedRanges() {
            synchronized (openedRanges) {
                return new ArrayList<>(openedRanges);
            }
        }

        private int openedStreamCount() {
            return openedStreams.get();
        }

        private int closedStreamCount() {
            return closedStreams.get();
        }
    }

    private static final class CloseCountingInputStream extends InputStream {

        private final InputStream delegate;
        private final AtomicInteger closeCount;
        private boolean closed;

        private CloseCountingInputStream(InputStream delegate, AtomicInteger closeCount) {
            this.delegate = delegate;
            this.closeCount = closeCount;
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            return delegate.read(bytes, offset, length);
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                closeCount.incrementAndGet();
            }
            delegate.close();
        }
    }

    private static final class RangeCall {

        private final int fromInclusive;
        private final int toExclusive;

        private RangeCall(int fromInclusive, int toExclusive) {
            this.fromInclusive = fromInclusive;
            this.toExclusive = toExclusive;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof RangeCall)) {
                return false;
            }
            RangeCall that = (RangeCall) other;
            return fromInclusive == that.fromInclusive && toExclusive == that.toExclusive;
        }

        @Override
        public int hashCode() {
            return 31 * fromInclusive + toExclusive;
        }

        @Override
        public String toString() {
            return "[" + fromInclusive + ", " + toExclusive + ")";
        }
    }

    private static final class ReverseBatchExecutor implements Executor {

        private final int expectedTasks;
        private final List<Runnable> tasks = new ArrayList<>();
        private boolean batchExecuted;

        private ReverseBatchExecutor(int expectedTasks) {
            this.expectedTasks = expectedTasks;
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (batchExecuted) {
                command.run();
                return;
            }
            tasks.add(command);
            if (tasks.size() == expectedTasks) {
                batchExecuted = true;
                for (int index = tasks.size() - 1; index >= 0; index--) {
                    tasks.get(index).run();
                }
                tasks.clear();
            }
        }
    }
}
