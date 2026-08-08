package io.github.jutil.parallelrangeprocessor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileRangeSourceTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void reportsSizeAndReadsOnlyRequestedHalfOpenRange() throws IOException {
        Path file = temporaryDirectory.resolve("data.txt");
        Files.write(file, "0123456789".getBytes(StandardCharsets.UTF_8));
        FileRangeSource source = new FileRangeSource(file);

        assertEquals(10L, source.size());
        try (InputStream input = source.openRange(2L, 7L)) {
            assertArrayEquals("23456".getBytes(StandardCharsets.UTF_8), readAll(input));
            assertEquals(-1, input.read());
        }
    }

    @Test
    void emptyRangeProducesEmptyStream() throws IOException {
        Path file = temporaryDirectory.resolve("data.txt");
        Files.write(file, "abc".getBytes(StandardCharsets.UTF_8));
        FileRangeSource source = new FileRangeSource(file);

        try (InputStream input = source.openRange(2L, 2L)) {
            assertEquals(-1, input.read());
        }
    }

    @Test
    void invalidRangesAreRejected() throws IOException {
        Path file = temporaryDirectory.resolve("data.txt");
        Files.write(file, "abc".getBytes(StandardCharsets.UTF_8));
        FileRangeSource source = new FileRangeSource(file);

        assertThrows(IllegalArgumentException.class, () -> source.openRange(-1L, 1L));
        assertThrows(IllegalArgumentException.class, () -> source.openRange(0L, -1L));
        assertThrows(IllegalArgumentException.class, () -> source.openRange(2L, 1L));
        assertThrows(IllegalArgumentException.class, () -> source.openRange(0L, 4L));
    }

    @Test
    void nullPathIsRejected() {
        assertThrows(NullPointerException.class, () -> new FileRangeSource(null));
    }

    private static byte[] readAll(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[32];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }
}
