package io.github.jutil.parallelrangeprocessor;

/**
 * Immutable single-byte record-delimiter configuration.
 *
 * <p>V1 deliberately supports only a delimiter that can be recognized from
 * one byte without format-specific or stateful parsing. The delimiter byte is
 * included in every delimiter-terminated stream passed to the configured
 * parser. This preserves empty records between consecutive delimiters and lets
 * parsers observe the original record bytes.</p>
 */
public final class RecordDelimiter {

    private static final RecordDelimiter NEWLINE = new RecordDelimiter((byte) '\n');

    private final byte value;

    private RecordDelimiter(byte value) {
        this.value = value;
    }

    /**
     * Returns a delimiter for the line-feed byte ({@code 0x0A}).
     *
     * <p>This also frames CRLF input because the carriage return remains part
     * of the record bytes immediately before the line feed.</p>
     *
     * @return the shared newline delimiter
     */
    public static RecordDelimiter newline() {
        return NEWLINE;
    }

    /**
     * Returns a delimiter for {@code value}.
     *
     * @param value the one-byte record delimiter
     * @return an immutable delimiter configuration
     */
    public static RecordDelimiter singleByte(byte value) {
        if (value == '\n') {
            return NEWLINE;
        }
        return new RecordDelimiter(value);
    }

    byte value() {
        return value;
    }
}
