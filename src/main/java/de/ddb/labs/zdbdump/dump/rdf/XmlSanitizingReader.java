/*
 * Copyright 2023-2026 Michael Büchner, Deutsche Digitale Bibliothek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.ddb.labs.zdbdump.dump.rdf;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.util.Objects;

/**
 * Repairs malformed ampersand sequences in upstream XML before StAX parses it.
 *
 * <p>The reader passes through the five predefined XML entities and valid numeric
 * character references. For other sequences it escapes the opening ampersand,
 * so StAX reads text such as {@code &unknown;} as literal content. The candidate
 * buffer holds at most 32 characters, while numeric leading zeros are counted and streamed.
 *
 * <p>Comments, CDATA sections and processing instructions pass through verbatim
 * because character references have no special meaning there. StAX validates
 * the document structure and controls DTD and external entity handling.
 */
final class XmlSanitizingReader extends Reader {

    private static final int MAX_ENTITY_LOOKAHEAD = 32;

    private final PushbackReader delegate;
    private String pending = "";
    private int pendingIndex;
    private int insertedLength;
    private int pendingZeroIndex;
    private long pendingZeros;
    private String literalEnd;
    private int literalOpeningRemaining;
    private int literalMatch;
    private long line = 1;
    private long column;
    private boolean previousWasCarriageReturn;
    private boolean beginning = true;
    private long recoveredEntities;
    private long firstRecoveryLine = -1;
    private long firstRecoveryColumn = -1;

    /**
     * Wraps the decoded XML source with bounded lookahead.
     *
     * @param delegate the reader to sanitize and close
     */
    XmlSanitizingReader(Reader delegate) {
        this.delegate = new PushbackReader(delegate, MAX_ENTITY_LOOKAHEAD);
    }

    /**
     * Reads sanitized characters into the requested range of the destination array.
     *
     * @param destination the array that receives the characters
     * @param offset the starting position in the array
     * @param length the maximum number of characters to read
     * @return the number of characters read, or {@code -1} at the end of input if no characters were read
     * @throws IOException if the source cannot be read
     * @throws IndexOutOfBoundsException if the requested destination range is invalid
     */
    @Override
    public int read(char[] destination, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, destination.length);
        for (int count = 0; count < length; count++) {
            final int current = read();
            if (current == -1) {
                return count == 0 ? -1 : count;
            }
            destination[offset + count] = (char) current;
        }
        return length;
    }

    /**
     * Reads one sanitized character, first consuming any buffered expansion from an entity repair.
     *
     * @return the next character, or {@code -1} at the end of input
     * @throws IOException if the source cannot be read
     */
    @Override
    public int read() throws IOException {
        if (pendingZeros > 0 && pendingIndex == pendingZeroIndex) {
            pendingZeros--;
            advanceSourcePosition('0');
            return '0';
        }
        if (pendingIndex < pending.length()) {
            final int index = pendingIndex++;
            final char current = pending.charAt(index);
            // Only original source characters advance the diagnostic position.
            if (index >= insertedLength) {
                advanceSourcePosition(current);
            }
            return current;
        }

        int next = delegate.read();
        if (beginning) {
            beginning = false;
            if (next == '\uFEFF') {
                next = delegate.read();
            }
        }
        if (next == -1) {
            return -1;
        }
        final char current = (char) next;
        if (literalOpeningRemaining > 0) {
            literalOpeningRemaining--;
        } else if (literalEnd != null) {
            advanceLiteral(current);
        } else if (current == '<') {
            detectLiteral();
        } else if (current == '&') {
            readEntity();
        }
        advanceSourcePosition(current);
        return current;
    }

    /** Detects literal markup and passes its opening delimiter through before looking for the end. */
    private void detectLiteral() throws IOException {
        final int marker = delegate.read();
        if (marker == '?') {
            literalEnd = "?>";
            literalOpeningRemaining = 1;
        } else if (marker == '!') {
            final int type = delegate.read();
            if (type == '-' && follows("-")) {
                literalEnd = "-->";
                literalOpeningRemaining = "!--".length();
            } else if (type == '[' && follows("CDATA[")) {
                literalEnd = "]]>";
                literalOpeningRemaining = "![CDATA[".length();
            }
            if (type != -1) {
                delegate.unread(type);
            }
        }
        if (marker != -1) {
            delegate.unread(marker);
        }
        literalMatch = 0;
    }

    /** Looks for a markup delimiter and restores the lookahead for normal reading. */
    private boolean follows(String expected) throws IOException {
        final char[] readAhead = new char[expected.length()];
        int count = 0;
        while (count < readAhead.length) {
            final int next = delegate.read();
            if (next == -1) {
                break;
            }
            readAhead[count] = (char) next;
            if (readAhead[count] != expected.charAt(count++)) {
                delegate.unread(readAhead, 0, count);
                return false;
            }
        }
        delegate.unread(readAhead, 0, count);
        return count == readAhead.length;
    }

    /** Recognizes the end delimiter of the current literal region across successive reads. */
    private void advanceLiteral(char current) {
        if (current == literalEnd.charAt(literalMatch)) {
            if (++literalMatch == literalEnd.length()) {
                literalEnd = null;
                literalMatch = 0;
            }
        } else if (current != literalEnd.charAt(0)) {
            literalMatch = 0;
        }
    }

    /**
     * Buffers the characters after an ampersand. The caller emits the ampersand,
     * and a buffered {@code amp;} prefix makes it literal when recovery is needed.
     */
    private void readEntity() throws IOException {
        final StringBuilder candidate = new StringBuilder();
        pendingZeros = readNumericPrefix(candidate);
        final int numericPrefixLength = candidate.length();
        while (candidate.length() < MAX_ENTITY_LOOKAHEAD) {
            final int next = delegate.read();
            if (next == -1) {
                break;
            }
            if (next == ';') {
                candidate.append(';');
                break;
            }
            if (!Character.isLetterOrDigit(next) && next != '#') {
                delegate.unread(next);
                break;
            }
            candidate.append((char) next);
        }
        final String entity = candidate.toString();
        pendingIndex = 0;
        if (isValidXmlEntity(entity)) {
            pending = entity;
            insertedLength = 0;
        } else {
            if (++recoveredEntities == 1) {
                firstRecoveryLine = line;
                firstRecoveryColumn = column + 1;
            }
            pending = "amp;" + entity;
            insertedLength = 4;
        }
        pendingZeroIndex = insertedLength + numericPrefixLength;
    }

    /** Buffers the numeric marker and counts leading zeros without allocating space for them. */
    private long readNumericPrefix(StringBuilder candidate) throws IOException {
        int next = delegate.read();
        long zeros = 0;
        if (next == '#') {
            candidate.append('#');
            next = delegate.read();
            if (next == 'x') {
                candidate.append('x');
                next = delegate.read();
            }
            while (next == '0') {
                zeros++;
                next = delegate.read();
            }
        }
        if (next != -1) {
            delegate.unread(next);
        }
        return zeros;
    }

    /** Advances the original source position. A carriage return followed by a line feed counts once. */
    private void advanceSourcePosition(char current) {
        if (current == '\r' || current == '\n') {
            if (current == '\r' || !previousWasCarriageReturn) {
                line++;
            }
            column = 0;
        } else {
            column++;
        }
        previousWasCarriageReturn = current == '\r';
    }

    /**
     * Returns the number of repairs so far and the one-based location of the first
     * repaired ampersand in the decoded source. A leading BOM is skipped and CRLF
     * counts as one line break. Both position fields are {@code -1} before any repair.
     *
     * @return a snapshot of the recovery statistics
     */
    Recoveries recoveries() {
        return new Recoveries(recoveredEntities, firstRecoveryLine, firstRecoveryColumn);
    }

    /**
     * Tracks recovery diagnostics in the original decoded XML.
     *
     * @param count the number of repaired ampersands
     * @param firstLine the one-based line of the first repair, or {@code -1} if there was no repair
     * @param firstColumn the one-based column of the first repair, or {@code -1} if there was no repair
     */
    record Recoveries(long count, long firstLine, long firstColumn) {}

    private static boolean isValidXmlEntity(String entity) {
        return switch (entity) {
            case "amp;", "lt;", "gt;", "apos;", "quot;" -> true;
            default -> isValidCharacterReference(entity);
        };
    }

    /** Checks the reference syntax and the range of characters permitted by XML 1.0. */
    private static boolean isValidCharacterReference(String entity) {
        if (!entity.startsWith("#") || !entity.endsWith(";")) {
            return false;
        }
        final boolean hexadecimal = entity.startsWith("#x");
        final int start = hexadecimal ? 2 : 1;
        final int radix = hexadecimal ? 16 : 10;
        int codePoint = 0;
        for (int index = start; index < entity.length() - 1; index++) {
            final char current = entity.charAt(index);
            final int digit = Character.digit(current, radix);
            if (current > 127 || digit < 0) {
                return false;
            }
            codePoint = codePoint * radix + digit;
            if (codePoint > Character.MAX_CODE_POINT) {
                return false;
            }
        }
        return codePoint == 0x9 || codePoint == 0xA || codePoint == 0xD
                || (codePoint >= 0x20 && codePoint <= 0xD7FF)
                || (codePoint >= 0xE000 && codePoint <= 0xFFFD)
                || (codePoint >= 0x10000 && codePoint <= Character.MAX_CODE_POINT);
    }

    /**
     * Closes the underlying XML source.
     *
     * @throws IOException if the source cannot be closed
     */
    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
