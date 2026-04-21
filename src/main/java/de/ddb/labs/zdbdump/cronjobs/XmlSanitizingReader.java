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
package de.ddb.labs.zdbdump.cronjobs;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import org.slf4j.Logger;

final class XmlSanitizingReader extends Reader {

    private static final int MAX_ENTITY_LOOKAHEAD = 32;
    private static final int RECENT_CONTEXT_LIMIT = 240;

    private final PushbackReader delegate;
    private final String sourceDescription;
    private final Logger log;
    private final StringBuilder recentContext = new StringBuilder();
    private String pending = "";
    private int pendingIndex = 0;
    private int line = 1;
    private int column = 0;
    private String lastMalformedEntity = null;
    private int lastMalformedEntityLine = -1;
    private int lastMalformedEntityColumn = -1;

    XmlSanitizingReader(Reader delegate, String sourceDescription, Logger log) {
        this.delegate = new PushbackReader(delegate, MAX_ENTITY_LOOKAHEAD);
        this.sourceDescription = sourceDescription;
        this.log = log;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int charsRead = 0;
        while (charsRead < len) {
            final int nextChar = read();
            if (nextChar == -1) {
                return charsRead == 0 ? -1 : charsRead;
            }
            cbuf[off + charsRead] = (char) nextChar;
            charsRead++;
        }
        return charsRead;
    }

    @Override
    public int read() throws IOException {
        if (pendingIndex < pending.length()) {
            final char pendingChar = pending.charAt(pendingIndex++);
            updatePosition(pendingChar);
            return pendingChar;
        }

        pending = "";
        pendingIndex = 0;

        final int current = delegate.read();
        if (current == -1) {
            return -1;
        }
        if (current != '&') {
            updatePosition((char) current);
            return current;
        }

        final int entityLine = line;
        final int entityColumn = column + 1;
        final StringBuilder candidate = new StringBuilder();
        while (candidate.length() < MAX_ENTITY_LOOKAHEAD) {
            final int next = delegate.read();
            if (next == -1) {
                break;
            }
            final char nextChar = (char) next;
            if (nextChar == ';') {
                candidate.append(nextChar);
                break;
            }
            if (Character.isLetterOrDigit(nextChar) || nextChar == '#' || nextChar == 'x' || nextChar == 'X') {
                candidate.append(nextChar);
                continue;
            }
            delegate.unread(next);
            break;
        }

        final String entity = candidate.toString();
        if (isValidXmlEntity(entity)) {
            pending = entity;
        } else {
            lastMalformedEntity = entity;
            lastMalformedEntityLine = entityLine;
            lastMalformedEntityColumn = entityColumn;
            log.warn(
                    "Recovered malformed XML entity in {} at line {}, column {}: '&{}'",
                    sourceDescription,
                    entityLine,
                    entityColumn,
                    abbreviate(entity));
            pending = "amp;" + entity;
        }
        updatePosition('&');
        return '&';
    }

    private void updatePosition(char currentChar) {
        appendRecentContext(currentChar);
        if (currentChar == '\n') {
            line++;
            column = 0;
        } else {
            column++;
        }
    }

    String getRecentContext() {
        return abbreviate(escapeControlCharacters(recentContext.toString()));
    }

    String getLastMalformedEntitySummary() {
        if (lastMalformedEntity == null) {
            return null;
        }
        return "lastRecoveredEntity='&" + abbreviate(lastMalformedEntity) + "' at line "
                + lastMalformedEntityLine + ", column " + lastMalformedEntityColumn;
    }

    private void appendRecentContext(char currentChar) {
        recentContext.append(currentChar);
        if (recentContext.length() > RECENT_CONTEXT_LIMIT) {
            recentContext.delete(0, recentContext.length() - RECENT_CONTEXT_LIMIT);
        }
    }

    private String abbreviate(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value.length() > 80 ? value.substring(0, 80) + "..." : value;
    }

    private String escapeControlCharacters(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }

    private static boolean isValidXmlEntity(String entity) {
        return entity.matches("(amp;|lt;|gt;|apos;|quot;|#\\d+;|#x[0-9A-Fa-f]+;)");
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
