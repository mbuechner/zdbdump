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
package de.ddb.labs.zdbdump.dump.support;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.zip.ZipException;
import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamException;
import org.slf4j.Logger;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;

/**
 * A dump failure with a message safe for application logs. The shared helpers classify upstream
 * failures, preserve cancellation, and report failed staging cleanup.
 */
public final class DumpFailure extends IOException {

    /**
     * Creates a failure with an operator-facing explanation.
     *
     * @param reason message suitable for logs, with upstream payloads and tokens removed
     * @param cause original exception, or {@code null} for a failed validation
     */
    public DumpFailure(String reason, Throwable cause) {
        super(reason, cause);
    }

    /**
     * Reports the unavailable resource and its public URL. The original cause remains available to the retry policy.
     *
     * @param resource the name of the content being downloaded
     * @param uri the HTTP endpoint, whose path is public
     * @param cause the failure reported by the client or response handler
     * @return a failure safe to summarize in application logs
     */
    public static DumpFailure downloadFailure(String resource, URI uri, Throwable cause) {
        final String port = uri.getPort() < 0 ? "" : ":" + uri.getPort();
        final String endpoint = uri.getScheme() + "://" + uri.getHost() + port + uri.getRawPath();
        return new DumpFailure("Could not download %s from %s.".formatted(resource, endpoint), cause);
    }

    /**
     * Stops processing non-successful upstream responses before the body reaches an XML parser.
     * The exception retains the HTTP status and headers for the retry policy.
     *
     * @param response upstream response to check
     * @param uri request URI, used for its host only
     * @throws IOException if the status cannot be read
     * @throws RestClientResponseException if the upstream response has a non-2xx status
     */
    public static void requireSuccessfulStatus(ClientHttpResponse response, URI uri) throws IOException {
        final HttpStatusCode status = response.getStatusCode();
        if (!status.is2xxSuccessful()) {
            throw new RestClientResponseException(
                    "Request to " + uri.getHost() + " failed",
                    status, "", response.getHeaders(), null, null);
        }
    }

    /**
     * Converts the thread's interrupt flag into an exception so the current operation can unwind.
     * The outer dump runner restores the flag after it handles cancellation.
     *
     * @throws InterruptedException if the worker thread was interrupted
     */
    public static void checkInterrupted() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("Dump generation interrupted");
        }
    }

    /**
     * Classifies upstream HTTP responses for retry.
     *
     * @param status HTTP status returned by the upstream service
     * @return whether the status indicates rate limiting or a server error
     */
    public static boolean isTransientHttpStatus(HttpStatusCode status) {
        return status.value() == 429 || status.is5xxServerError();
    }

    /**
     * Checks wrapped failures at both the page and whole-dump retry boundaries. Thread interruption
     * and a deferred harvest end the current run.
     *
     * @param failure exception reported by the request or its caller
     * @return whether the failure warrants an automatic retry
     */
    public static boolean isTransientRequest(Throwable failure) {
        if (Thread.currentThread().isInterrupted()) {
            return false;
        }
        final List<Throwable> causes = causesOf(failure);
        if (causes.stream().anyMatch(cause -> cause instanceof InterruptedException || cause instanceof HarvestDeferred)) {
            return false;
        }
        final boolean requestFailure = causes.stream().anyMatch(ResourceAccessException.class::isInstance);
        for (Throwable cause : causes) {
            if (cause instanceof RestClientResponseException response) {
                return isTransientHttpStatus(response.getStatusCode());
            }
            if (cause instanceof SocketException || cause instanceof SocketTimeoutException
                    || cause instanceof HttpTimeoutException || cause instanceof UnknownHostException
                    || (cause instanceof EOFException && requestFailure)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks an exception and its causes for the specified type.
     *
     * @param failure outermost exception, or {@code null} when absent
     * @param type exception type to find
     * @return whether the chain contains an instance of {@code type}
     */
    public static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        return causesOf(failure).stream().anyMatch(type::isInstance);
    }

    /** Follows ordinary causes first, then any separate StAX nested exception, at most once per instance. */
    private static List<Throwable> causesOf(Throwable failure) {
        if (failure == null) {
            return List.of();
        }
        final List<Throwable> causes = new ArrayList<>();
        final var pending = new ArrayDeque<Throwable>();
        final var visited = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        pending.add(failure);
        while (!pending.isEmpty()) {
            final Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            causes.add(current);
            final Throwable cause = current.getCause();
            if (current instanceof XMLStreamException xml) {
                final Throwable nested = xml.getNestedException();
                if (nested != null && nested != cause) {
                    pending.addFirst(nested);
                }
            }
            if (cause != null) {
                pending.addFirst(cause);
            }
        }
        return causes;
    }

    /**
     * Summarizes known failures without including upstream response bodies or resumption tokens.
     *
     * @param failure exception to summarize, or {@code null} when details are unavailable
     * @return diagnostic message suitable for an application log
     */
    public static String describe(Throwable failure) {
        String fallback = null;
        for (Throwable cause : causesOf(failure)) {
            if (cause instanceof DumpFailure || cause instanceof HarvestDeferred) {
                return cause.getMessage();
            }
            if (cause instanceof OaiProtocol protocol) {
                return "The upstream repository returned an OAI error (" + protocol.code + ")";
            }
            if (cause instanceof FileSystemException file) {
                return file.getClass().getSimpleName() + " for " + file.getFile();
            }
            if (cause instanceof ZipException) {
                return "Incomplete or invalid compressed input (ZipException)";
            }
            if (cause instanceof EOFException) {
                return "The input ended unexpectedly (EOFException)";
            }
            if (fallback == null && cause instanceof XMLStreamException xml) {
                final var location = xml.getLocation();
                fallback = location == null || location.getLineNumber() < 0 ? "Invalid XML"
                        : "Invalid XML at line %d, column %d".formatted(location.getLineNumber(), location.getColumnNumber());
            } else if (fallback == null && cause instanceof IOException && cause.getCause() == null) {
                fallback = "I/O failed (" + cause.getClass().getSimpleName() + ")";
            }
        }
        if (fallback != null) {
            return fallback;
        }
        return failure == null ? "No failure details available" : failure.getClass().getSimpleName();
    }

    /**
     * Removes a staged file if present. A cleanup failure is logged so any original error can propagate.
     *
     * @param file temporary file to discard
     * @param log logger associated with the staging operation
     */
    public static void discardStage(Path file, Logger log) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            log.warn("Could not remove staging file {}. {}", file, describe(e));
        }
    }

    /** Signals that the upstream OAI deadline is too far away to keep the worker waiting. */
    public static final class HarvestDeferred extends RuntimeException {
        /**
         * Records the earliest time the service can contact the OAI repository again.
         *
         * @param until upstream deadline derived from Retry-After
         */
        public HarvestDeferred(Instant until) {
            super("Retry-After prevents OAI requests until " + until);
        }
    }

    /** Represents an OAI error response whose code is safe for diagnostic logs. */
    public static final class OaiProtocol extends XMLStreamException {
        /** Normalized protocol code included in diagnostic messages. */
        private final String code;

        /**
         * Records the normalized OAI error code and where it occurred in the response.
         *
         * @param safeCode known protocol code, or a local fallback for an unrecognized code
         * @param location parser location of the OAI error element
         */
        public OaiProtocol(String safeCode, Location location) {
            super("OAI protocol error", location);
            this.code = safeCode;
        }
    }
}
