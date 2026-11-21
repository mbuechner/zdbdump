package de.ddb.labs.zdbdump.dump.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import javax.net.ssl.SSLHandshakeException;
import javax.xml.stream.XMLStreamException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;

/** Tests diagnostics, retry and cancellation for wrapped HTTP and streaming failures. */
class DumpFailureTest {

    @Test
    void retriesTimeoutReportedThroughSeparateStaxLink() {
        final var failure = new IOException(withNestedException(new SocketTimeoutException()));

        assertTrue(DumpFailure.isTransientRequest(failure));
        assertTrue(DumpFailure.hasCause(failure, SocketTimeoutException.class));
        assertFalse(DumpFailure.isTransientRequest(new XMLStreamException("Malformed XML")));
    }

    @Test
    void reportsOnlyTheArchiveUrlAndKeepsTheOriginalCause() {
        final var original = new ResourceAccessException("Raw client details",
                new SSLHandshakeException("Private certificate details"));
        final var failure = DumpFailure.downloadFailure("the DNB archive index",
                URI.create("https://data.dnb.de/opendata/"), original);

        assertEquals("Could not download the DNB archive index from https://data.dnb.de/opendata/.",
                DumpFailure.describe(failure));
        assertSame(original, failure.getCause());
        assertFalse(DumpFailure.isTransientRequest(failure));
    }

    @Test
    void logsOnlyThePublicPartOfTheEndpointAndKeepsTheHttpStatusForRetry() {
        final var original = new RestClientResponseException("Private response details", HttpStatus.SERVICE_UNAVAILABLE,
                "Untrusted reason phrase", null, null, null);
        final var failure = DumpFailure.downloadFailure("the OAI page",
                URI.create("https://user:password@services.dnb.de:8443/oai/repository?resumptionToken=private#fragment"), original);

        assertEquals("Could not download the OAI page from https://services.dnb.de:8443/oai/repository.",
                DumpFailure.describe(failure));
        assertTrue(DumpFailure.isTransientRequest(failure));
    }

    @Test
    void givesNestedCancellationPriorityOverNetworkErrors() {
        final var failure = new SocketTimeoutException();
        failure.initCause(withNestedException(new InterruptedException()));

        assertTrue(DumpFailure.hasCause(failure, InterruptedException.class));
        assertFalse(DumpFailure.isTransientRequest(failure));
    }

    @Test
    @Timeout(5)
    void handlesOverlappingAndCyclicCauseLinks() {
        final var timeout = new SocketTimeoutException();
        final var stream = new XMLStreamException(timeout);
        timeout.initCause(stream);

        assertFalse(DumpFailure.hasCause(stream, InterruptedException.class));
        assertTrue(DumpFailure.isTransientRequest(stream));
    }

    /** Models a StAX failure whose nested exception is separate from its ordinary cause. */
    private static XMLStreamException withNestedException(Throwable staxCause) {
        return new XMLStreamException("Failed to read XML", new IOException()) {
            @Override
            public Throwable getNestedException() {
                return staxCause;
            }
        };
    }
}
