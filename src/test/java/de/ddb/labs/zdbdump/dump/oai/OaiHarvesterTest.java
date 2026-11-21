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
package de.ddb.labs.zdbdump.dump.oai;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.ddb.labs.zdbdump.dump.rdf.RdfDump;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertPathBuilderException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RestClient;

/** Verifies metadata parsing, page validation and interpretation of Retry-After headers. */
class OaiHarvesterTest {

    private static final Instant FROM = Instant.parse("2026-11-20T15:44:00Z");
    private static final Instant UNTIL = Instant.parse("2026-11-20T15:45:00Z");

    @Test
    void appliesDescriptionWithoutAnRdfWrapper() throws IOException {
        final String response = page(description("new title").replace("https://ld.zdb-services.de/resource/1", "1"), "")
                .replace("<metadata>", "<metadata xml:base=\"https://ld.zdb-services.de/resource/\">");
        final OaiHarvester harvester = harvester(response);
        final Map<String, String> found = new HashMap<>(Map.of("1", "old title", "2", "untouched"));

        assertEquals(1, harvester.applyWindow(FROM, UNTIL, found));
        assertEquals(2, found.size());
        assertTrue(found.get("1").contains("new title"));
        assertEquals("untouched", found.get("2"));
    }

    @Test
    void appliesRdfMetadataReturnedWithADeletedHeader() throws IOException {
        final String metadata = """
                <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
                  %s
                </rdf:RDF>
                """.formatted(description("replacement title"));
        final String response = page(metadata, "").replace("<header>", "<header status=\"deleted\">");
        final Map<String, String> found = new HashMap<>(Map.of("1", "old title", "2", "untouched"));

        assertEquals(1, harvester(response).applyWindow(FROM, UNTIL, found));
        assertTrue(found.get("1").contains("replacement title"));
        assertEquals("untouched", found.get("2"));
    }

    @Test
    void preservesXmlContextAcrossOaiAndRdfEnvelopes() throws IOException {
        final String metadata = """
                <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xml:base="../">
                  <rdf:Description rdf:about="1">
                    <dc:title xmlns:dc="http://purl.org/dc/elements/1.1/">Inherited language</dc:title>
                    <dc:relation xmlns:dc="http://purl.org/dc/elements/1.1/" rdf:resource="2"/>
                  </rdf:Description>
                </rdf:RDF>
                """;
        final String response = page(metadata, "")
                .replace("<OAI-PMH xmlns=", "<OAI-PMH xml:base=\"https://ld.zdb-services.de/catalog/\" xmlns=")
                .replace("<ListRecords>", "<ListRecords xml:base=\"../resource/\">")
                .replace("<record>", "<record xml:lang=\"de\">")
                .replace("<metadata>", "<metadata xml:base=\"versions/\">");
        final Map<String, String> found = new HashMap<>(Map.of("1", "previous"));

        assertEquals(1, harvester(response).applyWindow(FROM, UNTIL, found));
        assertEquals(1, found.size());
        assertTrue(found.get("1").contains("xml:base=\"https://ld.zdb-services.de/resource/\""));
        assertTrue(found.get("1").contains("xml:lang=\"de\""));
        assertTrue(found.get("1").contains("rdf:resource=\"2\""));
    }

    @Test
    void followsWhitespaceOnlyResumptionTokenExactly() throws IOException {
        final List<URI> requests = new ArrayList<>();
        final OaiHarvester harvester = harvester(requests,
                page(description("first page"), "<resumptionToken>  &#9; </resumptionToken>"),
                page(description("second page"), "<resumptionToken/>"));
        final Map<String, String> found = new HashMap<>();

        assertEquals(2, harvester.applyWindow(FROM, UNTIL, found));
        assertTrue(found.get("1").contains("second page"));
        assertEquals("verb=ListRecords&resumptionToken=%20%20%09%20", requests.get(1).getRawQuery());
    }

    @Test
    void redactsTheContinuationTokenInRetryAndFailureMessages() {
        final String token = "opaque-page-token";
        final String upstreamDetail = "Confidential upstream exception text";
        final List<URI> requests = new ArrayList<>();
        final RestClient client = RestClient.builder()
                .requestInterceptor((request, body, execution) -> {
                    requests.add(request.getURI());
                    if (requests.size() == 1) {
                        return response(page(description("first page"),
                                "<resumptionToken>" + token + "</resumptionToken>"));
                    }
                    if (requests.size() == 2) {
                        throw new SocketTimeoutException(upstreamDetail);
                    }
                    final var tlsFailure = new SSLHandshakeException(upstreamDetail);
                    tlsFailure.initCause(new CertPathBuilderException(upstreamDetail));
                    throw tlsFailure;
                }).build();
        final OaiHarvester harvester = new OaiHarvester(client, new RdfDump(), Duration.ofSeconds(1));
        final Logger logger = (Logger) LoggerFactory.getLogger(OaiHarvester.class);
        final var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);

        try {
            final IOException failure = assertThrows(IOException.class,
                    () -> harvester.applyWindow(FROM, UNTIL, new HashMap<>()));
            final String retry = appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                    .filter(message -> message.startsWith("Retrying OAI request"))
                    .findFirst().orElseThrow();
            final String message = "Could not download OAI page 2 for " + FROM + " through " + UNTIL
                    + " (GET https://services.dnb.de/oai/repository?verb=ListRecords&resumptionToken=[redacted]).";
            assertEquals("Retrying OAI request (2/3) after " + message, retry);
            assertEquals(message, failure.getMessage());
            assertEquals(3, requests.size());
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void includesTheFirstPageRequestWhenXmlParsingFails() {
        final List<URI> requests = new ArrayList<>();
        final String invalid = page(description("a title"), "").replace("</metadata>", "</unexpected>");
        final OaiHarvester harvester = harvester(requests, invalid);

        final IOException failure = assertThrows(IOException.class,
                () -> harvester.applyWindow(FROM, UNTIL, new HashMap<>()));

        assertEquals(1, requests.size());
        assertTrue(failure.getMessage().contains("(GET " + requests.getFirst().toASCIIString() + ")"));
        assertTrue(failure.getMessage().contains("Invalid XML at line"));
    }

    @Test
    void typedNodesDoNotOverwriteDescriptionsForTheSameSubject() throws IOException {
        final String metadata = """
                <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                         xmlns:example="https://example.test/schema#">
                  %s
                  <example:Publication rdf:about="https://ld.zdb-services.de/resource/1">
                    <example:subtitle>Typed node for the same subject</example:subtitle>
                  </example:Publication>
                </rdf:RDF>
                """.formatted(description("new title"));
        final OaiHarvester harvester = harvester(page(metadata, ""));
        final Map<String, String> found = new HashMap<>();

        assertEquals(1, harvester.applyWindow(FROM, UNTIL, found));
        assertTrue(found.get("1").contains("new title"));
    }

    @Test
    void repeatedTokenRejectsThePageBeforeApplyingItsRecords() {
        final String token = "<resumptionToken>same-token</resumptionToken>";
        final OaiHarvester harvester = harvester(page(description("first page"), token),
                page(description("rejected page"), token));
        final Map<String, String> found = new HashMap<>();

        final IOException failure = assertThrows(IOException.class, () -> harvester.applyWindow(FROM, UNTIL, found));
        assertTrue(failure.getMessage().contains("repeated a resumption token"));
        assertTrue(found.get("1").contains("first page"));
    }

    @Test
    void interpretsRetryAfterInSecondsAndHttpDate() {
        final Instant received = Instant.parse("2026-11-20T15:45:01Z");
        final HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.RETRY_AFTER, "45");
        assertEquals(received.plusSeconds(45), OaiHarvester.retryAfterDeadline(headers, received));

        headers.set(HttpHeaders.RETRY_AFTER, "Fri, 20 Nov 2026 15:59:00 GMT");
        assertEquals(Instant.parse("2026-11-20T15:59:00Z"),
                OaiHarvester.retryAfterDeadline(headers, received));

        headers.set(HttpHeaders.RETRY_AFTER, "invalid");
        assertEquals(Instant.MIN, OaiHarvester.retryAfterDeadline(headers, received));
    }

    private static String description(String title) {
        return """
                <rdf:Description xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                 rdf:about="https://ld.zdb-services.de/resource/1">
                  <dc:title xmlns:dc="http://purl.org/dc/elements/1.1/">%s</dc:title>
                </rdf:Description>
                """.formatted(title);
    }

    private static String page(String metadata, String token) {
        return """
                <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
                  <responseDate>2026-11-20T15:45:01Z</responseDate>
                  <request verb="ListRecords">https://services.dnb.de/oai/repository</request>
                  <ListRecords>
                    <record>
                      <header><identifier>oai:dnb.de/zdb/27</identifier><datestamp>2026-11-20T15:44:30Z</datestamp></header>
                      <metadata>%s</metadata>
                    </record>%s
                  </ListRecords>
                </OAI-PMH>
                """.formatted(metadata, token);
    }

    private static OaiHarvester harvester(String... pages) {
        return harvester(new ArrayList<>(), pages);
    }

    private static OaiHarvester harvester(List<URI> requests, String... pages) {
        final var responses = new ArrayDeque<>(List.of(pages));
        final RestClient client = RestClient.builder()
                .requestInterceptor((request, body, execution) -> {
                    requests.add(request.getURI());
                    return response(responses.remove());
                }).build();
        return new OaiHarvester(client, new RdfDump(), Duration.ofSeconds(1));
    }

    private static ClientHttpResponse response(String xml) {
        return new ClientHttpResponse() {
            @Override
            public HttpStatus getStatusCode() {
                return HttpStatus.OK;
            }

            @Override
            public String getStatusText() {
                return HttpStatus.OK.getReasonPhrase();
            }

            @Override
            public void close() {
            }

            @Override
            public InputStream getBody() {
                return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public HttpHeaders getHeaders() {
                return HttpHeaders.EMPTY;
            }
        };
    }
}
