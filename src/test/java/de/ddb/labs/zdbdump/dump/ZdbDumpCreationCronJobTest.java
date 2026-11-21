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
package de.ddb.labs.zdbdump.dump;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.ddb.labs.zdbdump.dump.support.DumpFailure;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.security.cert.CertPathBuilderException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.SSLHandshakeException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RestClient;

/** Exercises the complete dump workflow using local files and simulated upstream responses. */
class ZdbDumpCreationCronJobTest {

    private static final String FILENAME = "dump.rdf.gz";
    private static final URI DNB_DIRECTORY = URI.create("https://data.dnb.de/opendata/");
    private static final String ARCHIVE_ETAG = "\"test-archive\"";
    private static final String RDF = """
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:t="urn:test">%s</rdf:RDF>""";
    private static final String OAI = """
            <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
            <responseDate>2026-11-20T12:00:00Z</responseDate><request verb="ListRecords">https://example.test</request>%s
            </OAI-PMH>""";

    @TempDir
    Path workspace;

    @Test
    void publishesAdjacentRecordsAndAllHarvestPagesUsingUtcAndAnEncodedToken() throws Exception {
        final Instant modified = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(120);
        final String opaqueToken = "a+ &=%/?{}?";
        final byte[] base = gzip(RDF.formatted(description("1", "old") + description("2", "unchanged")));
        final String firstPage = OAI.formatted("<ListRecords>" + oaiRecord(description("1", "updated")
                + description("3", "third")) + "<resumptionToken>a+ &amp;=%/?{}?</resumptionToken></ListRecords>");
        final String nextPage = OAI.formatted("<ListRecords>" + oaiRecord(description("4", "last"))
                + "<record><header status=\"deleted\"><identifier>oai:dnb.de/zdb/123</identifier></header></record>"
                + "<resumptionToken/></ListRecords>");
        Files.createDirectories(outputPath());
        final Path publicFile = Files.write(outputPath().resolve(FILENAME), gzip(RDF.formatted(description("5", "prior dump"))));
        final boolean posix = Files.getFileStore(outputPath()).supportsFileAttributeView(PosixFileAttributeView.class);
        final var regularFilePermissions = posix ? Files.getPosixFilePermissions(publicFile) : null;
        final List<URI> requests = new ArrayList<>();
        final RestClient client = catalogClient(modified, (request, body, execution) -> {
            requests.add(request.getURI());
            if (request.getURI().getHost().equals("data.dnb.de")) {
                return response(modified, base);
            }
            final String query = request.getURI().getRawQuery();
            if (query.contains("resumptionToken")) {
                assertEquals("verb=ListRecords&resumptionToken=" + opaqueToken,
                        URLDecoder.decode(query, StandardCharsets.UTF_8));
                assertTrue(query.contains("%2B"));
                assertTrue(query.contains("%26"));
                return xmlResponse(nextPage);
            }
            assertTrue(request.getURI().getQuery().contains("from=" + modified));
            return xmlResponse(firstPage);
        });

        try (MVStore store = new MVStore.Builder().open()) {
            job(store, client).createDump();
        }

        final String result = publishedDump();
        assertTrue(result.contains("updated"));
        assertFalse(result.contains(">old<"));
        assertFalse(result.contains("prior dump"));
        assertTrue(result.contains("unchanged"));
        assertTrue(result.contains("third"));
        assertTrue(result.contains("last"));
        assertEquals(3, requests.size());
        if (posix) {
            assertEquals(regularFilePermissions, Files.getPosixFilePermissions(publicFile));
        }
    }

    @Test
    void keepsHttpAndHttpsRdfSubjectsDistinctWhenApplyingAnUpdate() throws Exception {
        final Instant modified = Instant.now().minusSeconds(120);
        final String httpRecord = description("1", "http record").replace("https:", "http:");
        final byte[] base = gzip(RDF.formatted(httpRecord + description("1", "old https record")));
        final String update = oaiRecord(description("1", "updated https record"));
        final RestClient client = catalogClient(modified, (request, body, execution) -> {
            if (request.getURI().getHost().equals("data.dnb.de")) {
                return response(modified, request.getMethod() == HttpMethod.HEAD ? new byte[0] : base);
            }
            return xmlResponse(OAI.formatted("<ListRecords>" + update + "</ListRecords>"));
        });

        try (MVStore store = new MVStore.Builder().open()) {
            job(store, client).createDump();
        }

        final String published = publishedDump();
        assertTrue(published.contains("rdf:about=\"http://ld.zdb-services.de/resource/1\""));
        assertTrue(published.contains("rdf:about=\"https://ld.zdb-services.de/resource/1\""));
        assertTrue(published.contains(">http record<"));
        assertTrue(published.contains(">updated https record<"));
        assertFalse(published.contains(">old https record<"));
    }

    @Test
    void malformedDownloadLeavesThePreviouslyPublishedDumpAndCacheUntouched() throws Exception {
        final byte[] published = gzip(RDF.formatted(description("1", "previously published")));
        Files.createDirectories(outputPath());
        Files.write(outputPath().resolve(FILENAME), published);
        final Instant modified = Instant.now().minusSeconds(120);
        final String incomplete = RDF.formatted(description("1", "partial")).replace("</rdf:RDF>", "");
        final byte[] corrupt = gzip(incomplete);
        final RestClient client = catalogClient(modified, (request, body, execution) ->
                response(modified, request.getMethod() == HttpMethod.HEAD ? new byte[0] : corrupt));

        try (MVStore store = new MVStore.Builder().open()) {
            assertThrows(XMLStreamException.class, () -> job(store, client).createDump());
        }

        assertTrue(publishedDump().contains("previously published"));
        assertFalse(Files.exists(tempPath().resolve(ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME)));
        try (var stagingFiles = Files.list(tempPath())) {
            assertEquals(0, stagingFiles.count());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {200, 403, 405, 501, 503})
    void usesCatalogDataStateWhenHeadHasNoUsableValidator(int headStatus) throws Exception {
        final Instant dataState = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(120);
        final Instant httpLastModified = dataState.plusSeconds(3600);
        final byte[] base = gzip(RDF.formatted(description("1", "base archive")));
        prepareCachedArchive(dataState, gzip(RDF.formatted(description("1", "cached archive"))));
        final RestClient client = catalogClient(dataState, (request, body, execution) -> {
            if (request.getMethod() == HttpMethod.HEAD) {
                return response(HttpStatus.valueOf(headStatus), new HttpHeaders(), new byte[0]);
            }
            if (request.getURI().getHost().equals("data.dnb.de")) {
                return response(httpLastModified, base);
            }
            assertTrue(request.getURI().getQuery().contains("from=" + dataState));
            final String update = oaiRecord(description("1", "harvested update"));
            return xmlResponse(OAI.formatted("<ListRecords>" + update + "</ListRecords>"));
        });

        try (MVStore store = new MVStore.Builder().open()) {
            job(store, client).createDump();
        }

        assertTrue(publishedDump().contains(">harvested update<"));
        assertEquals(List.of(archiveUri(dataState).toString(), dataState.toString(), ARCHIVE_ETAG),
                Files.readAllLines(tempPath().resolve(ZdbDumpCreationCronJob.BASE_DUMP_METADATA_FILENAME)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stopsAfterAnInterruptedHeadWithoutDownloadingTheArchive(boolean interruptFlag) throws Exception {
        final Instant dataState = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(120);
        prepareCachedArchive(dataState, gzip(RDF.formatted(description("1", "cached archive"))));
        final RestClient client = catalogClient(dataState, (request, body, execution) -> {
            assertEquals(HttpMethod.HEAD, request.getMethod());
            if (interruptFlag) {
                Thread.currentThread().interrupt();
                return response(HttpStatus.SERVICE_UNAVAILABLE, new HttpHeaders(), new byte[0]);
            }
            throw new IOException("Interrupted HEAD request", new InterruptedException());
        });

        try (MVStore store = new MVStore.Builder().open()) {
            final var job = job(store, client);
            try {
                job.run();
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
        }
        assertFalse(Files.exists(outputPath().resolve(FILENAME)));
    }

    @Test
    @Timeout(5)
    void logsOnlyTheArchiveIndexDownloadWhenTheScheduledRunStops() throws Exception {
        final byte[] published = gzip(RDF.formatted(description("1", "previously published")));
        Files.createDirectories(outputPath());
        Files.write(outputPath().resolve(FILENAME), published);
        final AtomicInteger requests = new AtomicInteger();
        final RestClient client = RestClient.builder().requestInterceptor((request, body, execution) -> {
            assertEquals(DNB_DIRECTORY, request.getURI());
            requests.incrementAndGet();
            final var failure = new SSLHandshakeException("private handshake details");
            failure.initCause(new CertPathBuilderException("private certificate details"));
            throw failure;
        }).build();

        final var logger = (Logger) LoggerFactory.getLogger(ZdbDumpCreationCronJob.class);
        final var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        try (MVStore store = new MVStore.Builder().open()) {
            job(store, client).run();
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }

        final List<String> errors = appender.list.stream().filter(event -> event.getLevel() == Level.ERROR)
                .map(ILoggingEvent::getFormattedMessage).toList();
        assertEquals(1, errors.size());
        final String error = errors.getFirst();
        assertTrue(error.endsWith("Could not download the DNB archive index from " + DNB_DIRECTORY + "."), error);
        assertEquals(1, requests.get());
        assertTrue(publishedDump().contains("previously published"));
    }

    @Test
    void identifiesTheSelectedArchiveAndPreservesItsHttpStatusForRetry() throws Exception {
        final Instant dataState = Instant.now().minusSeconds(120);
        final RestClient client = catalogClient(dataState, (request, body, execution) -> {
            assertEquals(archiveUri(dataState), request.getURI());
            assertEquals(HttpMethod.GET, request.getMethod());
            return response(HttpStatus.SERVICE_UNAVAILABLE, new HttpHeaders(), new byte[0]);
        });

        try (MVStore store = new MVStore.Builder().open()) {
            final var failure = assertThrows(DumpFailure.class, () -> job(store, client).createDump());
            assertEquals("Could not download the DNB RDF/XML dump from " + archiveUri(dataState) + ".",
                    failure.getMessage());
            assertTrue(DumpFailure.isTransientRequest(failure));
        }
        try (var stagingFiles = Files.list(tempPath())) {
            assertEquals(0, stagingFiles.count());
        }
    }

    @Test
    void keepsThePublishedDumpWhenLatestCatalogEntryHasNoDataState() throws Exception {
        Files.createDirectories(outputPath());
        Files.write(outputPath().resolve(FILENAME), gzip(RDF.formatted(description("1", "previously published"))));
        final RestClient client = RestClient.builder().requestInterceptor((request, body, execution) -> {
            assertEquals(DNB_DIRECTORY, request.getURI());
            return xmlResponse("""
                    <a href="zdb_lds_20251110.rdf.gz">zdb_lds_20251110.rdf.gz</a> (Stand: 10.11.2025 07:10 UTC)
                    <a href="zdb_lds_20261110.rdf.gz">zdb_lds_20261110.rdf.gz</a> ZDB RDF/XML
                    """);
        }).build();

        try (MVStore store = new MVStore.Builder().open()) {
            final var failure = assertThrows(DumpFailure.class, () -> job(store, client).createDump());
            assertTrue(failure.getMessage().contains("no catalog data state"));
        }
        assertTrue(publishedDump().contains("previously published"));
    }

    @Test
    void doesNotCacheDownloadedBytesUnderTheHeadValidator() throws Exception {
        final Instant dataState = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(120);
        final byte[] cached = gzip(RDF.formatted(description("1", "previous base")));
        prepareCachedArchive(dataState, cached);
        final byte[] download = gzip(RDF.formatted(description("1", "verified download")));
        final RestClient client = catalogClient(dataState, (request, body, execution) -> {
            if (request.getURI().equals(archiveUri(dataState))) {
                final var headers = new HttpHeaders();
                if (request.getMethod() == HttpMethod.HEAD) {
                    headers.set(HttpHeaders.ETAG, "\"new-validator\"");
                    return response(HttpStatus.OK, headers, new byte[0]);
                }
                return response(HttpStatus.OK, headers, download);
            }
            return xmlResponse(OAI.formatted("<error code=\"noRecordsMatch\">Empty</error>"));
        });

        try (MVStore store = new MVStore.Builder().open()) {
            job(store, client).createDump();
        }

        assertTrue(publishedDump().contains("verified download"));
        assertEquals(List.of(archiveUri(dataState).toString(), dataState.toString(), ARCHIVE_ETAG),
                Files.readAllLines(tempPath().resolve(ZdbDumpCreationCronJob.BASE_DUMP_METADATA_FILENAME)));
        try (var input = new GZIPInputStream(Files.newInputStream(
                tempPath().resolve(ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME)))) {
            assertTrue(new String(input.readAllBytes(), StandardCharsets.UTF_8).contains("previous base"));
        }
    }

    @Test
    void protocolErrorsDoNotReplaceThePublishedDump() throws Exception {
        final byte[] published = gzip(RDF.formatted(description("1", "previously published")));
        Files.createDirectories(outputPath());
        Files.write(outputPath().resolve(FILENAME), published);
        final Instant modified = Instant.now().minusSeconds(120);
        final byte[] base = gzip(RDF.formatted(description("1", "base")));
        final RestClient client = catalogClient(modified, (request, body, execution) -> {
            if (request.getURI().getHost().equals("data.dnb.de")) {
                return response(modified, request.getMethod() == HttpMethod.HEAD ? new byte[0] : base);
            }
            return xmlResponse(OAI.formatted("<error code=\"badArgument\">Invalid request</error>"));
        });

        try (MVStore store = new MVStore.Builder().open()) {
            final var failure = assertThrows(IOException.class, () -> job(store, client).createDump());
            assertTrue(failure.getMessage().contains("OAI page 1"));
            assertTrue(failure.getMessage().contains("badArgument"));
            assertFalse(failure.getMessage().contains("Invalid request"));
        }

        assertTrue(publishedDump().contains("previously published"));
    }

    @Test
    void refreshesTheLegacyCacheAndThenReusesTheExactValidatedArchive() throws Exception {
        final Instant modified = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(120);
        final byte[] base = gzip(RDF.formatted(description("1", "dated edition")));
        Files.createDirectories(tempPath());
        Files.write(tempPath().resolve(ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME),
                gzip(RDF.formatted(description("1", "legacy cache"))));
        Files.writeString(tempPath().resolve(ZdbDumpCreationCronJob.BASE_DUMP_METADATA_FILENAME),
                DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(modified.atOffset(ZoneOffset.UTC)));
        final List<HttpMethod> archiveRequests = new ArrayList<>();
        final RestClient client = catalogClient(modified, (request, body, execution) -> {
            if (request.getURI().getHost().equals("data.dnb.de")) {
                archiveRequests.add(request.getMethod());
                return response(modified, request.getMethod() == HttpMethod.HEAD ? new byte[0] : base);
            }
            return xmlResponse(OAI.formatted("<error code=\"noRecordsMatch\">Empty</error>"));
        });

        try (MVStore store = new MVStore.Builder().open()) {
            final var generator = job(store, client);
            generator.createDump();
            generator.createDump();
        }

        assertEquals(List.of(HttpMethod.GET, HttpMethod.HEAD), archiveRequests);
        assertTrue(publishedDump().contains("dated edition"));
    }


    @ParameterizedTest
    @ValueSource(strings = {
            "<record/>",
            "<record><header/></record>",
            "<record><header/><header/><metadata/></record>",
            "<record><header status=\"deleted\"/><metadata/></record>"
    })
    void malformedOaiRecordDoesNotReplaceThePublishedDump(String record) throws Exception {
        Files.createDirectories(outputPath());
        Files.write(outputPath().resolve(FILENAME), gzip(RDF.formatted(description("1", "prior dump"))));
        final Instant modified = Instant.now().minusSeconds(120);
        final byte[] base = gzip(RDF.formatted(description("1", "base")));
        final RestClient client = catalogClient(modified, (request, body, execution) -> {
            if (request.getURI().getHost().equals("data.dnb.de")) {
                return response(modified, request.getMethod() == HttpMethod.HEAD ? new byte[0] : base);
            }
            return xmlResponse(OAI.formatted("<ListRecords>" + record + "</ListRecords>"));
        });

        try (MVStore store = new MVStore.Builder().open()) {
            final var failure = assertThrows(IOException.class, () -> job(store, client).createDump());
            assertTrue(failure.getMessage().contains("Invalid XML at line"));
        }
        assertTrue(publishedDump().contains("prior dump"));
    }

    @Test
    void preservesCharacterReferenceWhitespaceInPublishedRdf() throws Exception {
        final String record = """
                <rdf:Description rdf:about="https://ld.zdb-services.de/resource/1">
                  <t:title t:attribute="a&#9;b&#10;c&#13;d">one&#13;two</t:title>
                  <?fixture content="line
                tab	end"?><!-- Comment with a "quoted
                line". -->
                </rdf:Description>
                """;
        final Instant modified = Instant.now().minusSeconds(120);
        final byte[] base = gzip(RDF.formatted(record));
        final String update = oaiRecord(record.replace("/1", "/2").replace("one", "three"));
        final RestClient client = catalogClient(modified, (request, body, execution) -> {
            if (request.getURI().getHost().equals("data.dnb.de")) {
                return response(modified, request.getMethod() == HttpMethod.HEAD ? new byte[0] : base);
            }
            return xmlResponse(OAI.formatted("<ListRecords>" + update + "</ListRecords>"));
        });

        try (MVStore store = new MVStore.Builder().open()) {
            job(store, client).createDump();
        }

        final String published = publishedDump();
        final List<String> titles = new ArrayList<>();
        final XMLStreamReader xml = XMLInputFactory.newDefaultFactory().createXMLStreamReader(new StringReader(published));
        try {
            while (xml.hasNext()) {
                xml.next();
                if (xml.isStartElement() && xml.getLocalName().equals("title")) {
                    assertEquals("a\tb\nc\rd", xml.getAttributeValue("urn:test", "attribute"));
                    titles.add(xml.getElementText());
                }
            }
        } finally {
            xml.close();
        }
        assertEquals(List.of("one\rtwo", "three\rtwo"), titles);
        assertTrue(published.contains("<?fixture content=\"line\ntab\tend\"?>"));
        assertTrue(published.contains("<!-- Comment with a \"quoted\nline\". -->"));
    }

    @Test
    @Timeout(5)
    void permanentHttpFailureEndsTheRunWithoutRetryingAndReleasesTheJob() throws Exception {
        final AtomicInteger requests = new AtomicInteger();
        final RestClient client = RestClient.builder().requestInterceptor((request, body, execution) -> {
            requests.incrementAndGet();
            return response(HttpStatus.NOT_FOUND, new HttpHeaders(), new byte[0]);
        }).build();

        try (MVStore store = new MVStore.Builder().open()) {
            final var job = job(store, client);
            job.run();
            assertEquals(1, requests.get());
            job.run();
            assertEquals(2, requests.get());
        }
    }

    @Test
    @Timeout(10)
    void transientOaiFailureRetriesTheSamePageAndPublishesOnSuccess() throws Exception {
        final Instant modified = Instant.now().minusSeconds(120);
        final byte[] base = gzip(RDF.formatted(description("1", "base")));
        final List<URI> harvestRequests = new ArrayList<>();
        final RestClient client = catalogClient(modified, (request, body, execution) -> {
            if (request.getURI().getHost().equals("data.dnb.de")) {
                return response(modified, request.getMethod() == HttpMethod.HEAD ? new byte[0] : base);
            }
            harvestRequests.add(request.getURI());
            if (harvestRequests.size() == 1) {
                return response(HttpStatus.SERVICE_UNAVAILABLE, new HttpHeaders(), new byte[0]);
            }
            return xmlResponse(OAI.formatted("<ListRecords>" + oaiRecord(description("1", "retried")) + "</ListRecords>"));
        });

        try (MVStore store = new MVStore.Builder().open()) {
            job(store, client).createDump();
        }
        assertEquals(2, harvestRequests.size());
        assertEquals(harvestRequests.getFirst(), harvestRequests.getLast());
        assertTrue(publishedDump().contains("retried"));
    }

    @Test
    void publishesVerifiedDumpEvenWhenTheOptionalCacheCannotBeUpdated() throws Exception {
        final Path metadata = Files.createDirectories(tempPath().resolve(ZdbDumpCreationCronJob.BASE_DUMP_METADATA_FILENAME));
        final Path marker = Files.writeString(metadata.resolve("existing"), "keep");
        final Instant modified = Instant.now().minusSeconds(120);
        final byte[] base = gzip(RDF.formatted(description("1", "verified base")));
        final RestClient client = catalogClient(modified, (request, body, execution) -> {
            if (request.getURI().getHost().equals("data.dnb.de")) {
                return response(modified, request.getMethod() == HttpMethod.HEAD ? new byte[0] : base);
            }
            return xmlResponse(OAI.formatted("<error code=\"noRecordsMatch\">Empty</error>"));
        });

        try (MVStore store = new MVStore.Builder().open()) {
            job(store, client).createDump();
        }
        assertTrue(publishedDump().contains("verified base"));
        assertEquals("keep", Files.readString(marker));
        try (var files = Files.list(tempPath())) {
            assertEquals(List.of(metadata), files.toList());
        }
    }

    @Test
    @Timeout(5)
    void longRetryAfterDefersFurtherRequestsAndPreservesThePublishedDump() throws Exception {
        Files.createDirectories(outputPath());
        Files.write(outputPath().resolve(FILENAME), gzip(RDF.formatted(description("1", "prior dump"))));
        final Instant modified = Instant.now().minusSeconds(120);
        final byte[] base = gzip(RDF.formatted(description("1", "base")));
        final AtomicInteger requests = new AtomicInteger();
        final RestClient client = catalogClient(modified, (request, body, execution) -> {
            requests.incrementAndGet();
            if (request.getURI().getHost().equals("data.dnb.de")) {
                return response(modified, request.getMethod() == HttpMethod.HEAD ? new byte[0] : base);
            }
            final var headers = new HttpHeaders();
            headers.set(HttpHeaders.RETRY_AFTER, "3600");
            return response(HttpStatus.TOO_MANY_REQUESTS, headers, new byte[0]);
        });

        try (MVStore store = new MVStore.Builder().open()) {
            final var job = job(store, client);
            job.run();
            assertEquals(2, requests.get());
            job.run();
            assertEquals(2, requests.get());
        }
        assertTrue(publishedDump().contains("prior dump"));
    }

    @Test
    void interruptionStopsBeforeNetworkAccessAndPreservesInterruptStatus() throws IOException {
        final RestClient client = RestClient.builder().requestInterceptor((request, body, execution) -> {
            throw new AssertionError("An interrupted dump run must not contact the upstream service");
        }).build();

        try (MVStore store = new MVStore.Builder().open()) {
            final var job = job(store, client);
            try {
                Thread.currentThread().interrupt();
                job.run();
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
        }
    }

    private ZdbDumpCreationCronJob job(MVStore store, RestClient client) throws IOException {
        return new ZdbDumpCreationCronJob(store.openMap("test"), client,
                DumpPaths.prepare(outputPath().toString(), tempPath().toString(), FILENAME, "test.mv.db"));
    }

    private Path outputPath() {
        return workspace.resolve("output");
    }

    private Path tempPath() {
        return workspace.resolve("cache");
    }

    /** Simulates the directory listing separately from the selected archive and OAI responses. */
    private static RestClient catalogClient(Instant dataState, ClientHttpRequestInterceptor interceptor) {
        return RestClient.builder().requestInterceptor((request, body, execution) -> {
            if (request.getURI().equals(DNB_DIRECTORY)) {
                final String filename = archiveUri(dataState).getPath().substring("/opendata/".length());
                final String date = DateTimeFormatter.ofPattern("dd.MM.uuuu HH:mm:ss")
                        .withZone(ZoneOffset.UTC).format(dataState);
                return xmlResponse("""
                        <a href="zdb_lds.rdf.gz">zdb_lds.rdf.gz</a> Stable link to current archive
                        <a href="%1$s">%1$s</a> 2026-11-12 15:04 391M RDF/XML (Stand: %2$s UTC)
                        """.formatted(filename, date));
            }
            if (request.getURI().getHost().equals("data.dnb.de")) {
                assertEquals(archiveUri(dataState), request.getURI());
            }
            return interceptor.intercept(request, body, execution);
        }).build();
    }

    private static URI archiveUri(Instant dataState) {
        final String date = DateTimeFormatter.BASIC_ISO_DATE.format(dataState.atOffset(ZoneOffset.UTC).toLocalDate());
        return DNB_DIRECTORY.resolve("zdb_lds_" + date + ".rdf.gz");
    }

    private void prepareCachedArchive(Instant dataState, byte[] base) throws IOException {
        Files.createDirectories(tempPath());
        Files.write(tempPath().resolve(ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME), base);
        Files.write(tempPath().resolve(ZdbDumpCreationCronJob.BASE_DUMP_METADATA_FILENAME),
                List.of(archiveUri(dataState).toString(), dataState.toString(), ARCHIVE_ETAG));
    }

    private String publishedDump() throws IOException {
        try (InputStream stream = new GZIPInputStream(Files.newInputStream(outputPath().resolve(FILENAME)))) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String description(String id, String title) {
        return "<rdf:Description rdf:about=\"https://ld.zdb-services.de/resource/" + id
                + "\"><t:title>" + title + "</t:title></rdf:Description>";
    }

    private static String oaiRecord(String descriptions) {
        return "<record><header><identifier>oai:dnb.de/zdb/123</identifier></header><metadata>"
                + RDF.formatted(descriptions) + "</metadata></record>";
    }

    private static byte[] gzip(String value) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream compressed = new GZIPOutputStream(bytes)) {
            compressed.write(value.getBytes(StandardCharsets.UTF_8));
        }
        return bytes.toByteArray();
    }

    private static ClientHttpResponse xmlResponse(String body) {
        return response(null, body.getBytes(StandardCharsets.UTF_8));
    }

    private static ClientHttpResponse response(Instant lastModified, byte[] body) {
        final HttpHeaders headers = new HttpHeaders();
        if (lastModified != null) {
            headers.setLastModified(lastModified.toEpochMilli());
            headers.set(HttpHeaders.ETAG, ARCHIVE_ETAG);
        }
        return response(HttpStatus.OK, headers, body);
    }

    private static ClientHttpResponse response(HttpStatus status, HttpHeaders headers, byte[] body) {
        final InputStream stream = new ByteArrayInputStream(body);
        return new ClientHttpResponse() {
            /** {@inheritDoc} */
            @Override
            public HttpStatus getStatusCode() {
                return status;
            }

            /** {@inheritDoc} */
            @Override
            public String getStatusText() {
                return status.getReasonPhrase();
            }

            /** {@inheritDoc} */
            @Override
            public HttpHeaders getHeaders() {
                return headers;
            }

            /** {@inheritDoc} */
            @Override
            public InputStream getBody() {
                return stream;
            }

            /** {@inheritDoc} */
            @Override
            public void close() {}
        };
    }
}
