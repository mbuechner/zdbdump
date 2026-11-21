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

import de.ddb.labs.zdbdump.dump.rdf.RdfDump;
import de.ddb.labs.zdbdump.dump.rdf.RdfDump.XmlContext;
import de.ddb.labs.zdbdump.dump.support.DumpFailure;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryListener;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryState;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.core.retry.Retryable;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Applies fully validated OAI-PMH pages from the DNB ZDB collection to the working records.
 * One dump worker owns the harvester so the upstream backoff deadline survives retries.
 */
public final class OaiHarvester {

    private static final Logger log = LoggerFactory.getLogger(OaiHarvester.class);
    private static final String REPOSITORY_URL = "https://services.dnb.de/oai/repository";
    private static final int MAX_PAGE_ATTEMPTS = 3;
    private static final String OAI_NAMESPACE = "http://www.openarchives.org/OAI/2.0/";
    private static final QName OAI_ROOT = new QName(OAI_NAMESPACE, "OAI-PMH");
    private static final QName LIST_RECORDS = new QName(OAI_NAMESPACE, "ListRecords");
    private static final QName RECORD = new QName(OAI_NAMESPACE, "record");
    private static final QName HEADER = new QName(OAI_NAMESPACE, "header");
    private static final QName DELETION_STATUS = new QName("status");
    private static final QName METADATA = new QName(OAI_NAMESPACE, "metadata");
    private static final QName RDF_DESCRIPTION = new QName(RdfDump.RDF_ROOT.getNamespaceURI(), "Description");
    private static final QName ABOUT = new QName(OAI_NAMESPACE, "about");
    private static final QName ERROR = new QName(OAI_NAMESPACE, "error");
    private static final QName ERROR_CODE = new QName("code");
    private static final QName RESUMPTION_TOKEN = new QName(OAI_NAMESPACE, "resumptionToken");
    private static final Set<String> OAI_ERROR_CODES = Set.of("badArgument", "badResumptionToken", "badVerb",
            "cannotDisseminateFormat", "idDoesNotExist", "noMetadataFormats", "noRecordsMatch", "noSetHierarchy");

    private final RestClient restClient;
    private final RdfDump rdf;
    private final Duration maxWait;
    private final RetryTemplate pageRetry = new RetryTemplate(RetryPolicy.builder()
            .predicate(DumpFailure::isTransientRequest)
            .maxRetries(MAX_PAGE_ATTEMPTS - 1)
            .delay(Duration.ofSeconds(2))
            .multiplier(2)
            .build());
    // The same worker owns this harvester across requests and complete dump attempts.
    private Instant retryNotBefore = Instant.MIN;

    /**
     * Creates a harvester with a shared HTTP client and RDF reader.
     *
     * @param restClient client used for OAI-PMH requests
     * @param rdf reader for the XML response and its RDF payloads
     * @param maxWait longest time a run will wait in place for the upstream deadline
     */
    public OaiHarvester(RestClient restClient, RdfDump rdf, Duration maxWait) {
        this.restClient = restClient;
        this.rdf = rdf;
        this.maxWait = maxWait;
        pageRetry.setRetryListener(new RetryListener() {
            /** Logs the next page attempt without exposing the request's resumption token. */
            @Override
            public void beforeRetry(RetryPolicy policy, Retryable<?> operation, RetryState state) {
                log.warn("Retrying OAI request ({}/{}) after {}",
                        state.getRetryCount() + 1, MAX_PAGE_ATTEMPTS, DumpFailure.describe(state.getLastException()));
            }
        });
    }

    /**
     * Applies all pages of one inclusive UTC window, following opaque resumption tokens after
     * validating each response. Each record replaces the previous value for its RDF subject.
     *
     * @param from earliest UTC timestamp to request
     * @param until latest UTC timestamp to request
     * @param target working map that receives the validated records
     * @return number of RDF records applied, including replacements already present in the map
     * @throws IOException if a page cannot be fetched or parsed, or a resumption token repeats
     */
    public int applyWindow(Instant from, Instant until, Map<String, String> target) throws IOException {
        URI nextPageUri = firstPageUri(from, until);
        final Set<String> seenTokens = new HashSet<>();
        int updates = 0;
        int unmappedDeletions = 0;
        while (true) {
            final int pageNumber = seenTokens.size() + 1;
            final boolean firstPage = pageNumber == 1;
            final String requestUrl = firstPage ? nextPageUri.toASCIIString()
                    : REPOSITORY_URL + "?verb=ListRecords&resumptionToken=[redacted]";
            final String pageContext = "OAI page %d for %s through %s (GET %s)"
                    .formatted(pageNumber, from, until, requestUrl);
            final Page page = requestPage(nextPageUri, firstPage, pageContext);
            final String token = page.nextToken();
            if (token != null && !seenTokens.add(token)) {
                throw new DumpFailure(pageContext + " repeated a resumption token", null);
            }
            target.putAll(page.records());
            updates += page.records().size();
            unmappedDeletions += page.unmappedDeletions();

            if (token == null) {
                break;
            }
            // A resumption token replaces the time range and all other ListRecords arguments.
            nextPageUri = listRecordsUri().queryParam("resumptionToken", "{token}").encode().buildAndExpand(token).toUri();
        }
        if (unmappedDeletions != 0) {
            log.warn("{} OAI deletion headers in the window starting {} lack a ZDB RDF subject; "
                    + "the existing RDF entries could not be removed", unmappedDeletions, from);
        }
        return updates;
    }

    private static URI firstPageUri(Instant from, Instant until) {
        return listRecordsUri()
                .queryParam("metadataPrefix", "RDFxml")
                .queryParam("set", "zdb")
                .queryParam("from", "{from}")
                .queryParam("until", "{until}")
                .encode().buildAndExpand(from.toString(), until.toString()).toUri();
    }

    private static UriComponentsBuilder listRecordsUri() {
        return UriComponentsBuilder.fromUriString(REPOSITORY_URL).queryParam("verb", "ListRecords");
    }

    /** Retries one page before handing the failure to the whole-dump retry policy. */
    private Page requestPage(URI uri, boolean firstPage, String pageContext) throws IOException {
        try {
            return pageRetry.execute(() -> fetchPage(uri, firstPage, pageContext));
        } catch (RetryException e) {
            if (e.getLastException() instanceof DumpFailure failure) {
                throw new DumpFailure(failure.getMessage(), e);
            }
            throw new DumpFailure(pageContext + " failed. " + DumpFailure.describe(e.getLastException()), e);
        }
    }

    /** Captures upstream backoff before checking the status or passing a successful body to the parser. */
    private Page fetchPage(URI uri, boolean firstPage, String pageContext) throws IOException, InterruptedException {
        awaitAvailability();
        try {
            return restClient.get().uri(uri).exchange((request, response) -> {
                if (DumpFailure.isTransientHttpStatus(response.getStatusCode())) {
                    final Instant receivedAt = Instant.now();
                    retryNotBefore = retryAfterDeadline(response.getHeaders(), receivedAt);
                    deferIfTooLong(Duration.between(receivedAt, retryNotBefore));
                }
                DumpFailure.requireSuccessfulStatus(response, uri);
                try {
                    return rdf.readXml(response.getBody(), pageContext, log, xml -> readPage(xml, firstPage, uri));
                } catch (XMLStreamException e) {
                    throw new UncheckedIOException(new IOException(e));
                }
            });
        } catch (RestClientException | UncheckedIOException e) {
            if (e instanceof UncheckedIOException && !DumpFailure.isTransientRequest(e)) {
                throw e;
            }
            throw new DumpFailure("Could not download " + pageContext + ".", e);
        }
    }

    /** Validates the response envelope and accepts {@code noRecordsMatch} only on the first page. */
    private Page readPage(XMLEventReader xml, boolean firstPage, URI documentUri) throws XMLStreamException {
        final var root = new XmlContext(documentUri).descend(RdfDump.requireRoot(xml, OAI_ROOT));
        Page page = null;
        boolean noRecords = false;
        XMLEvent event;
        while ((event = xml.nextTag()).isStartElement()) {
            final StartElement element = event.asStartElement();
            final QName name = element.getName();
            if (name.equals(ERROR)) {
                final String code = RdfDump.attribute(element, ERROR_CODE);
                xml.getElementText();
                if (!firstPage || !"noRecordsMatch".equals(code)) {
                    final String safeCode = code != null && OAI_ERROR_CODES.contains(code) ? code : "unknown code";
                    throw new DumpFailure.OaiProtocol(safeCode, event.getLocation());
                }
                noRecords = true;
            } else if (name.equals(LIST_RECORDS)) {
                if (page != null) {
                    throw new XMLStreamException("Duplicate OAI ListRecords response", event.getLocation());
                }
                page = readListRecords(xml, root.descend(element));
            } else {
                RdfDump.skipElement(xml);
            }
        }
        while (!xml.nextEvent().isEndDocument()) {
            // Validate through the end of the document before the page can affect the working records.
        }
        if (page == null && noRecords) {
            return new Page(Map.of(), null, 0);
        }
        if (page != null && !noRecords) {
            return page;
        }
        throw new XMLStreamException("Expected an OAI ListRecords response or noRecordsMatch");
    }

    /** Collects RDF records and the next token without mutating the working map during parsing. */
    private Page readListRecords(XMLEventReader xml, XmlContext context) throws XMLStreamException {
        final Map<String, String> records = new LinkedHashMap<>();
        String token = null;
        int unmappedDeletions = 0;
        XMLEvent event;
        while ((event = xml.nextTag()).isStartElement()) {
            final StartElement element = event.asStartElement();
            final QName name = element.getName();
            if (name.equals(RESUMPTION_TOKEN)) {
                final String candidate = xml.getElementText();
                token = candidate.isEmpty() ? null : candidate;
            } else if (name.equals(RECORD)) {
                if (readRecord(xml, records, context.descend(element))) {
                    unmappedDeletions++;
                }
            } else {
                throw new XMLStreamException("Unexpected element in OAI ListRecords: " + name, event.getLocation());
            }
        }
        return new Page(records, token, unmappedDeletions);
    }

    /** Applies any supplied RDF and returns whether a deletion header lacked metadata. */
    private boolean readRecord(XMLEventReader xml, Map<String, String> records, XmlContext context)
            throws XMLStreamException {
        final XMLEvent header = xml.nextTag();
        if (!RdfDump.isStartElement(header, HEADER)) {
            throw new XMLStreamException("Expected an OAI record header", header.getLocation());
        }
        final boolean deleted = "deleted".equals(RdfDump.attribute(header.asStartElement(), DELETION_STATUS));
        RdfDump.skipElement(xml);
        XMLEvent next = xml.nextTag();
        final boolean hasMetadata = RdfDump.isStartElement(next, METADATA);
        if (deleted && !hasMetadata) {
            requireRecordEnd(next);
            return true;
        }

        if (!hasMetadata) {
            throw new XMLStreamException("Expected metadata for an active OAI record", next.getLocation());
        }
        // DNB can send RDF on a deleted record. Apply that metadata as the previous harvester did.
        readMetadata(xml, records, context.descend(next.asStartElement()));
        next = xml.nextTag();
        while (RdfDump.isStartElement(next, ABOUT)) {
            RdfDump.skipElement(xml);
            next = xml.nextTag();
        }
        requireRecordEnd(next);
        return false;
    }

    private static void requireRecordEnd(XMLEvent event) throws XMLStreamException {
        if (!event.isEndElement() || !event.asEndElement().getName().equals(RECORD)) {
            throw new XMLStreamException("Unexpected element after OAI record data", event.getLocation());
        }
    }

    /** Accepts both a wrapped RDF graph and a single description without the optional RDF wrapper. */
    private void readMetadata(XMLEventReader xml, Map<String, String> records, XmlContext context)
            throws XMLStreamException {
        final XMLEvent root = xml.nextTag();
        if (RdfDump.isStartElement(root, RDF_DESCRIPTION)) {
            rdf.readRecord(xml, root.asStartElement(), context, records);
        } else if (RdfDump.isStartElement(root, RdfDump.RDF_ROOT)) {
            readWrappedDescriptions(xml, records, context.descend(root.asStartElement()));
        } else {
            throw new XMLStreamException("Expected RDF metadata", root.getLocation());
        }
        final XMLEvent end = xml.nextTag();
        if (!end.isEndElement() || !end.asEndElement().getName().equals(METADATA)) {
            throw new XMLStreamException("Expected closing OAI metadata element", end.getLocation());
        }
    }

    /** Only descriptions are ZDB updates. Typed RDF nodes in the wrapper must not replace them. */
    private void readWrappedDescriptions(XMLEventReader xml, Map<String, String> records, XmlContext root)
            throws XMLStreamException {
        final var contexts = new ArrayDeque<XmlContext>();
        contexts.push(root);
        while (!contexts.isEmpty()) {
            final XMLEvent event = xml.nextEvent();
            if (RdfDump.isStartElement(event, RDF_DESCRIPTION)) {
                // Reading a description consumes its nested nodes as part of the same record.
                rdf.readRecord(xml, event.asStartElement(), contexts.peek(), records);
            } else if (event.isStartElement()) {
                contexts.push(contexts.peek().descend(event.asStartElement()));
            } else if (event.isEndElement()) {
                contexts.pop();
            }
        }
    }

    /**
     * Honors the last upstream Retry-After deadline across page requests and whole-dump attempts.
     * A longer wait defers the run so the worker becomes available again.
     *
     * @throws InterruptedException if the worker is interrupted while waiting or before the request
     * @throws DumpFailure.HarvestDeferred if the remaining wait exceeds the configured limit
     */
    public void awaitAvailability() throws InterruptedException {
        DumpFailure.checkInterrupted();
        Duration remaining = Duration.between(Instant.now(), retryNotBefore);
        deferIfTooLong(remaining);
        while (remaining.isPositive()) {
            log.info("Waiting until {} before contacting OAI again as requested by Retry-After", retryNotBefore);
            Thread.sleep(remaining);
            remaining = Duration.between(Instant.now(), retryNotBefore);
        }
        retryNotBefore = Instant.MIN;
    }

    private void deferIfTooLong(Duration remaining) {
        if (remaining.compareTo(maxWait) > 0) {
            throw new DumpFailure.HarvestDeferred(retryNotBefore);
        }
    }

    /** Converts a delay in seconds or an HTTP date to a deadline. Missing or invalid values yield {@link Instant#MIN}. */
    static Instant retryAfterDeadline(HttpHeaders headers, Instant receivedAt) {
        final String value = headers.getFirst(HttpHeaders.RETRY_AFTER);
        if (value == null || value.isBlank()) {
            return Instant.MIN;
        }
        final String trimmed = value.trim();
        if (trimmed.chars().allMatch(c -> c >= '0' && c <= '9')) {
            try {
                return receivedAt.plusSeconds(Long.parseLong(trimmed));
            } catch (NumberFormatException | ArithmeticException | DateTimeException e) {
                return Instant.MIN;
            }
        }
        try {
            return Instant.ofEpochMilli(headers.getFirstDate(HttpHeaders.RETRY_AFTER));
        } catch (IllegalArgumentException e) {
            return Instant.MIN;
        }
    }

    /** Holds the records and continuation state found in an OAI response. */
    private record Page(Map<String, String> records, String nextToken, int unmappedDeletions) {}
}
