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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ZdbDumpCreationCronJob {

    private static final Logger log = LoggerFactory.getLogger(ZdbDumpCreationCronJob.class);
    private static final int PROGRESS_LOG_STEP = 100_000;
    private static final int HARVEST_RETRY_ATTEMPTS = 3;
    private static final long HARVEST_RETRY_DELAY_MILLIS = 2_000L;

    private final static String DUMP_URL = "https://data.dnb.de/opendata/zdb_lds.rdf.gz";
    private final static String HARVEST_URL = "https://services.dnb.de/oai/repository?verb=ListRecords&metadataPrefix=RDFxml&set=zdb";
    private final static String HARVEST_WITH_RESUMPTION_TOKEN_URL = "https://services.dnb.de/oai/repository?verb=ListRecords&resumptionToken=";

    @Value("${zdbdump.path.output}")
    private String outputPath;

    @Value("${zdbdump.path.temp}")
    private String tempPath;

    @Value("${zdbdump.output.filename}")
    private String outputFilename;

    @Autowired
    private MVMap<String, String> mvStoreZdbData;

    @Autowired
    private RestClient restClient;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public boolean isRunning() {
        return isRunning.get();
    }

    public void setRunning(boolean running) {
        isRunning.set(running);
    }

    private final XMLInputFactory xif = XMLInputFactory.newInstance();

    private final XMLEventFactory xmlEventFactory = XMLEventFactory.newFactory();

    private int dumpReadCount = 0;
    private int harvestUpdateCount = 0;
    private int outputWriteCount = 0;

    private final List<Namespace> nsl = createNamespaces();

    private List<Namespace> createNamespaces() {
        final List<Namespace> namespaces = new ArrayList<>();
        namespaces.add(xmlEventFactory.createNamespace("schema", "http://schema.org/"));
        namespaces.add(xmlEventFactory.createNamespace("gndo", "https://d-nb.info/standards/elementset/gnd#"));
        namespaces.add(xmlEventFactory.createNamespace("lib", "http://purl.org/library/"));
        namespaces.add(xmlEventFactory.createNamespace("owl", "http://www.w3.org/2002/07/owl#"));
        namespaces.add(xmlEventFactory.createNamespace("xsd", "http://www.w3.org/2001/XMLSchema#"));
        namespaces.add(xmlEventFactory.createNamespace("skos", "http://www.w3.org/2004/02/skos/core#"));
        namespaces.add(xmlEventFactory.createNamespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#"));
        namespaces.add(xmlEventFactory.createNamespace("editeur", "https://ns.editeur.org/thema/"));
        namespaces.add(xmlEventFactory.createNamespace("geo", "http://www.opengis.net/ont/geosparql#"));
        namespaces.add(xmlEventFactory.createNamespace("umbel", "http://umbel.org/umbel#"));
        namespaces.add(xmlEventFactory.createNamespace("rdau", "http://rdaregistry.info/Elements/u/"));
        namespaces.add(xmlEventFactory.createNamespace("sf", "http://www.opengis.net/ont/sf#"));
        namespaces.add(xmlEventFactory.createNamespace("bflc", "http://id.loc.gov/ontologies/bflc/"));
        namespaces.add(xmlEventFactory.createNamespace("dcterms", "http://purl.org/dc/terms/"));
        namespaces.add(xmlEventFactory.createNamespace("vivo", "http://vivoweb.org/ontology/core#"));
        namespaces.add(xmlEventFactory.createNamespace("isbd", "http://iflastandards.info/ns/isbd/elements/"));
        namespaces.add(xmlEventFactory.createNamespace("foaf", "http://xmlns.com/foaf/0.1/"));
        namespaces.add(xmlEventFactory.createNamespace("mo", "http://purl.org/ontology/mo/"));
        namespaces.add(xmlEventFactory.createNamespace("marcRole", "http://id.loc.gov/vocabulary/relators/"));
        namespaces.add(xmlEventFactory.createNamespace("agrelon", "https://d-nb.info/standards/elementset/agrelon#"));
        namespaces.add(xmlEventFactory.createNamespace("dcmitype", "http://purl.org/dc/dcmitype/"));
        namespaces.add(xmlEventFactory.createNamespace("dbp", "http://dbpedia.org/property/"));
        namespaces.add(xmlEventFactory.createNamespace("dnbt", "https://d-nb.info/standards/elementset/dnb#"));
        namespaces.add(xmlEventFactory.createNamespace("madsrdf", "http://www.loc.gov/mads/rdf/v1#"));
        namespaces.add(xmlEventFactory.createNamespace("dnb_intern", "http://dnb.de/"));
        namespaces.add(xmlEventFactory.createNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"));
        namespaces.add(xmlEventFactory.createNamespace("v", "http://www.w3.org/2006/vcard/ns#"));
        namespaces.add(xmlEventFactory.createNamespace("wdrs", "http://www.w3.org/2007/05/powder-s#"));
        namespaces.add(xmlEventFactory.createNamespace("ebu", "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#"));
        namespaces.add(xmlEventFactory.createNamespace("bibo", "http://purl.org/ontology/bibo/"));
        namespaces.add(xmlEventFactory.createNamespace("gbv", "http://purl.org/ontology/gbv/"));
        namespaces.add(xmlEventFactory.createNamespace("dc", "http://purl.org/dc/elements/1.1/"));
        return namespaces;
    }

    @Scheduled(cron = "${zdbdump.cron.job}")
    @Retryable(retryFor = {
            Exception.class }, maxAttemptsExpression = "5", backoff = @Backoff(delayExpression = "600000"))
    public void run() {

        if (!isRunning.compareAndSet(false, true)) {
            log.info("ZDB/RDF dump creation already running. Abort.");
            return;
        }

        try {
            final long startedAt = System.currentTimeMillis();
            final Path tempDumpPath = Path.of(tempPath).resolve(outputFilename);
            final Path targetDumpPath = Path.of(outputPath).resolve(outputFilename);

            dumpReadCount = 0;
            harvestUpdateCount = 0;
            outputWriteCount = 0;

            downloadZdbDump(tempDumpPath);

            loadZdbDumpToCache(tempDumpPath.toString());

            Files.deleteIfExists(tempDumpPath);

            LocalDateTime ldt = getLastModifiedRemote();
            log.info("Last modification of dump at {} was {}", DUMP_URL, ldt);
            while (ldt.isBefore(LocalDateTime.now())) {
                final String from = "&from=" + ldt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "Z";
                final String until = "&until=" + ldt.plusMinutes(30).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "Z";
                final String url = HARVEST_URL + from + until;
                log.info("Start harvest from {} ...", url);
                harvestZdbRecords(url);
                ldt = ldt.plusMinutes(30);
            }
            log.info("Finally applied {} harvested updates to cache", harvestUpdateCount);

            createNewZdbDump(tempDumpPath.toString());

            log.info("Move ZDB dump from {} to {} ...", tempDumpPath, targetDumpPath);
            Files.createDirectories(targetDumpPath.getParent());
            Files.move(tempDumpPath, targetDumpPath, StandardCopyOption.REPLACE_EXISTING);

            final long durationSeconds = (System.currentTimeMillis() - startedAt) / 1000;
            final long outputSizeBytes = Files.exists(targetDumpPath) ? Files.size(targetDumpPath) : 0L;
            log.info(
                    "Run statistics: dumpRead={}, harvestedUpdates={}, writtenToDump={}, duration={}s, output={}, size={} bytes",
                    dumpReadCount,
                    harvestUpdateCount,
                    outputWriteCount,
                    durationSeconds,
                    targetDumpPath,
                    outputSizeBytes);
            log.info("Successfully finished.");

        } catch (Exception e) {
            log.error("Dump creation failed", e);
            throw new IllegalStateException("Dump creation failed", e);
        } finally {
            setRunning(false);
        }
    }

    private void downloadZdbDump(Path tempFile) throws IOException {

        log.info("Start to download dump from {} to {} ...", DUMP_URL, tempFile);
        Files.createDirectories(tempFile.getParent());

        restClient.get()
                .uri(DUMP_URL)
                .exchange((request, response) -> {
                    if (!response.getStatusCode().is2xxSuccessful()) {
                        throw new IOException("Download failed with status " + response.getStatusCode().value());
                    }
                    try (final InputStream body = response.getBody()) {
                        if (body == null) {
                            throw new IOException("Download returned an empty response body.");
                        }
                        Files.copy(body, tempFile, StandardCopyOption.REPLACE_EXISTING);
                    }
                    return null;
                });
        log.info("Successfully downloaded dump.");
    }

    private void loadZdbDumpToCache(String pathToZdbDump)
            throws FileNotFoundException, IOException, XMLStreamException, TransformerConfigurationException {
        log.info("Start to write datasets to cache ...");

        final File dumpFile = new File(pathToZdbDump);

        try (final BufferedReader in = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(new FileInputStream(dumpFile)), StandardCharsets.UTF_8))) {

            final XMLStreamReader xsr = createXmlStreamReader(in, pathToZdbDump);
            xsr.nextTag(); // Advance to statements element
            mvStoreZdbData.clear();
            final TransformerFactory tf = TransformerFactory.newInstance();
            final Transformer t = tf.newTransformer();
            dumpReadCount = 0;
            try {

                while (xsr.nextTag() == XMLStreamConstants.START_ELEMENT) {

                    String fileName = xsr.getAttributeValue("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "about");
                    fileName = fileName.replace("https://ld.zdb-services.de/resource/", "");

                    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            final Writer writer = new OutputStreamWriter(bos, StandardCharsets.UTF_8);) {
                        try {
                            t.transform(new StAXSource(xsr), new StreamResult(writer));

                            mvStoreZdbData.put(fileName, bos.toString(StandardCharsets.UTF_8));
                            if (++dumpReadCount % PROGRESS_LOG_STEP == 0) {
                                log.info("Read {} datasets from base dump into cache ...", dumpReadCount);
                            }
                        } catch (Exception e) {
                            logXmlWarning(pathToZdbDump, fileName, e);
                        }
                    }
                }
            } catch (Exception e) {
                logXmlWarning(pathToZdbDump, null, e);
            } finally {
                log.info("Finally read {} datasets from base dump into cache", dumpReadCount);
            }
        }
        log.info("Successfully finished to write datasets to cache");
    }

    private void harvestZdbRecords(String url)
            throws IOException, XMLStreamException, TransformerConfigurationException {

        final TransformerFactory tf = TransformerFactory.newInstance();
        final Transformer t = tf.newTransformer();
        final String resumptionToken = fetchHarvestResponseWithRetry(url, t);

        if (resumptionToken != null) {
            harvestZdbRecords(HARVEST_WITH_RESUMPTION_TOKEN_URL + resumptionToken);
        }

    }

    private String fetchHarvestResponseWithRetry(String url, Transformer transformer) throws IOException {
        Exception lastException = null;

        for (int attempt = 1; attempt <= HARVEST_RETRY_ATTEMPTS; attempt++) {
            try {
                return restClient.get()
                        .uri(url)
                        .exchange((request, response) -> {
                            if (!response.getStatusCode().is2xxSuccessful()) {
                                throw new IOException("Harvest request failed for " + url + " with status "
                                        + response.getStatusCode().value());
                            }

                            try (final InputStream body = response.getBody()) {
                                if (body == null) {
                                    return null;
                                }

                                final XMLStreamReader xsr = createXmlStreamReader(
                                        new InputStreamReader(body, StandardCharsets.UTF_8),
                                        url);
                                xsr.nextTag();
                                String nextResumptionToken = null;

                                while (xsr.hasNext()) {
                                    if (xsr.next() != XMLStreamConstants.START_ELEMENT) {
                                        continue;
                                    }
                                    final String name = xsr.getName().getLocalPart();
                                    final String nameNamespace = xsr.getName().getNamespaceURI();

                                    if (name.equals("Description")
                                            && nameNamespace.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#")) {
                                        String fileName = xsr.getAttributeValue(
                                                "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                                "about");
                                        fileName = fileName.replace("https://ld.zdb-services.de/resource/", "");

                                        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                                final Writer writer = new OutputStreamWriter(
                                                        bos,
                                                        StandardCharsets.UTF_8)) {
                                            try {
                                                transformer.transform(new StAXSource(xsr), new StreamResult(writer));
                                                mvStoreZdbData.put(fileName, bos.toString(StandardCharsets.UTF_8));
                                                if (++harvestUpdateCount % PROGRESS_LOG_STEP == 0) {
                                                    log.info(
                                                            "Applied {} harvested updates to cache ...",
                                                            harvestUpdateCount);
                                                }
                                            } catch (Exception e) {
                                                logXmlWarning(url, fileName, e);
                                            }
                                        }
                                    }

                                    if (name.equals("resumptionToken")
                                            && nameNamespace.equals("http://www.openarchives.org/OAI/2.0/")) {
                                        final String rt = xsr.getElementText();
                                        if (rt != null && !rt.isBlank()) {
                                            log.debug("{} is {}", name, rt);
                                            nextResumptionToken = rt;
                                        }
                                    }
                                }

                                return nextResumptionToken;
                            } catch (XMLStreamException e) {
                                logXmlWarning(url, null, e);
                                throw new IOException("Failed to parse harvest response from " + url, e);
                            }
                        });
            } catch (Exception e) {
                lastException = e;
                if (!isTransientHarvestFailure(e) || attempt >= HARVEST_RETRY_ATTEMPTS) {
                    break;
                }
                log.warn(
                        "Harvest request attempt {}/{} failed for {}: {}. Retrying in {} ms ...",
                        attempt,
                        HARVEST_RETRY_ATTEMPTS,
                        url,
                        rootCauseMessage(e),
                        HARVEST_RETRY_DELAY_MILLIS * attempt);
                sleepBeforeRetry(url, attempt);
            }
        }

        log.error("Harvest failed for URL after {} attempts: {}", HARVEST_RETRY_ATTEMPTS, url, lastException);
        if (lastException instanceof IOException ioException) {
            throw ioException;
        }
        throw new IOException("Harvest failed for " + url, lastException);
    }

    private void sleepBeforeRetry(String url, int attempt) throws IOException {
        try {
            Thread.sleep(HARVEST_RETRY_DELAY_MILLIS * attempt);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to retry harvest request for " + url, e);
        }
    }

    private boolean isTransientHarvestFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof ResourceAccessException) {
                return true;
            }
            final String message = current.getMessage();
            if (message != null) {
                final String normalized = message.toLowerCase();
                if (normalized.contains("connection reset")
                        || normalized.contains("timed out")
                        || normalized.contains("timeout")
                        || normalized.contains("temporarily unavailable")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private String rootCauseMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() != null ? current.getMessage() : throwable.getMessage();
    }

    private void logXmlWarning(String source, String datasetId, Exception exception) {
        final XMLStreamException xmlException = findXmlStreamException(exception);
        final String datasetInfo = datasetId != null && !datasetId.isBlank() ? ", dataset=" + datasetId : "";

        if (xmlException != null && xmlException.getLocation() != null) {
            log.warn(
                    "Malformed XML in {}{} at line {}, column {}: {}",
                    source,
                    datasetInfo,
                    xmlException.getLocation().getLineNumber(),
                    xmlException.getLocation().getColumnNumber(),
                    xmlException.getMessage());
            return;
        }

        log.warn("Malformed XML in {}{}: {}", source, datasetInfo, exception.getMessage());
    }

    private XMLStreamException findXmlStreamException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof XMLStreamException xmlStreamException) {
                return xmlStreamException;
            }
            current = current.getCause();
        }
        return null;
    }

    private XMLStreamReader createXmlStreamReader(Reader reader, String sourceDescription) throws XMLStreamException {
        return xif.createXMLStreamReader(new XmlSanitizingReader(reader, sourceDescription, log));
    }

    private void createNewZdbDump(String outputFile) throws FileNotFoundException, IOException, XMLStreamException {

        log.info("Start to write dump to \"{}\" ...", outputFile);

        try (final FileOutputStream outputWriter = new FileOutputStream(outputFile);
                final Writer writer = new OutputStreamWriter(new GZIPOutputStream(outputWriter),
                        StandardCharsets.UTF_8);) {
            final XMLOutputFactory xmlOutFactory = XMLOutputFactory.newFactory();
            xmlOutFactory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);

            final XMLEventWriter xmlEventWriter = xmlOutFactory.createXMLEventWriter(writer);

            // xmlEventWriter.setDefaultNamespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#");
            // // setze Default-Namespace
            xmlEventWriter.add(xmlEventFactory.createStartDocument("UTF-8", "1.0"));
            xmlEventWriter.add(xmlEventFactory.createCharacters("\n"));
            xmlEventWriter.add(xmlEventFactory.createStartElement("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    "RDF", null, nsl.iterator()));
            xmlEventWriter.add(xmlEventFactory.createCharacters("\n"));

            final XMLInputFactory xmlInFactory = XMLInputFactory.newFactory();
            final Iterator<Map.Entry<String, String>> it = mvStoreZdbData.entrySet().iterator();
            outputWriteCount = 0;
            while (it.hasNext()) {

                final Map.Entry<String, String> e = it.next();
                final ByteArrayInputStream inputStream = new ByteArrayInputStream(
                        e.getValue().getBytes(StandardCharsets.UTF_8));
                final XMLEventReader xmlEventReader = xmlInFactory.createXMLEventReader(inputStream);
                XMLEvent event = xmlEventReader.nextEvent();
                // Skip ahead in the input to the opening document element
                while (event.getEventType() != XMLEvent.START_ELEMENT) {
                    event = xmlEventReader.nextEvent();
                }

                do {
                    xmlEventWriter.add(event);
                    event = xmlEventReader.nextEvent();
                } while (event.getEventType() != XMLEvent.END_DOCUMENT);
                xmlEventReader.close();
                xmlEventWriter.add(xmlEventFactory.createCharacters("\n"));

                if (++outputWriteCount % PROGRESS_LOG_STEP == 0) {
                    log.info("Wrote {} datasets to \"{}\" ...", outputWriteCount, outputFile);
                }
            }
            log.info("Successfully wrote {} datasets to \"{}\"", outputWriteCount, outputFile);

            xmlEventWriter.add(xmlEventFactory.createEndElement("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "RDF"));
            xmlEventWriter.add(xmlEventFactory.createCharacters("\n"));
            xmlEventWriter.add(xmlEventFactory.createEndDocument());

            xmlEventWriter.close();
        }

    }

    private LocalDateTime getLastModifiedRemote() throws IOException {
        try {
            return restClient.head()
                    .uri(DUMP_URL)
                    .exchange((request, response) -> {
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            throw new IOException("Failed to retrieve last modification date of dump. HTTP status: "
                                    + response.getStatusCode().value());
                        }
                        final String lastModified = response.getHeaders().getFirst("Last-Modified");
                        if (lastModified == null || lastModified.isBlank()) {
                            throw new IOException(
                                    "Failed to retrieve last modification date of dump. Last-Modified header is missing or empty.");
                        }
                        return ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME)
                                .toLocalDateTime();
                    });
        } catch (Exception e) {
            log.warn(e.getMessage());
            return LocalDateTime.now(); // return now to avoid infinite loop in case of error
        }
    }

}
