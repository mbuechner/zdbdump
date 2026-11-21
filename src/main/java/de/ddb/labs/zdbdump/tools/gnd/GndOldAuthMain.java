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
package de.ddb.labs.zdbdump.tools.gnd;

import de.ddb.labs.zdbdump.dump.support.DumpFailure;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.zip.GZIPInputStream;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.json.JsonFactory;

/**
 * Standalone exporter for former GND authority numbers from the DNB person authority dump.
 * Each former number produces a text mapping and a JSON-LD redirect to the current GND record.
 * The CLI accepts a JSON-LD output path followed by a mapping output path in existing directories.
 * The optional {@code --beacon} flag selects a BEACON link dump. The default text mapping uses the legacy format.
 */
public class GndOldAuthMain {

    private static final Logger log = LoggerFactory.getLogger(GndOldAuthMain.class);
    private static final URI DOWNLOAD_URL = URI.create("https://data.dnb.de/opendata/authorities-gnd-person_lds.rdf.gz");
    private static final String RDF_NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    private static final String GND_NAMESPACE = "https://d-nb.info/standards/elementset/gnd#";
    private static final String GND_PREFIX = "https://d-nb.info/gnd/";
    private static final Path DEFAULT_JSON_OUTPUT = Path.of("D:\\neu3\\output.json");
    private static final Path DEFAULT_MAPPING_OUTPUT = Path.of("D:\\neu3\\output.txt");
    private static final JsonFactory JSON_FACTORY = JsonFactory.builder().build();

    private final RestClient restClient;

    /** Creates an exporter for the DNB person authority dump. */
    public GndOldAuthMain() {
        this(RestClient.create());
    }

    GndOldAuthMain(RestClient restClient) {
        this.restClient = restClient;
    }

    /**
     * Runs the export with the default destinations or the two destinations supplied by the caller.
     * Use {@code --beacon} for a former-to-current BEACON link dump. The default format is {@code current|former}.
     *
     * @param args optional {@code --beacon} flag and optional JSON-LD and mapping paths, in that order
     * @throws IllegalArgumentException if the argument count is invalid, a supplied path is malformed,
     *         or the two destinations resolve to the same path
     * @throws IOException if an export file cannot be prepared, read, or written
     * @throws XMLStreamException if the downloaded RDF/XML cannot be parsed
     */
    public static void main(String[] args) throws IOException, XMLStreamException {
        final boolean beaconFormat = args.length > 0 && "--beacon".equals(args[0]);
        final int firstPath = beaconFormat ? 1 : 0;
        final boolean useDefaultPaths = args.length == firstPath;
        if (!useDefaultPaths && args.length != firstPath + 2) {
            throw new IllegalArgumentException("Usage: GndOldAuthMain [--beacon] [json-output mapping-output]");
        }
        final var exporter = new GndOldAuthMain();
        final Path jsonOutput = useDefaultPaths ? DEFAULT_JSON_OUTPUT : Path.of(args[firstPath]);
        final Path mappingOutput = useDefaultPaths ? DEFAULT_MAPPING_OUTPUT : Path.of(args[firstPath + 1]);
        try {
            exporter.run(jsonOutput, mappingOutput, beaconFormat);
        } catch (DumpFailure failure) {
            log.error("{}", failure.getMessage());
            System.exit(1);
        }
    }

    /**
     * Downloads and exports the redirects to the legacy Windows paths
     * {@code D:\neu3\output.json} and {@code D:\neu3\output.txt}.
     *
     * @throws IOException if the DNB download or the export cannot be completed
     * @throws XMLStreamException if the downloaded RDF/XML cannot be parsed
     */
    public void run() throws IOException, XMLStreamException {
        run(DEFAULT_JSON_OUTPUT, DEFAULT_MAPPING_OUTPUT, false);
    }

    /** Downloads the staged source before opening either export destination. */
    void run(Path jsonOutput, Path mappingOutput, boolean beaconFormat) throws IOException, XMLStreamException {
        try (var dumpFile = new TemporaryFile(Files.createTempFile("GndOldAuthMain-", ".tmp"))) {
            download(dumpFile.path());
            export(dumpFile.path(), jsonOutput, mappingOutput, beaconFormat);
        }
    }

    /**
     * Builds both exports from a local authority dump before publishing each with an atomic move.
     * Staging lets the existing files remain readable during processing and keeps partial output
     * out of the destination if the RDF cannot be read completely.
     *
     * @param dumpFile local GZIP-compressed RDF/XML input
     * @param jsonOutput destination for the JSON-LD redirects
     * @param mappingOutput destination for the text mappings
     * @param beaconFormat whether to write a former-to-current BEACON link dump
     * @throws IllegalArgumentException if both destinations resolve to the same path
     * @throws IOException if a file cannot be opened, written, or published
     * @throws XMLStreamException if the RDF/XML cannot be parsed
     */
    static void export(Path dumpFile, Path jsonOutput, Path mappingOutput, boolean beaconFormat)
            throws IOException, XMLStreamException {
        final Path jsonDestination = outputPath(jsonOutput);
        final Path mappingDestination = outputPath(mappingOutput);
        if (jsonDestination.equals(mappingDestination)) {
            throw new IllegalArgumentException("The JSON and mapping exports require separate output paths.");
        }

        final long startedAt = System.nanoTime();
        log.info("Exporting GND redirects to JSON at {} and {} mappings at {}",
                jsonDestination, beaconFormat ? "BEACON" : "legacy", mappingDestination);
        final long recordsWritten;
        try (var stagedJson = TemporaryFile.forOutput(jsonDestination);
                var stagedMapping = TemporaryFile.forOutput(mappingDestination)) {
            recordsWritten = writeOutputs(dumpFile, stagedJson.path(), stagedMapping.path(), beaconFormat);
            stagedJson.publish(jsonDestination);
            stagedMapping.publish(mappingDestination);
        }
        log.info("Published {} GND redirects in {} ms", recordsWritten,
                Duration.ofNanos(System.nanoTime() - startedAt).toMillis());
    }

    /** Resolves the existing parent directory so aliases can be detected before staging. */
    private static Path outputPath(Path destination) throws IOException {
        final Path absolute = destination.toAbsolutePath().normalize();
        if (absolute.getFileName() == null || Files.isDirectory(absolute)) {
            throw new IOException("The export destination must refer to a file at " + absolute);
        }
        return absolute.getParent().toRealPath().resolve(absolute.getFileName());
    }

    private static long writeOutputs(Path dumpFile, Path stagedJson, Path stagedMapping, boolean beaconFormat)
            throws IOException, XMLStreamException {
        try (var compressed = Files.newInputStream(dumpFile);
                var input = new BufferedReader(new InputStreamReader(new GZIPInputStream(compressed),
                        StandardCharsets.UTF_8.newDecoder()));
                var mapping = Files.newBufferedWriter(stagedMapping);
                var output = Files.newOutputStream(stagedJson);
                var json = JSON_FACTORY.createGenerator(ObjectWriteContext.empty(), output)) {
            final XMLInputFactory factory = XMLInputFactory.newDefaultFactory();
            factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
            final XMLStreamReader reader = factory.createXMLStreamReader(input);
            try (XmlReaderClose ignored = reader::close) {
                if (reader.nextTag() != XMLStreamReader.START_ELEMENT || !"RDF".equals(reader.getLocalName())
                        || !RDF_NAMESPACE.equals(reader.getNamespaceURI())) {
                    throw new XMLStreamException("Expected an rdf:RDF document", reader.getLocation());
                }
                if (beaconFormat) {
                    writeBeaconHeader(mapping);
                }
                json.writeStartArray();
                final long recordsWritten = readRecords(reader, mapping, json, beaconFormat);
                json.writeEndArray();
                return recordsWritten;
            }
        } catch (IOException e) {
            throw new IOException("Could not read or write the GND export files", e);
        }
    }

    /** Streams top-level descriptions after the RDF opening tag and returns the number of redirects written. */
    private static long readRecords(XMLStreamReader reader, BufferedWriter mapping, JsonGenerator json,
            boolean beaconFormat)
            throws XMLStreamException, IOException {
        long resourcesProcessed = 0;
        long recordsWritten = 0;
        int depth = 1;
        String currentId = null;
        while (reader.hasNext()) {
            switch (reader.next()) {
                case XMLStreamReader.START_ELEMENT -> {
                    depth++;
                    if (depth == 2 && "Description".equals(reader.getLocalName())
                            && RDF_NAMESPACE.equals(reader.getNamespaceURI())) {
                        final String subject = reader.getAttributeValue(RDF_NAMESPACE, "about");
                        if (subject != null && subject.startsWith(GND_PREFIX)) {
                            currentId = subject.substring(GND_PREFIX.length());
                        }
                    } else if (depth == 3 && currentId != null && "oldAuthorityNumber".equals(reader.getLocalName())
                            && GND_NAMESPACE.equals(reader.getNamespaceURI())) {
                        final String previousId = reader.getElementText().replaceAll("\\([^)]*\\)", "").strip();
                        // getElementText already consumed the closing tag of oldAuthorityNumber.
                        depth--;
                        if (!previousId.isEmpty() && !previousId.equals(currentId)) {
                            writeMapping(mapping, previousId, currentId, beaconFormat, reader);
                            writeRedirect(json, currentId, previousId);
                            recordsWritten++;
                        }
                    }
                }
                case XMLStreamReader.END_ELEMENT -> {
                    if (depth == 2 && currentId != null) {
                        currentId = null;
                        if (++resourcesProcessed % 100_000 == 0) {
                            log.info("Processed {} GND resources and wrote {} redirects", resourcesProcessed, recordsWritten);
                        }
                    }
                    depth--;
                }
                default -> { }
            }
        }
        return recordsWritten;
    }

    /** Declares how former and current GND identifiers expand to URLs in the link dump. */
    private static void writeBeaconHeader(BufferedWriter mapping) throws IOException {
        mapping.write("#FORMAT: BEACON");
        mapping.newLine();
        mapping.write("#PREFIX: " + GND_PREFIX);
        mapping.newLine();
        mapping.write("#TARGET: " + GND_PREFIX);
        mapping.newLine();
    }

    /** Writes either a BEACON row with an explicit target token or the legacy two-column mapping. */
    private static void writeMapping(BufferedWriter mapping, String previousId, String currentId, boolean beaconFormat,
            XMLStreamReader reader) throws IOException, XMLStreamException {
        if (beaconFormat) {
            if (!isBeaconToken(previousId) || !isBeaconToken(currentId)) {
                throw new XMLStreamException("Invalid GND identifier for BEACON export", reader.getLocation());
            }
            mapping.write(previousId + "||" + currentId);
        } else {
            mapping.write(currentId + "|" + previousId);
        }
        mapping.newLine();
    }

    /** Checks that an identifier can occupy one BEACON token without changing the row structure. */
    private static boolean isBeaconToken(String id) {
        return !id.isEmpty() && !id.startsWith("#") && id.indexOf('|') < 0
                && id.codePoints().noneMatch(cp -> Character.isWhitespace(cp) || Character.isISOControl(cp));
    }

    /** Describes the former GND URI as an alias of the current URI in JSON-LD. */
    private static void writeRedirect(JsonGenerator json, String currentId, String previousId) {
        json.writeStartObject();
        json.writeStringProperty("@id", GND_PREFIX + previousId);
        writeRedirectValue(json, "http://www.w3.org/2002/07/owl#sameAs", currentId);
        writeRedirectValue(json, "https://d-nb.info/standards/elementset/dnb#canonicalUri", currentId);
        json.writeEndObject();
    }

    /** Writes a redirect target in the legacy JSON-LD value-object format. */
    private static void writeRedirectValue(JsonGenerator json, String property, String id) {
        json.writeName(property);
        json.writeStartArray();
        json.writeStartObject();
        json.writeStringProperty("@value", GND_PREFIX + id);
        json.writeEndObject();
        json.writeEndArray();
    }

    /** Streams the compressed response into the staging file for the export. */
    private void download(Path destination) throws IOException {
        log.info("Starting download from {}", DOWNLOAD_URL);
        try {
            final Long bytes = restClient.get()
                    .uri(DOWNLOAD_URL)
                    .exchange((request, response) -> {
                        DumpFailure.requireSuccessfulStatus(response, DOWNLOAD_URL);
                        return Files.copy(response.getBody(), destination, StandardCopyOption.REPLACE_EXISTING);
                    });
            log.info("Downloaded {} bytes from {}", bytes, DOWNLOAD_URL);
        } catch (RestClientException e) {
            throw DumpFailure.downloadFailure("the GND RDF/XML dump", DOWNLOAD_URL, e);
        }
    }

    /** Adapts StAX reader cleanup to try-with-resources. */
    @FunctionalInterface
    private interface XmlReaderClose extends AutoCloseable {

        /**
         * Closes the adapted StAX reader.
         *
         * @throws XMLStreamException if the reader cannot be closed
         */
        @Override
        void close() throws XMLStreamException;
    }

    /**
     * Owns a staging file until it is published or removed at the end of the operation.
     *
     * @param path the staging file
     */
    private record TemporaryFile(Path path) implements Closeable {

        /** Stages beside the destination so publication can use an atomic rename. */
        private static TemporaryFile forOutput(Path destination) throws IOException {
            try {
                return new TemporaryFile(Files.createTempFile(destination.getParent(), ".gnd-export-", ".tmp"));
            } catch (IOException e) {
                throw new IOException("Could not prepare a temporary export file for " + destination, e);
            }
        }

        private void publish(Path destination) throws IOException {
            try {
                Files.move(path, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                log.debug("Published GND export file at {}", destination);
            } catch (IOException e) {
                throw new IOException("Could not publish the GND export to " + destination, e);
            }
        }

        /**
         * Removes a staging file that still exists.
         *
         * @throws IOException if the staging file cannot be removed
         */
        @Override
        public void close() throws IOException {
            Files.deleteIfExists(path);
        }
    }
}
