package de.ddb.labs.zdbdump.tools.gnd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertPathBuilderException;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.SSLHandshakeException;
import javax.xml.stream.XMLStreamException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

/** Exercises GND export publication and cleanup using local RDF fixtures. */
class GndOldAuthMainTest {

    private static final String RDF = """
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                    xmlns:gnd="https://d-nb.info/standards/elementset/gnd#">
              <rdf:Description rdf:about="https://d-nb.info/gnd/current">
                <gnd:oldAuthorityNumber>previous</gnd:oldAuthorityNumber>
              </rdf:Description>
            """;

    @TempDir
    Path directory;

    @Test
    void identifiesTheFailedGndDownloadWithoutOverwritingExports() throws IOException {
        final Path json = Files.writeString(directory.resolve("output.json"), "existing JSON");
        final Path mapping = Files.writeString(directory.resolve("output.txt"), "existing mapping");
        final var transport = new SSLHandshakeException("internal TLS diagnostic");
        transport.initCause(new CertPathBuilderException("untrusted certificate chain"));
        final RestClient client = RestClient.builder()
                .requestInterceptor((request, body, execution) -> { throw transport; })
                .build();

        final var failure = assertThrows(IOException.class, () -> new GndOldAuthMain(client).run(json, mapping, false));

        assertEquals("Could not download the GND RDF/XML dump from https://data.dnb.de/opendata/authorities-gnd-person_lds.rdf.gz.",
                failure.getMessage());
        assertEquals("existing JSON", Files.readString(json));
        assertEquals("existing mapping", Files.readString(mapping));
    }

    @Test
    void preservesExistingExportsAfterMalformedRdf() throws IOException {
        final Path dump = writeDump(RDF);
        final Path json = Files.writeString(directory.resolve("output.json"), "existing JSON");
        final Path beacon = Files.writeString(directory.resolve("output.txt"), "existing BEACON");

        assertThrows(XMLStreamException.class, () -> GndOldAuthMain.export(dump, json, beacon, false));

        assertEquals("existing JSON", Files.readString(json));
        assertEquals("existing BEACON", Files.readString(beacon));
        try (var files = Files.list(directory)) {
            assertEquals(3, files.count());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void publishesBothExportsAfterSuccessfulProcessing(boolean beaconFormat) throws IOException, XMLStreamException {
        final Path dump = writeDump(RDF + "</rdf:RDF>");
        final Path json = Files.writeString(directory.resolve("output.json"), "existing JSON");
        final Path beacon = Files.writeString(directory.resolve("output.txt"), "existing BEACON");

        GndOldAuthMain.export(dump, json, beacon, beaconFormat);

        final var mapper = JsonMapper.builder().build();
        assertEquals(mapper.readTree("""
                [{"@id":"https://d-nb.info/gnd/previous",
                  "http://www.w3.org/2002/07/owl#sameAs":[{"@value":"https://d-nb.info/gnd/current"}],
                  "https://d-nb.info/standards/elementset/dnb#canonicalUri":[{"@value":"https://d-nb.info/gnd/current"}]}]
                """), mapper.readTree(Files.readString(json)));
        final String expectedMapping = beaconFormat ? """
                #FORMAT: BEACON
                #PREFIX: https://d-nb.info/gnd/
                #TARGET: https://d-nb.info/gnd/
                previous||current
                """ : "current|previous\n";
        assertEquals(expectedMapping.replace("\n", System.lineSeparator()), Files.readString(beacon));
        try (var files = Files.list(directory)) {
            assertEquals(3, files.count());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "<html/>", "<RDF xmlns=\"urn:other\"/>"})
    void preservesExistingExportsWithoutAnRdfRoot(String xml) throws IOException {
        final Path dump = writeDump(xml);
        final Path json = Files.writeString(directory.resolve("output.json"), "existing JSON");
        final Path beacon = Files.writeString(directory.resolve("output.txt"), "existing BEACON");

        assertThrows(XMLStreamException.class, () -> GndOldAuthMain.export(dump, json, beacon, true));

        assertEquals("existing JSON", Files.readString(json));
        assertEquals("existing BEACON", Files.readString(beacon));
    }

    @Test
    void preservesExistingExportsWhenTheDumpContainsInvalidUtf8() throws IOException {
        final String rdf = (RDF + "</rdf:RDF>").replace("previous", "pr\u00E9vious");
        final Path dump = writeDump(rdf.getBytes(StandardCharsets.ISO_8859_1));
        final Path json = Files.writeString(directory.resolve("output.json"), "existing JSON");
        final Path beacon = Files.writeString(directory.resolve("output.txt"), "existing BEACON");

        assertThrows(XMLStreamException.class, () -> GndOldAuthMain.export(dump, json, beacon, true));

        assertEquals("existing JSON", Files.readString(json));
        assertEquals("existing BEACON", Files.readString(beacon));
    }

    @Test
    void publishesAValidEmptyLinkDump() throws IOException, XMLStreamException {
        final Path dump = writeDump("<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"/>");
        final Path json = directory.resolve("output.json");
        final Path beacon = directory.resolve("output.txt");

        GndOldAuthMain.export(dump, json, beacon, true);

        assertEquals("[]", Files.readString(json));
        assertEquals("""
                #FORMAT: BEACON
                #PREFIX: https://d-nb.info/gnd/
                #TARGET: https://d-nb.info/gnd/
                """.replace("\n", System.lineSeparator()), Files.readString(beacon));
    }

    private Path writeDump(String rdf) throws IOException {
        return writeDump(rdf.getBytes(StandardCharsets.UTF_8));
    }

    private Path writeDump(byte[] rdf) throws IOException {
        final Path dump = directory.resolve("dump.rdf.gz");
        try (var gzip = new GZIPOutputStream(Files.newOutputStream(dump))) {
            gzip.write(rdf);
        }
        return dump;
    }
}
