package de.ddb.labs.zdbdump.dump.rdf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Element;

/** Verifies that extracted RDF fragments retain the context of their original document. */
class RdfDumpTest {

    private static final String RDF_NS = RdfDump.RDF_ROOT.getNamespaceURI();
    private static final URI SNAPSHOT = URI.create("https://data.example/snapshots/zdb.rdf.gz?revision=1");

    @TempDir
    Path workspace;

    @Test
    void readsValidBaseWithManyEscapedAmpersands() throws Exception {
        final String escapedText = "&amp;".repeat(60_000);
        final Map<String, String> records = new LinkedHashMap<>();
        new RdfDump().loadBase(archive("""
                <rdf:RDF xmlns:rdf="%s" xmlns:dc="http://purl.org/dc/elements/1.1/">
                  <rdf:Description rdf:about="https://ld.zdb-services.de/resource/1">
                    <dc:title>%s</dc:title>
                  </rdf:Description>
                  <rdf:Description rdf:about="https://ld.zdb-services.de/resource/2">
                    <dc:title>%s</dc:title>
                  </rdf:Description>
                </rdf:RDF>
                """.formatted(RDF_NS, escapedText, escapedText)), SNAPSHOT, records);

        assertEquals(Set.of("1", "2"), records.keySet());
        assertTrue(records.get("2").contains(escapedText));
    }

    @Test
    void preservesRootContextAndResolvesSameSubjectBeforeReplacingRecords() throws Exception {
        final RdfDump rdf = new RdfDump();
        final Map<String, String> records = new LinkedHashMap<>();
        rdf.loadBase(archive("""
                <rdf:RDF xmlns:rdf="%s" xmlns:dc="http://purl.org/dc/elements/1.1/"
                         xml:base="https://ld.zdb-services.de/resource/" xml:lang="de">
                  <rdf:Description rdf:about="1"><dc:title>before</dc:title></rdf:Description>
                  <rdf:Description rdf:about="https://ld.zdb-services.de/resource/1">
                    <dc:title xml:lang="fr">updated</dc:title>
                  </rdf:Description>
                  <rdf:Description rdf:about="2" xml:base="editions/" xml:lang="">
                    <dc:relation xml:base="../" rdf:resource="1"/>
                  </rdf:Description>
                </rdf:RDF>
                """.formatted(RDF_NS)), SNAPSHOT, records);

        assertEquals(Set.of("1", "editions/2"), records.keySet());
        final Path result = workspace.resolve("result.rdf.gz");
        rdf.write(result, records.values());
        try (var gzip = new GZIPInputStream(Files.newInputStream(result))) {
            final var factory = DocumentBuilderFactory.newDefaultNSInstance();
            final var elements = factory.newDocumentBuilder().parse(gzip).getElementsByTagNameNS(RDF_NS, "Description");
            final var first = (Element) elements.item(0);
            assertEquals("https://ld.zdb-services.de/resource/", first.getBaseURI());
            assertEquals("de", first.getAttributeNS(XMLConstants.XML_NS_URI, "lang"));
            final var title = first.getElementsByTagNameNS("http://purl.org/dc/elements/1.1/", "title");
            assertEquals("updated", title.item(0).getTextContent());
            assertEquals("fr", ((Element) title.item(0)).getAttributeNS(XMLConstants.XML_NS_URI, "lang"));

            final var second = (Element) elements.item(1);
            assertEquals("https://ld.zdb-services.de/resource/editions/", second.getBaseURI());
            assertTrue(second.hasAttributeNS(XMLConstants.XML_NS_URI, "lang"));
            assertEquals("", second.getAttributeNS(XMLConstants.XML_NS_URI, "lang"));
            final var relation = second.getElementsByTagNameNS("http://purl.org/dc/elements/1.1/", "relation");
            assertEquals("https://ld.zdb-services.de/resource/", relation.item(0).getBaseURI());
        }
    }

    @Test
    void preservesImplicitDocumentBaseOnlyWhereAnRdfReferenceNeedsIt() throws Exception {
        final Map<String, String> records = new LinkedHashMap<>();
        new RdfDump().loadBase(archive("""
                <rdf:RDF xmlns:rdf="%s" xmlns:dc="http://purl.org/dc/elements/1.1/">
                  <rdf:Description rdf:about=""/>
                  <rdf:Description rdf:about="?revision=2"/>
                  <rdf:Description rdf:about="https://example.test/relative-child">
                    <dc:relation rdf:resource="../work/1"/>
                  </rdf:Description>
                  <rdf:Description rdf:about="https://example.test/absolute-child">
                    <dc:relation rdf:resource="https://example.test/work/1"/>
                  </rdf:Description>
                </rdf:RDF>
                """.formatted(RDF_NS)), SNAPSHOT, records);

        assertTrue(records.get(SNAPSHOT.toString()).contains("xml:base=\"" + SNAPSHOT + "\""));
        assertTrue(records.containsKey("https://data.example/snapshots/zdb.rdf.gz?revision=2"));
        assertTrue(records.get("https://example.test/relative-child").contains("xml:base=\"" + SNAPSHOT + "\""));
        assertFalse(records.get("https://example.test/absolute-child").contains("xml:base="));
    }

    @Test
    void matchesRelativePathsToAbsoluteSubjectsWithoutLosingEmptySegments() throws Exception {
        final Map<String, String> records = new LinkedHashMap<>();
        new RdfDump().loadBase(archive("""
                <rdf:RDF xmlns:rdf="%s">
                  <rdf:Description rdf:about="https://data.example/item"/>
                  <rdf:Description rdf:about="/one/../item"/>
                  <rdf:Description rdf:about="https://data.example/snapshots/nested//item"/>
                  <rdf:Description rdf:about="nested//item"/>
                  <rdf:Description rdf:about="https://mirror.example:8443/item/?item=a%%2Fb#part%%26rest"/>
                  <rdf:Description rdf:about="//mirror.example:8443/one/../item/.?item=a%%2Fb#part%%26rest"/>
                </rdf:RDF>
                """.formatted(RDF_NS)), SNAPSHOT, records);

        assertEquals(Set.of("https://data.example/item", "https://data.example/snapshots/nested//item",
                "https://mirror.example:8443/item/?item=a%2Fb#part%26rest"), records.keySet());
        assertTrue(records.get("https://data.example/item").contains("rdf:about=\"/one/../item\""));
        assertTrue(records.get("https://data.example/snapshots/nested//item").contains("rdf:about=\"nested//item\""));
    }

    private Path archive(String xml) throws IOException {
        final Path path = workspace.resolve("snapshot.rdf.gz");
        try (var gzip = new GZIPOutputStream(Files.newOutputStream(path))) {
            gzip.write(xml.getBytes(StandardCharsets.UTF_8));
        }
        return path;
    }
}
