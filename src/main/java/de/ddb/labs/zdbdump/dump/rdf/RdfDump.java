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
package de.ddb.labs.zdbdump.dump.rdf;

import com.ctc.wstx.api.WstxOutputProperties;
import com.ctc.wstx.stax.WstxOutputFactory;
import de.ddb.labs.zdbdump.dump.support.DumpFailure;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streams RDF/XML records from the base archive and OAI responses, then writes the published GZIP dump.
 * Owns the XML parser and writer factories used by a single dump worker.
 */
public final class RdfDump {

    private static final Logger log = LoggerFactory.getLogger(RdfDump.class);
    private static final int PROGRESS_LOG_STEP = 100_000;
    private static final String RDF_NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    /** Qualified name of the root element in an RDF/XML document. */
    public static final QName RDF_ROOT = new QName(RDF_NAMESPACE, "RDF");
    private static final QName RDF_ABOUT = new QName(RDF_NAMESPACE, "about");
    private static final QName XML_BASE = new QName(XMLConstants.XML_NS_URI, "base", XMLConstants.XML_NS_PREFIX);
    private static final QName XML_LANG = new QName(XMLConstants.XML_NS_URI, "lang", XMLConstants.XML_NS_PREFIX);
    private static final String HTTPS_ZDB_RESOURCE = "https://ld.zdb-services.de/resource/";
    private static final Pattern IRI_SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*:");

    private final XMLInputFactory xmlInputFactory = XMLInputFactory.newDefaultFactory();
    private final XMLOutputFactory xmlOutputFactory = new WstxOutputFactory();
    private final XMLEventFactory xmlEventFactory = XMLEventFactory.newDefaultFactory();
    private final List<Namespace> namespaces = createNamespaces();

    /** Creates the StAX factories with DTD and external entity handling disabled on input. */
    public RdfDump() {
        xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        // JDK StAX counts predefined references such as &amp; across the whole document.
        // Use finite limits sized for large RDF snapshots.
        xmlInputFactory.setProperty("jdk.xml.maxGeneralEntitySizeLimit", 1_000_000);
        xmlInputFactory.setProperty("jdk.xml.totalEntitySizeLimit", 10_000_000);
        xmlOutputFactory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);
        // Write carriage returns as references so reading a serialized fragment preserves its text.
        xmlOutputFactory.setProperty(WstxOutputProperties.P_OUTPUT_ESCAPE_CR, true);
    }

    /**
     * Rebuilds the working map one RDF subject at a time. Replaying a new attempt from this base
     * also discards any partial updates left in MVStore by an earlier failure.
     *
     * @param path the downloaded or cached GZIP archive
     * @param documentUri the original retrieval URI for the RDF document
     * @param recordsBySubject the working map to rebuild once the RDF root has been verified
     * @throws IOException if the archive cannot be read or contains no RDF datasets
     * @throws XMLStreamException if its RDF/XML cannot be parsed or serialized
     * @throws InterruptedException if the worker is interrupted while loading records
     */
    public void loadBase(Path path, URI documentUri, Map<String, String> recordsBySubject)
            throws IOException, XMLStreamException, InterruptedException {
        long records = 0;
        try (InputStream raw = Files.newInputStream(path);
                InputStream gzip = new GZIPInputStream(raw);
                XmlSanitizingReader sanitized = sanitize(gzip)) {
            final XMLEventReader xml = openReader(sanitized);
            try (XmlClose ignored = xml::close) {
                final XmlContext root = new XmlContext(documentUri).descend(requireRoot(xml, RDF_ROOT));
                recordsBySubject.clear();
                XMLEvent event;
                while ((event = xml.nextTag()).isStartElement()) {
                    readRecord(xml, event.asStartElement(), root, recordsBySubject);
                    if (++records % PROGRESS_LOG_STEP == 0) {
                        DumpFailure.checkInterrupted();
                        log.info("Read {} datasets from the base dump ...", records);
                    }
                }
                while (!xml.nextEvent().isEndDocument()) {
                    // Read the full stream to detect a malformed document or a damaged gzip trailer.
                }
            } finally {
                logRecoveries(log, "Base RDF dump", sanitized);
            }
        }
        if (records == 0) {
            throw new DumpFailure("Base dump contains no RDF datasets", null);
        }
        log.info("Loaded {} datasets from the base dump", records);
    }

    /**
     * Streams the working fragments into a single UTF-8 RDF document and closes its GZIP stream before publication.
     *
     * @param path the staging file to write
     * @param records the standalone RDF/XML fragments in the desired output order
     * @throws IOException if the staging file cannot be written or closed
     * @throws XMLStreamException if a record cannot be read or written as XML
     * @throws InterruptedException if the worker is interrupted while writing records
     */
    public void write(Path path, Collection<String> records)
            throws IOException, XMLStreamException, InterruptedException {
        try (var output = Files.newOutputStream(path);
                Writer compressed = new OutputStreamWriter(new GZIPOutputStream(output), StandardCharsets.UTF_8)) {
            final XMLEventWriter xml = xmlOutputFactory.createXMLEventWriter(compressed);
            try (XmlClose ignored = xml::close) {
                xml.add(xmlEventFactory.createStartDocument("UTF-8", "1.0"));
                xml.add(xmlEventFactory.createCharacters("\n"));
                xml.add(xmlEventFactory.createStartElement("rdf", RDF_NAMESPACE, "RDF", null, namespaces.iterator()));
                xml.add(xmlEventFactory.createCharacters("\n"));
                long count = 0;
                for (String record : records) {
                    writeFragment(xml, record);
                    xml.add(xmlEventFactory.createCharacters("\n"));
                    if (++count % PROGRESS_LOG_STEP == 0) {
                        DumpFailure.checkInterrupted();
                        log.info("Wrote {} datasets to the staged dump ...", count);
                    }
                }
                xml.add(xmlEventFactory.createEndElement("rdf", RDF_NAMESPACE, "RDF"));
                xml.add(xmlEventFactory.createCharacters("\n"));
                xml.add(xmlEventFactory.createEndDocument());
            }
        }
    }

    /** Copies a standalone fragment into the open RDF document, omitting its document envelope. */
    private void writeFragment(XMLEventWriter output, String fragment) throws XMLStreamException {
        final XMLEventReader record = xmlInputFactory.createXMLEventReader(new StringReader(fragment));
        try (XmlClose ignored = record::close) {
            XMLEvent event;
            while (!(event = record.nextEvent()).isEndDocument()) {
                if (!event.isStartDocument()) {
                    output.add(event);
                }
            }
        }
    }

    /**
     * Runs an operation on a UTF-8 upstream XML stream after repairing malformed entity references.
     * Closes the parser and the input stream, and reports repairs using the supplied logger.
     *
     * @param <T> the type of the result produced by the operation
     * @param input the upstream input stream owned by this call
     * @param source the source label to include in the logs
     * @param diagnostics the logger for recovery warnings
     * @param operation the operation that consumes the StAX events
     * @return the result of the operation
     * @throws IOException if closing the underlying input fails
     * @throws XMLStreamException if opening, reading, or closing the StAX reader fails
     */
    public <T> T readXml(InputStream input, String source, Logger diagnostics, XmlRead<T> operation)
            throws IOException, XMLStreamException {
        try (XmlSanitizingReader sanitized = sanitize(input)) {
            final XMLEventReader xml = openReader(sanitized);
            try (XmlClose ignored = xml::close) {
                return operation.read(xml);
            } finally {
                logRecoveries(diagnostics, source, sanitized);
            }
        }
    }

    private XMLEventReader openReader(XmlSanitizingReader source) throws XMLStreamException {
        return xmlInputFactory.createXMLEventReader(source);
    }

    /**
     * Reads one complete RDF element and stores its standalone fragment under its subject key.
     * A later record with the same key replaces the previous value.
     *
     * @param xml the reader positioned immediately after the record's opening tag
     * @param record the opening tag of the record
     * @param parent the inherited base URI and language from its enclosing elements
     * @param records the destination map keyed by RDF subject
     * @throws XMLStreamException if the record is malformed, lacks a subject, or cannot be serialized
     */
    public void readRecord(XMLEventReader xml, StartElement record, XmlContext parent, Map<String, String> records)
            throws XMLStreamException {
        final XmlContext context = parent.descend(record);
        records.put(recordKey(record, context.base), serializeRecord(xml, record, context));
    }

    /**
     * Checks for an opening tag by its namespace URI and local name.
     *
     * @param event the event to inspect
     * @param name the expected name
     * @return whether the event opens the named element
     */
    public static boolean isStartElement(XMLEvent event, QName name) {
        return event.isStartElement() && event.asStartElement().getName().equals(name);
    }

    /**
     * Discards the content of the current element, including nested elements and the closing tag.
     *
     * @param xml the reader positioned immediately after the opening tag
     * @throws XMLStreamException if the reader cannot reach the closing tag
     */
    public static void skipElement(XMLEventReader xml) throws XMLStreamException {
        copyElement(xml, null);
    }

    /** Uses a strict UTF-8 decoder so decoding failures surface before entity repair. */
    private static XmlSanitizingReader sanitize(InputStream stream) {
        return new XmlSanitizingReader(
                new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8.newDecoder())));
    }

    /**
     * Consumes the XML prolog and checks the root element. A DTD causes validation to fail.
     *
     * @param xml the reader positioned at the beginning of the document
     * @param expected the required root name
     * @return the root opening tag with its attributes
     * @throws XMLStreamException if a DTD appears or the root is missing or has a different name
     */
    public static StartElement requireRoot(XMLEventReader xml, QName expected) throws XMLStreamException {
        XMLEvent event;
        while (!(event = xml.nextEvent()).isEndDocument()) {
            if (event.getEventType() == XMLStreamConstants.DTD) {
                throw new XMLStreamException("XML documents may not declare a DTD", event.getLocation());
            }
            if (event.isStartElement()) {
                if (!event.asStartElement().getName().equals(expected)) {
                    throw new XMLStreamException("Expected element " + expected, event.getLocation());
                }
                return event.asStartElement();
            }
        }
        throw new XMLStreamException("Missing element " + expected);
    }

    /**
     * Looks up a decoded attribute by qualified name.
     *
     * @param element the opening tag containing the attributes
     * @param name the name of the requested attribute
     * @return the attribute value, or {@code null} if the attribute is absent
     */
    public static String attribute(StartElement element, QName name) {
        final Attribute attribute = element.getAttributeByName(name);
        return attribute == null ? null : attribute.getValue();
    }

    /** Uses the local identifier for canonical ZDB subjects and keeps other IRIs distinct. */
    private static String recordKey(StartElement record, URI base) throws XMLStreamException {
        final String subject = attribute(record, RDF_ABOUT);
        if (subject == null) {
            throw new XMLStreamException("RDF record is missing its rdf:about identifier", record.getLocation());
        }
        final String resolved = IRI_SCHEME.matcher(subject).lookingAt() ? subject
                : resolveReference(base, subject, record).toString();
        if (resolved.startsWith(HTTPS_ZDB_RESOURCE)) {
            return resolved.substring(HTTPS_ZDB_RESOURCE.length());
        }
        return resolved;
    }

    /** Resolves relative IRIs, retaining the base path for empty and query-only references. */
    private static URI resolveReference(URI base, String value, StartElement element) throws XMLStreamException {
        try {
            final URI reference = URI.create(value);
            if (reference.isAbsolute()) {
                return reference;
            }
            if (!base.isAbsolute()) {
                throw new IllegalArgumentException("A relative RDF reference needs an absolute base URI");
            }
            if (reference.getRawAuthority() == null && reference.getRawPath().isEmpty()) {
                final String document = withoutFragment(base.toString());
                final int query = document.indexOf('?');
                if (reference.getRawQuery() != null && query >= 0) {
                    return URI.create(document.substring(0, query) + value);
                }
                return URI.create(document + value);
            }
            return resolvePathReference(base, reference);
        } catch (IllegalArgumentException e) {
            throw new XMLStreamException("Invalid RDF IRI or XML base URI", element.getLocation(), e);
        }
    }

    /** Resolves path references as in RFC 3986 without dropping empty or decoding escaped segments. */
    private static URI resolvePathReference(URI base, URI reference) {
        if (base.isOpaque()) {
            throw new IllegalArgumentException("A relative RDF path needs a hierarchical base URI");
        }
        final String referenceAuthority = reference.getRawAuthority();
        final String authority = referenceAuthority == null ? base.getRawAuthority() : referenceAuthority;
        String path = reference.getRawPath();
        if (referenceAuthority == null && !path.startsWith("/")) {
            final String basePath = base.getRawPath();
            final int lastSlash = basePath.lastIndexOf('/');
            path = (lastSlash < 0 ? "/" : basePath.substring(0, lastSlash + 1)) + path;
        }
        final String resolvedPath = removeDotSegments(path);
        final StringBuilder result = new StringBuilder(base.getScheme()).append(':');
        if (authority != null) {
            result.append("//").append(authority);
        } else if (resolvedPath.startsWith("//")) {
            // An explicit empty authority keeps a leading empty path segment out of the authority.
            result.append("//");
        }
        result.append(resolvedPath);
        if (reference.getRawQuery() != null) {
            result.append('?').append(reference.getRawQuery());
        }
        if (reference.getRawFragment() != null) {
            result.append('#').append(reference.getRawFragment());
        }
        return URI.create(result.toString());
    }

    /** Normalizes an absolute raw path, preserving empty and trailing segments. */
    private static String removeDotSegments(String path) {
        if (path.isEmpty()) {
            return path;
        }
        final String[] segments = path.split("/", -1);
        final List<String> resolved = new ArrayList<>(segments.length);
        for (int index = 1; index < segments.length; index++) {
            final String segment = segments[index];
            if ("..".equals(segment)) {
                if (!resolved.isEmpty()) {
                    resolved.removeLast();
                }
            } else if (!".".equals(segment)) {
                resolved.add(segment);
                continue;
            }
            if (index == segments.length - 1) {
                resolved.add("");
            }
        }
        return "/" + String.join("/", resolved);
    }

    private static String withoutFragment(String uri) {
        final int fragment = uri.indexOf('#');
        return fragment < 0 ? uri : uri.substring(0, fragment);
    }

    /** Stores a rereadable standalone fragment with namespace bindings and significant whitespace preserved. */
    private String serializeRecord(XMLEventReader xml, StartElement start, XmlContext context) throws XMLStreamException {
        final StringWriter string = new StringWriter();
        final XMLEventWriter writer = xmlOutputFactory.createXMLEventWriter(string);
        final boolean writeBase = context.declaredBase || hasRelativeRdfIri(start);
        try (XmlClose ignored = writer::close) {
            writer.add(withContext(start, writeBase ? context.base.toString() : null, context.language));
            if (writeBase) {
                copyElement(xml, writer);
            } else {
                copyWithDocumentBase(xml, writer, context);
            }
        }
        return string.toString();
    }

    /** Keeps absolute fragments compact and adds the retrieval base where an implicit relative IRI needs it. */
    private void copyWithDocumentBase(XMLEventReader xml, XMLEventWriter writer, XmlContext context)
            throws XMLStreamException {
        int depth = 1;
        while (depth > 0) {
            final XMLEvent event = xml.nextEvent();
            if (event.isStartElement()) {
                final StartElement start = event.asStartElement();
                if (attribute(start, XML_BASE) != null || hasRelativeRdfIri(start)) {
                    writer.add(withContext(start, context.descend(start).base.toString(), null));
                    copyElement(xml, writer);
                    continue;
                }
                depth++;
            } else if (event.isEndElement()) {
                depth--;
            }
            writer.add(event);
        }
    }

    /** Copies the effective context to the fragment while retaining unrelated attributes and namespace bindings. */
    private StartElement withContext(StartElement start, String base, String language) {
        final boolean updateBase = base != null && !base.equals(attribute(start, XML_BASE));
        final boolean updateLanguage = language != null && !language.equals(attribute(start, XML_LANG));
        if (!updateBase && !updateLanguage) {
            return start;
        }
        final List<Attribute> attributes = new ArrayList<>();
        start.getAttributes().forEachRemaining(attribute -> {
            final QName name = attribute.getName();
            if ((!updateBase || !name.equals(XML_BASE)) && (!updateLanguage || !name.equals(XML_LANG))) {
                attributes.add(attribute);
            }
        });
        if (updateBase) {
            attributes.add(xmlEventFactory.createAttribute(XML_BASE, base));
        }
        if (updateLanguage) {
            attributes.add(xmlEventFactory.createAttribute(XML_LANG, language));
        }
        final QName name = start.getName();
        return xmlEventFactory.createStartElement(name.getPrefix(), name.getNamespaceURI(), name.getLocalPart(),
                attributes.iterator(), start.getNamespaces(), start.getNamespaceContext());
    }

    private static boolean hasRelativeRdfIri(StartElement start) {
        final var attributes = start.getAttributes();
        while (attributes.hasNext()) {
            final Attribute attribute = attributes.next();
            final QName name = attribute.getName();
            if (RDF_NAMESPACE.equals(name.getNamespaceURI())) {
                switch (name.getLocalPart()) {
                    case "ID" -> { return true; }
                    case "about", "resource", "datatype" -> {
                        if (!IRI_SCHEME.matcher(attribute.getValue()).lookingAt()) {
                            return true;
                        }
                    }
                    default -> { }
                }
            }
        }
        return false;
    }

    /** The effective XML base URI and language at one position in an upstream document. */
    public static final class XmlContext {
        private final URI base;
        private final String language;
        private final boolean declaredBase;

        /**
         * Starts with the document's retrieval URI and no default language.
         *
         * @param documentUri the absolute URI used to retrieve the document
         */
        public XmlContext(URI documentUri) {
            this(documentUri, null, false);
        }

        private XmlContext(URI base, String language, boolean declaredBase) {
            this.base = base;
            this.language = language;
            this.declaredBase = declaredBase;
        }

        /**
         * Applies declarations on a child element, inheriting context for attributes it omits.
         *
         * @param element the child opening tag
         * @return the effective context of the child
         * @throws XMLStreamException if the base cannot be established
         */
        public XmlContext descend(StartElement element) throws XMLStreamException {
            final String childBase = attribute(element, XML_BASE);
            final String childLanguage = attribute(element, XML_LANG);
            if (childBase == null && childLanguage == null) {
                return this;
            }
            final URI resolvedBase = childBase == null ? base
                    : URI.create(withoutFragment(resolveReference(base, childBase, element).toString()));
            return new XmlContext(resolvedBase, childLanguage == null ? language : childLanguage,
                    declaredBase || childBase != null);
        }
    }

    /**
     * Consumes the open element through its closing tag, leaving any following sibling for the caller.
     * When a writer is supplied, the consumed events become part of the serialized fragment.
     */
    private static void copyElement(XMLEventReader xml, XMLEventWriter writer) throws XMLStreamException {
        int depth = 1;
        while (depth > 0) {
            final XMLEvent event = xml.nextEvent();
            if (event.isStartElement()) {
                depth++;
            } else if (event.isEndElement()) {
                depth--;
            }
            if (writer != null) {
                writer.add(event);
            }
        }
    }

    /** Logs the repair count and first position with the caller's source label. */
    private static void logRecoveries(Logger log, String source, XmlSanitizingReader sanitized) {
        final XmlSanitizingReader.Recoveries recoveries = sanitized.recoveries();
        if (recoveries.count() > 0) {
            log.warn("Recovered {} malformed XML entities in {}. First at line {}, column {}",
                    recoveries.count(), source, recoveries.firstLine(), recoveries.firstColumn());
        }
    }

    /**
     * Consumes events from an upstream XML stream managed by {@link #readXml}.
     *
     * @param <T> the type of the result produced by reading the stream
     */
    @FunctionalInterface
    public interface XmlRead<T> {
        /**
         * Reads the XML events needed to produce a result.
         *
         * @param xml the event reader positioned at the beginning of the document
         * @return the parsed result
         * @throws XMLStreamException if the document cannot be read or validated
         */
        T read(XMLEventReader xml) throws XMLStreamException;
    }

    /** Allows try-with-resources to preserve the original error when a StAX close also fails. */
    @FunctionalInterface
    private interface XmlClose extends AutoCloseable {
        /**
         * Closes the StAX resource.
         *
         * @throws XMLStreamException if the resource cannot be closed
         */
        @Override
        void close() throws XMLStreamException;
    }

    /** Supplies common RDF prefixes for the published root. The XML writer repairs additional bindings in each record. */
    private List<Namespace> createNamespaces() {
        return List.of(
                xmlEventFactory.createNamespace("schema", "http://schema.org/"),
                xmlEventFactory.createNamespace("gndo", "https://d-nb.info/standards/elementset/gnd#"),
                xmlEventFactory.createNamespace("lib", "http://purl.org/library/"),
                xmlEventFactory.createNamespace("owl", "http://www.w3.org/2002/07/owl#"),
                xmlEventFactory.createNamespace("xsd", "http://www.w3.org/2001/XMLSchema#"),
                xmlEventFactory.createNamespace("skos", "http://www.w3.org/2004/02/skos/core#"),
                xmlEventFactory.createNamespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
                xmlEventFactory.createNamespace("editeur", "https://ns.editeur.org/thema/"),
                xmlEventFactory.createNamespace("geo", "http://www.opengis.net/ont/geosparql#"),
                xmlEventFactory.createNamespace("umbel", "http://umbel.org/umbel#"),
                xmlEventFactory.createNamespace("rdau", "http://rdaregistry.info/Elements/u/"),
                xmlEventFactory.createNamespace("sf", "http://www.opengis.net/ont/sf#"),
                xmlEventFactory.createNamespace("bflc", "http://id.loc.gov/ontologies/bflc/"),
                xmlEventFactory.createNamespace("dcterms", "http://purl.org/dc/terms/"),
                xmlEventFactory.createNamespace("vivo", "http://vivoweb.org/ontology/core#"),
                xmlEventFactory.createNamespace("isbd", "http://iflastandards.info/ns/isbd/elements/"),
                xmlEventFactory.createNamespace("foaf", "http://xmlns.com/foaf/0.1/"),
                xmlEventFactory.createNamespace("mo", "http://purl.org/ontology/mo/"),
                xmlEventFactory.createNamespace("marcRole", "http://id.loc.gov/vocabulary/relators/"),
                xmlEventFactory.createNamespace("agrelon", "https://d-nb.info/standards/elementset/agrelon#"),
                xmlEventFactory.createNamespace("dcmitype", "http://purl.org/dc/dcmitype/"),
                xmlEventFactory.createNamespace("dbp", "http://dbpedia.org/property/"),
                xmlEventFactory.createNamespace("dnbt", "https://d-nb.info/standards/elementset/dnb#"),
                xmlEventFactory.createNamespace("madsrdf", "http://www.loc.gov/mads/rdf/v1#"),
                xmlEventFactory.createNamespace("dnb_intern", "http://dnb.de/"),
                xmlEventFactory.createNamespace("rdf", RDF_NAMESPACE),
                xmlEventFactory.createNamespace("v", "http://www.w3.org/2006/vcard/ns#"),
                xmlEventFactory.createNamespace("wdrs", "http://www.w3.org/2007/05/powder-s#"),
                xmlEventFactory.createNamespace("ebu", "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#"),
                xmlEventFactory.createNamespace("bibo", "http://purl.org/ontology/bibo/"),
                xmlEventFactory.createNamespace("gbv", "http://purl.org/ontology/gbv/"),
                xmlEventFactory.createNamespace("dc", "http://purl.org/dc/elements/1.1/"));
    }
}
