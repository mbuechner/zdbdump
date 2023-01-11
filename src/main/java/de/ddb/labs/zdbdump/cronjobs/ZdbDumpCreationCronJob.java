/*
 * Copyright 2023 Michael Büchner, Deutsche Digitale Bibliothek
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
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okio.BufferedSink;
import okio.Okio;
import org.h2.mvstore.MVMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ZdbDumpCreationCronJob {

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
    private OkHttpClient httpClient;

    @Getter
    @Setter
    private boolean isRunning = false;

    private final XMLInputFactory xif = XMLInputFactory.newInstance();

    private final XMLEventFactory xmlEventFactory = XMLEventFactory.newFactory();

    private int count = 0;

    private final List<Namespace> nsl = new ArrayList<Namespace>() {
        {
            add(xmlEventFactory.createNamespace("schema", "http://schema.org/"));
            add(xmlEventFactory.createNamespace("gndo", "https://d-nb.info/standards/elementset/gnd#"));
            add(xmlEventFactory.createNamespace("lib", "http://purl.org/library/"));
            add(xmlEventFactory.createNamespace("owl", "http://www.w3.org/2002/07/owl#"));
            add(xmlEventFactory.createNamespace("xsd", "http://www.w3.org/2001/XMLSchema#"));
            add(xmlEventFactory.createNamespace("skos", "http://www.w3.org/2004/02/skos/core#"));
            add(xmlEventFactory.createNamespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#"));
            add(xmlEventFactory.createNamespace("editeur", "https://ns.editeur.org/thema/"));
            add(xmlEventFactory.createNamespace("geo", "http://www.opengis.net/ont/geosparql#"));
            add(xmlEventFactory.createNamespace("umbel", "http://umbel.org/umbel#"));
            add(xmlEventFactory.createNamespace("rdau", "http://rdaregistry.info/Elements/u/"));
            add(xmlEventFactory.createNamespace("sf", "http://www.opengis.net/ont/sf#"));
            add(xmlEventFactory.createNamespace("bflc", "http://id.loc.gov/ontologies/bflc/"));
            add(xmlEventFactory.createNamespace("dcterms", "http://purl.org/dc/terms/"));
            add(xmlEventFactory.createNamespace("vivo", "http://vivoweb.org/ontology/core#"));
            add(xmlEventFactory.createNamespace("isbd", "http://iflastandards.info/ns/isbd/elements/"));
            add(xmlEventFactory.createNamespace("foaf", "http://xmlns.com/foaf/0.1/"));
            add(xmlEventFactory.createNamespace("mo", "http://purl.org/ontology/mo/"));
            add(xmlEventFactory.createNamespace("marcRole", "http://id.loc.gov/vocabulary/relators/"));
            add(xmlEventFactory.createNamespace("agrelon", "https://d-nb.info/standards/elementset/agrelon#"));
            add(xmlEventFactory.createNamespace("dcmitype", "http://purl.org/dc/dcmitype/"));
            add(xmlEventFactory.createNamespace("dbp", "http://dbpedia.org/property/"));
            add(xmlEventFactory.createNamespace("dnbt", "https://d-nb.info/standards/elementset/dnb#"));
            add(xmlEventFactory.createNamespace("madsrdf", "http://www.loc.gov/mads/rdf/v1#"));
            add(xmlEventFactory.createNamespace("dnb_intern", "http://dnb.de/"));
            add(xmlEventFactory.createNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"));
            add(xmlEventFactory.createNamespace("v", "http://www.w3.org/2006/vcard/ns#"));
            add(xmlEventFactory.createNamespace("wdrs", "http://www.w3.org/2007/05/powder-s#"));
            add(xmlEventFactory.createNamespace("ebu", "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#"));
            add(xmlEventFactory.createNamespace("bibo", "http://purl.org/ontology/bibo/"));
            add(xmlEventFactory.createNamespace("gbv", "http://purl.org/ontology/gbv/"));
            add(xmlEventFactory.createNamespace("dc", "http://purl.org/dc/elements/1.1/"));
        }
    };

    @Scheduled(cron = "${zdbdump.cron.job}")
    @Retryable(value = {Exception.class}, maxAttemptsExpression = "5", backoff = @Backoff(delayExpression = "600000"))
    public void run() {

        if (isRunning()) {
            log.info("ZDB/RDF dump creation aready running. Abort.");
            return;
        }

        try {
            setRunning(true);
            
            final String pathToTempZdbDump = tempPath + outputFilename;
            final String pathToZdbDump = outputPath + outputFilename;

            downloadZdbDump(pathToTempZdbDump);
            
            loadZdbDumpToCache(pathToTempZdbDump);

            // seccessfully loaded to cache -> delete dump file
            Files.deleteIfExists(Path.of(pathToTempZdbDump));

            // update from OAI until today (this hour)
            LocalDateTime ldt = getLastModifiedRemote().toLocalDateTime();
            count = 0;
            while (ldt.isBefore(LocalDateTime.now())) {
                final String from = "&from=" + ldt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "Z";
                final String until = "&until=" + ldt.plusMinutes(30).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "Z";
                harvestZdbRecords(HARVEST_URL + from + until);
                ldt = ldt.plusMinutes(30); // add 30 min. (recom. from DNB)
            }
            log.info("Finally wrote {} datasets to cache", count);

            // write new dump to hdd
            createNewZdbDump(pathToTempZdbDump);

            // move temp output file to path
            log.info("Move ZDB dump from {} to {} ...", pathToTempZdbDump, pathToZdbDump);
            Files.move(Path.of(pathToTempZdbDump), Path.of(pathToZdbDump), StandardCopyOption.REPLACE_EXISTING);

            log.info("Sucessfully finished.");
            
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            setRunning(false);
        }
    }

    private void downloadZdbDump(String tempFile) {

        // Download dump to temorary file
        final File dumpFile = new File(tempFile);
        log.info("Start to download dump from {} to {} ...", DUMP_URL, dumpFile);

        // download dump from reomte
        final Request request = new Request.Builder()
                .url(DUMP_URL)
                .get()
                .build();

        try (final Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                try (final BufferedSink sink = Okio.buffer(Okio.sink(dumpFile))) {
                    sink.writeAll(response.body().source());
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            return;
        }
        log.info("Successfully downloaded dump.");
    }

    private void loadZdbDumpToCache(String pathToZdbDump) throws FileNotFoundException, IOException, XMLStreamException, TransformerConfigurationException {
        log.info("Start to write datasets to cache ...");

        final File dumpFile = new File(pathToZdbDump);

        try (final BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(dumpFile)), StandardCharsets.UTF_8))) {

            final XMLStreamReader xsr = xif.createXMLStreamReader(in);
            xsr.nextTag(); // Advance to statements element
            mvStoreZdbData.clear();
            final TransformerFactory tf = TransformerFactory.newInstance();
            final Transformer t = tf.newTransformer();
            count = 0;
            try {

                while (xsr.nextTag() == XMLStreamConstants.START_ELEMENT) {

                    String fileName = xsr.getAttributeValue("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "about");
                    fileName = fileName.replace("https://ld.zdb-services.de/resource/", "");

                    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream(); // final Writer writer = new OutputStreamWriter(new GZIPOutputStream(bos), StandardCharsets.UTF_8);
                             final Writer writer = new OutputStreamWriter(bos, StandardCharsets.UTF_8);) {
                        try {
                            t.transform(new StAXSource(xsr), new StreamResult(writer));

                            mvStoreZdbData.put(fileName, bos.toString(StandardCharsets.UTF_8));
                            if (++count % 100000 == 0) {

                                log.info("Wrote {} datasets to cache ...", count);
                            }
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            } finally {
                log.info("Finally wrote {} datasets to cache", count);
            }
        }
        log.info("Successfully finished to write datasets to cache");
    }

    private void harvestZdbRecords(String url) throws IOException, XMLStreamException, TransformerConfigurationException {

        log.info("Start to harvest request {} ...", url);
        final Request request = new Request.Builder()
                .url(url)
                .build();

        final Call call = httpClient.newCall(request);
        final Response response = call.execute();
        if (!response.isSuccessful()) {
            return;
        }

        final XMLStreamReader xsr = xif.createXMLStreamReader(response.body().byteStream());
        xsr.nextTag(); // Advance to statements element
        final TransformerFactory tf = TransformerFactory.newInstance();
        final Transformer t = tf.newTransformer();
        String resumptionToken = null;

        while (xsr.hasNext()) {

            if (xsr.next() != XMLStreamConstants.START_ELEMENT) {
                continue;
            }
            final String name = xsr.getName().getLocalPart();
            final String nameNamespace = xsr.getName().getNamespaceURI();

            if (name.equals("Description") && nameNamespace.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#")) {
                String fileName = xsr.getAttributeValue("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "about");
                fileName = fileName.replace("https://ld.zdb-services.de/resource/", "");

                try (final ByteArrayOutputStream bos = new ByteArrayOutputStream(); final Writer writer = new OutputStreamWriter(bos, StandardCharsets.UTF_8);) {
                    try {
                        t.transform(new StAXSource(xsr), new StreamResult(writer));
                        mvStoreZdbData.put(fileName, bos.toString(StandardCharsets.UTF_8));
                        ++count;
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }
            }

            // resumption token handling
            if (name.equals("resumptionToken") && nameNamespace.equals("http://www.openarchives.org/OAI/2.0/")) {
                final String rt = xsr.getElementText();
                if (rt != null && !rt.isBlank()) {
                    log.debug("{} is {}", name, rt);
                    resumptionToken = rt;
                }
            }
        }

        log.info("Wrote {} datasets to cache ...", count);
        if (resumptionToken != null) {
            harvestZdbRecords(HARVEST_WITH_RESUMPTION_TOKEN_URL + resumptionToken);
        }

    }
    
    private void createNewZdbDump(String outputFile) throws FileNotFoundException, IOException, XMLStreamException {

        log.info("Start to write dump to \"{}\" ...", outputFile);

        try (final FileOutputStream outputWriter = new FileOutputStream(outputFile); final Writer writer = new OutputStreamWriter(new GZIPOutputStream(outputWriter), StandardCharsets.UTF_8);) {
            final XMLOutputFactory xmlOutFactory = XMLOutputFactory.newFactory();
            xmlOutFactory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);

            final XMLEventWriter xmlEventWriter = xmlOutFactory.createXMLEventWriter(writer);

            // xmlEventWriter.setDefaultNamespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#"); // setze Default-Namespace          
            xmlEventWriter.add(xmlEventFactory.createStartDocument("UTF-8", "1.0"));
            xmlEventWriter.add(xmlEventFactory.createCharacters("\n"));
            xmlEventWriter.add(xmlEventFactory.createStartElement("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "RDF", null, nsl.iterator()));
            xmlEventWriter.add(xmlEventFactory.createCharacters("\n"));

            final XMLInputFactory xmlInFactory = XMLInputFactory.newFactory();
            final Iterator<Map.Entry<String, String>> it = mvStoreZdbData.entrySet().iterator();
            count = 0;
            while (it.hasNext()) {

                final Map.Entry<String, String> e = it.next();
                final ByteArrayInputStream inputStream = new ByteArrayInputStream(e.getValue().getBytes(StandardCharsets.UTF_8));
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

                if (++count % 100000 == 0) {

                    log.info("Wrote {} datasets to \"{}\" ...", count, outputFile);
                }
            }
            log.info("Sucessfully wrote {} datasets to \"{}\"", count, outputFile);

            xmlEventWriter.add(xmlEventFactory.createEndElement("rdf", "http://rdfs.org/ns/void#", "RDF"));
            xmlEventWriter.add(xmlEventFactory.createCharacters("\n"));
            xmlEventWriter.add(xmlEventFactory.createEndDocument());

            xmlEventWriter.close();
        }

    }
    
    private ZonedDateTime getLastModifiedRemote() throws IOException {
        final Request request = new Request.Builder()
                .url(DUMP_URL)
                .head()
                .build();
        try (final Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                final String d = response.header("Last-Modified");
                final ZonedDateTime zdt = ZonedDateTime.parse(d, DateTimeFormatter.RFC_1123_DATE_TIME);
                return zdt;
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return null;
    }

}
