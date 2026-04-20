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
package de.ddb.labs.zdbdump.playground;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 *
 * @author buechner
 */
public class GndOldAuthMain {

    private static final Logger log = LoggerFactory.getLogger(GndOldAuthMain.class);

    private final static String DOWNLOAD_URL = "https://data.dnb.de/opendata/authorities-gnd-person_lds.rdf.gz";
    private final static String XPATH_EXP = """
            /*[namespace-uri()='http://www.w3.org/1999/02/22-rdf-syntax-ns#' and local-name()='Description']/*[namespace-uri()='https://d-nb.info/standards/elementset/gnd#' and local-name()='oldAuthorityNumber']""";
    private final static String BEACON_TEMPLATE = """
            {{GNDID}}|{{OLDAUTHNO}}""";
    private final static String UMLENK_TEMPLATE = """
            {"@id":"https://d-nb.info/gnd/{{OLDAUTHNO}}","http://www.w3.org/2002/07/owl#sameAs":[{"@value":"https://d-nb.info/gnd/{{GNDID}}"}],"https://d-nb.info/standards/elementset/dnb#canonicalUri":[{"@value":"https://d-nb.info/gnd/{{GNDID}}"}]},""";
    private final static String UMLENK_OUTPUT_FILE = "D:\\neu3\\output.json";
    private final static String BEACON_OUTPUT_FILE = "D:\\neu3\\output.txt";

    private final XMLInputFactory xif = XMLInputFactory.newInstance();
    private final XPath xPath = XPathFactory.newInstance().newXPath();
    private final RestClient restClient = RestClient.create();

    public static void main(String[] args) {
        try {
            new GndOldAuthMain().run();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void run() throws FileNotFoundException, IOException, XMLStreamException, TransformerConfigurationException,
            XPathExpressionException, TransformerException {

        final File dumpFile = File.createTempFile("GndOldAuthMain-", ".tmp");
        dumpFile.deleteOnExit();

        download(DOWNLOAD_URL, dumpFile);

        // init xPath
        final XPathExpression xe = xPath.compile(XPATH_EXP);

        int count = 0;

        try (final BufferedReader in = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(new FileInputStream(dumpFile)), StandardCharsets.UTF_8))) {

            try (PrintWriter beaconWriter = new PrintWriter(new FileOutputStream(BEACON_OUTPUT_FILE, false), true,
                    StandardCharsets.UTF_8);
                    PrintWriter jsonWriter = new PrintWriter(new FileOutputStream(UMLENK_OUTPUT_FILE, false), true,
                            StandardCharsets.UTF_8);) {
                try {
                    jsonWriter.print("[");

                    final XMLStreamReader xsr = xif.createXMLStreamReader(in);
                    xsr.nextTag(); // Advance to statements element
                    final TransformerFactory tf = TransformerFactory.newInstance();
                    final Transformer t = tf.newTransformer();

                    while (xsr.nextTag() == XMLStreamConstants.START_ELEMENT) {

                        String gndId = xsr.getAttributeValue("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "about");
                        gndId = gndId.replace("https://d-nb.info/gnd/", "");

                        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                final Writer writer = new OutputStreamWriter(bos, StandardCharsets.UTF_8);) {
                            t.transform(new StAXSource(xsr), new StreamResult(writer));

                            final String xmlString = bos.toString(StandardCharsets.UTF_8);
                            final NodeList oldAuthNodeList = (NodeList) xe
                                    .evaluate(new InputSource(new StringReader(xmlString)), XPathConstants.NODESET);

                            for (int i = 0; i < oldAuthNodeList.getLength(); ++i) {
                                if (oldAuthNodeList.item(i).getNodeType() == Node.ELEMENT_NODE) {
                                    final Element el = (Element) oldAuthNodeList.item(i);
                                    final String elString = el.getTextContent().replaceAll("\\([^\\)]*\\)", "");
                                    if (!elString.equals(gndId)) {

                                        String beacon = BEACON_TEMPLATE;
                                        beacon = beacon.replaceAll("\\{\\{GNDID\\}\\}", gndId);
                                        beacon = beacon.replaceAll("\\{\\{OLDAUTHNO\\}\\}", elString);
                                        beaconWriter.println(beacon);

                                        // close JSON array
                                        String json = UMLENK_TEMPLATE;
                                        if (!xsr.hasNext() && i == oldAuthNodeList.getLength() - 1) {
                                            json = json.substring(0, json.length() - 1) + "]";
                                        }
                                        json = json.replaceAll("\\{\\{GNDID\\}\\}", gndId);
                                        json = json.replaceAll("\\{\\{OLDAUTHNO\\}\\}", elString);
                                        jsonWriter.println(json);
                                    }
                                }
                            }
                            if (++count % 100000 == 0) {
                                log.info("Processed {} datasets ...", count);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        }
        log.info("Finally processed {} datasets", count);
    }

    private void download(String url, File destFile) throws IOException {
        Objects.requireNonNull(url, "url must not be null");
        Objects.requireNonNull(destFile, "destFile must not be null");
        log.info("Start download of " + url + "...");
        restClient.get()
                .uri(url)
                .exchange((request, response) -> {
                    if (!response.getStatusCode().is2xxSuccessful()) {
                        throw new IOException("Download failed with status " + response.getStatusCode().value());
                    }
                    try (final var body = response.getBody()) {
                        if (body == null) {
                            throw new IOException("Download returned an empty response body.");
                        }
                        Files.copy(body, destFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    }
                    return null;
                });
        log.info("Download finished and saved to " + destFile.getAbsolutePath() + ".");
    }
}
