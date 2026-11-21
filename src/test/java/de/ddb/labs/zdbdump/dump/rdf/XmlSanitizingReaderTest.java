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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import org.junit.jupiter.api.Test;

/** Verifies entity repair, pass-through markup, and recovery positions in the source. */
class XmlSanitizingReaderTest {

    @Test
    void repairsOnlyEntitiesInTextAndAttributes() throws IOException {
        final String xml = "\uFEFF<?xml version=\"1.0\"?><?test no&change?>"
                + "<root attribute=\"a&b\"><!-- comment&example --><![CDATA[William & Mary &amp;]]]>"
                + "News &amp; notes &trends; &#x1F600; &#0; &#x110000;</root>";
        final String sanitized = "<?xml version=\"1.0\"?><?test no&change?>"
                + "<root attribute=\"a&amp;b\"><!-- comment&example --><![CDATA[William & Mary &amp;]]]>"
                + "News &amp; notes &amp;trends; &#x1F600; &amp;#0; &amp;#x110000;</root>";
        try (XmlSanitizingReader reader = new XmlSanitizingReader(new StringReader(xml))) {
            final StringWriter result = new StringWriter();
            reader.transferTo(result);
            assertEquals(sanitized, result.toString());
        }
    }

    @Test
    void matchesClosingDelimitersOnlyAfterTheOpeningMarkup() throws IOException {
        final String literal = "<!--> &unknown; --><![CDATA[&amp;unknown;]]]><?test &unknown;?>";
        final String xml = "<root>" + literal + "&repair;</root>";
        try (XmlSanitizingReader reader = new XmlSanitizingReader(new StringReader(xml))) {
            final StringWriter result = new StringWriter();
            reader.transferTo(result);
            assertEquals("<root>" + literal + "&amp;repair;</root>", result.toString());
            assertEquals(new XmlSanitizingReader.Recoveries(1, 1, 7 + literal.length()), reader.recoveries());
        }
    }

    @Test
    void decodesLongNumericReferencesAndPreservesInvalidCodePoints() throws IOException, XMLStreamException {
        final String zeros = "0".repeat(12_000);
        final String decimalA = "&#" + zeros + "65;";
        final String hexadecimalA = "&#x" + zeros + "41;";
        final String invalid = "&#" + zeros + ";";
        final String input = "<root>" + decimalA + hexadecimalA + invalid + "</root>";
        try (var reader = new XmlSanitizingReader(new StringReader(input))) {
            final StringWriter result = new StringWriter();
            reader.transferTo(result);
            assertEquals("<root>" + decimalA + hexadecimalA + "&amp;" + invalid.substring(1) + "</root>",
                    result.toString());
            assertEquals(new XmlSanitizingReader.Recoveries(1, 1, 7 + decimalA.length() + hexadecimalA.length()),
                    reader.recoveries());

            final var xml = XMLInputFactory.newFactory().createXMLStreamReader(new StringReader(result.toString()));
            try {
                xml.nextTag();
                assertEquals("AA" + invalid, xml.getElementText());
            } finally {
                xml.close();
            }
        }
    }

    @Test
    void countsRecoveriesAndReportsTheirFirstPositionInTheSource() throws IOException {
        final String xml = "<root>\r\nfirst &unescaped;\r\nsecond &unknown;</root>";
        try (XmlSanitizingReader reader = new XmlSanitizingReader(new StringReader(xml))) {
            assertEquals(new XmlSanitizingReader.Recoveries(0, -1, -1), reader.recoveries());

            reader.transferTo(Writer.nullWriter());

            assertEquals(new XmlSanitizingReader.Recoveries(2, 2, 7), reader.recoveries());
        }
    }
}
