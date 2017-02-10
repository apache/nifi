/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.fingerprint;

import static org.apache.nifi.fingerprint.FingerprintFactory.FLOW_CONFIG_XSD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

/**
 */
public class FingerprintFactoryTest {

    private FingerprintFactory fingerprinter;

    @Before
    public void setup() {
        fingerprinter = new FingerprintFactory(null);
    }

    @Test
    public void testSameFingerprint() throws IOException {
        final String fp1 = fingerprinter.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1a.xml"), null);
        final String fp2 = fingerprinter.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1b.xml"), null);
        assertEquals(fp1, fp2);
    }

    @Test
    public void testDifferentFingerprint() throws IOException {
        final String fp1 = fingerprinter.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1a.xml"), null);
        final String fp2 = fingerprinter.createFingerprint(getResourceBytes("/nifi/fingerprint/flow2.xml"), null);
        assertNotEquals(fp1, fp2);
    }

    @Test
    public void testResourceValueInFingerprint() throws IOException {
        final String fingerprint = fingerprinter.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1a.xml"), null);
        assertEquals(3, StringUtils.countMatches(fingerprint, "success"));
        assertTrue(fingerprint.contains("In Connection"));
    }

    @Test
    public void testSchemaValidation() throws IOException {
        FingerprintFactory fp = new FingerprintFactory(null, getValidatingDocumentBuilder());
        final String fingerprint = fp.createFingerprint(getResourceBytes("/nifi/fingerprint/validating-flow.xml"), null);
    }

    private byte[] getResourceBytes(final String resource) throws IOException {
        return IOUtils.toByteArray(FingerprintFactoryTest.class.getResourceAsStream(resource));
    }

    private DocumentBuilder getValidatingDocumentBuilder() {
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema;
        try {
            schema = schemaFactory.newSchema(FingerprintFactory.class.getResource(FLOW_CONFIG_XSD));
        } catch (final Exception e) {
            throw new RuntimeException("Failed to parse schema for file flow configuration.", e);
        }
        try {
            documentBuilderFactory.setSchema(schema);
            DocumentBuilder docBuilder = documentBuilderFactory.newDocumentBuilder();
            docBuilder.setErrorHandler(new ErrorHandler() {
                @Override
                public void warning(SAXParseException e) throws SAXException {
                    throw e;
                }

                @Override
                public void error(SAXParseException e) throws SAXException {
                    throw e;
                }

                @Override
                public void fatalError(SAXParseException e) throws SAXException {
                    throw e;
                }
            });
            return docBuilder;
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create document builder for flow configuration.", e);
        }
    }
}
