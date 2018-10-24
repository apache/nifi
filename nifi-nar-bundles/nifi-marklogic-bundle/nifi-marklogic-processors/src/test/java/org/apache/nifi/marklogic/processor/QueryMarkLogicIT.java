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
package org.apache.nifi.marklogic.processor;

import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryMarkLogicIT extends AbstractMarkLogicIT {
    private String collection;

    @Before
    public void setup() {
        super.setup();
        collection = "QueryMarkLogicTest";
        // Load documents to Query
        loadDocumentsIntoCollection(collection, documents);
    }

    private void loadDocumentsIntoCollection(String collection, List<IngestDoc> documents) {
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher()
            .withBatchSize(3)
            .withThreadCount(3);
        dataMovementManager.startJob(writeBatcher);
        for(IngestDoc document : documents) {
            DocumentMetadataHandle handle = new DocumentMetadataHandle();
            handle.withCollections(collection);
            writeBatcher.add(document.getFileName(), handle, new StringHandle(document.getContent()));
        }
        writeBatcher.flushAndWait();
        dataMovementManager.stopJob(writeBatcher);
    }

    @After
    public void teardown() {
        super.teardown();
        deleteDocumentsInCollection(collection);
    }

    protected TestRunner getNewTestRunner(Class processor) throws InitializationException {
        TestRunner runner = super.getNewTestRunner(processor);
        runner.assertNotValid();
        runner.setProperty(QueryMarkLogic.CONSISTENT_SNAPSHOT, "true");
        return runner;
    }

    @Test
    public void testSimpleCollectionQuery() throws InitializationException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, collection);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COLLECTION.name());
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, numDocs);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS,CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        byte[] actualByteArray = null;
        for(MockFlowFile flowFile : flowFiles) {
            if(flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("3.json")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(3).getContent().getBytes();
        assertEquals(expectedByteArray.length, actualByteArray.length);
        assertTrue(Arrays.equals(expectedByteArray, actualByteArray));
    }

    @Test
    public void testOldCollectionQuery() throws InitializationException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.COLLECTIONS, collection);
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, numDocs);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS,CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        byte[] actualByteArray = null;
        for(MockFlowFile flowFile : flowFiles) {
            if(flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("3.json")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(3).getContent().getBytes();
        assertEquals(expectedByteArray.length, actualByteArray.length);
        assertTrue(Arrays.equals(expectedByteArray, actualByteArray));
    }

    @Test
    public void testCombinedJSONQuery() throws InitializationException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "{\"search\" : {\n" +
                "  \"ctsquery\": {\n" +
                "    \"jsonPropertyValueQuery\":{\n" +
                "      \"property\":[\"sample\"],\n" +
                "      \"value\":[\"jsoncontent\"]\n" +
                "      } \n" +
                "  }\n" +
                "} }");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COMBINED_JSON.name());
        runner.assertValid();
        runner.run();
        // mod 3 == 0 docs are JSON, but mod 5 == 0 docs are XML and take precedence in doc generation
        int expectedSize = (int) (Math.ceil((numDocs - 1.0) / 3.0) - Math.ceil((numDocs - 1.0) / 15.0));
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedSize);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS,CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedSize);
        byte[] actualByteArray = null;
        for(MockFlowFile flowFile : flowFiles) {
            if(flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("3.json")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(3).getContent().getBytes();
        assertEquals(expectedByteArray.length, actualByteArray.length);
        assertTrue(Arrays.equals(expectedByteArray, actualByteArray));
        runner.shutdown();
    }

    @Test
    public void testCombinedXMLQuery() throws InitializationException, SAXException, IOException, ParserConfigurationException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "<cts:element-value-query xmlns:cts=\"http://marklogic.com/cts\">\n" +
                "  <cts:element>sample</cts:element>\n" +
                "  <cts:text xml:lang=\"en\">xmlcontent</cts:text>\n" +
                "</cts:element-value-query>");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COMBINED_XML.name());
        runner.assertValid();
        runner.run();
        // mod 5 == 0 docs are generated as XML
        int expectedSize = (int) Math.ceil((numDocs - 1.0) / 5.0);
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedSize);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS,CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedSize);
        byte[] actualByteArray = null;
        for(MockFlowFile flowFile : flowFiles) {
            if(flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("/5.xml")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(5).getContent().getBytes();

        assertBytesAreEqualXMLDocs(expectedByteArray,actualByteArray);
        runner.shutdown();
    }

    @Test
    public void testStructuredJSONQuery() throws InitializationException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "{\n" +
                "  \"query\": {\n" +
                "    \"queries\": [\n" +
                "      { \n" +
                "       \"value-query\": {\n" +
                "          \"type\": \"string\",\n" +
                "          \"json-property\": [\"sample\"],\n" +
                "          \"text\": [\"jsoncontent\"]\n" +
                "        }" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRUCTURED_JSON.name());
        runner.assertValid();
        runner.run();
        // mod 3 == 0 docs are JSON, but mod 5 == 0 docs are XML and take precedence in doc generation
        int expectedSize = (int) (Math.ceil((numDocs - 1.0) / 3.0) - Math.ceil((numDocs - 1.0) / 15.0));
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedSize);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS,CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedSize);
        byte[] actualByteArray = null;
        for(MockFlowFile flowFile : flowFiles) {
            if(flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("3.json")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(3).getContent().getBytes();
        assertEquals(expectedByteArray.length, actualByteArray.length);
        assertTrue(Arrays.equals(expectedByteArray, actualByteArray));
        runner.shutdown();
    }

    @Test
    public void testStructuredXMLQuery() throws InitializationException, SAXException, IOException, ParserConfigurationException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "<query xmlns=\"http://marklogic.com/appservices/search\">\n" +
                "  <word-query>\n" +
                "    <element name=\"sample\" ns=\"\" />\n" +
                "    <text>xmlcontent</text>\n" +
                "  </word-query>\n" +
                "</query>");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRUCTURED_XML.name());
        runner.assertValid();
        runner.run();
        // mod 5 == 0 docs are generated as XML
        int expectedSize = (int) Math.ceil((numDocs - 1.0) / 5.0);
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedSize);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS,CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedSize);
        byte[] actualByteArray = null;
        for(MockFlowFile flowFile : flowFiles) {
            if(flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("/5.xml")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(5).getContent().getBytes();

        assertBytesAreEqualXMLDocs(expectedByteArray,actualByteArray);
        runner.shutdown();
    }


    @Test
    public void testStringQuery() throws InitializationException, SAXException, IOException, ParserConfigurationException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, "xmlcontent");
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.STRING.name());
        runner.assertValid();
        runner.run();
        // mod 5 == 0 docs are generated as XML
        int expectedSize = (int) Math.ceil((numDocs - 1.0) / 5.0);
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, expectedSize);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS,CoreAttributes.FILENAME.key());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        assertEquals(flowFiles.size(), expectedSize);
        byte[] actualByteArray = null;
        for(MockFlowFile flowFile : flowFiles) {
            if(flowFile.getAttribute(CoreAttributes.FILENAME.key()).endsWith("/5.xml")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(5).getContent().getBytes();

        assertBytesAreEqualXMLDocs(expectedByteArray,actualByteArray);
        runner.shutdown();
    }

    @Test
    public void testJobProperties() throws InitializationException {
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.setProperty(QueryMarkLogic.QUERY, collection);
        runner.setProperty(QueryMarkLogic.QUERY_TYPE, QueryMarkLogic.QueryTypes.COLLECTION.name());
        runner.run();
        Processor processor = runner.getProcessor();
        if(processor instanceof QueryMarkLogic) {
            QueryBatcher queryBatcher = ((QueryMarkLogic) processor).getQueryBatcher();
            assertEquals(Integer.parseInt(batchSize), queryBatcher.getBatchSize());
            assertEquals(Integer.parseInt(threadCount), queryBatcher.getThreadCount());
        } else {
            fail("Processor not an instance of QueryMarkLogic");
        }
        runner.shutdown();
    }

    private void assertBytesAreEqualXMLDocs(byte[] expectedByteArray, byte[] actualByteArray) throws SAXException, IOException, ParserConfigurationException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        dbf.setCoalescing(true);
        dbf.setIgnoringElementContentWhitespace(true);
        dbf.setIgnoringComments(true);
        DocumentBuilder db = dbf.newDocumentBuilder();

        Document doc1 = db.parse(new ByteArrayInputStream(actualByteArray));
        doc1.normalizeDocument();

        Document doc2 = db.parse(new ByteArrayInputStream(expectedByteArray));
        doc2.normalizeDocument();

        assertTrue(doc1.isEqualNode(doc2));
    }
}