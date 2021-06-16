/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.solr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xmlunit.matchers.CompareMatcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.stream.StreamSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class TestGetSolr {
    private static final String DEFAULT_SOLR_CORE = "testCollection";

    private static final String DATE_STRING_EARLIER = "1970-01-01T00:00:00.000Z";

    private static final String DATE_STRING_LATER = "1970-01-01T00:00:00.001Z";

    private static SolrClient solrClient;

    @BeforeClass
    public static void createSolrClient() throws Exception {
        solrClient = createEmbeddedSolrClient();
    }

    @AfterClass
    public static void closeSolrClient() throws Exception {
        solrClient.close();
    }

    private static SolrClient createEmbeddedSolrClient() throws IOException, SolrServerException {
        final String relPath = TestGetSolr.class.getResource("/").getPath();

        final SolrClient embeddedSolrClient = EmbeddedSolrServerFactory.create(EmbeddedSolrServerFactory.DEFAULT_SOLR_HOME,
                DEFAULT_SOLR_CORE, relPath);

        final Date date = Date.from(Instant.parse(DATE_STRING_EARLIER));

        for (int i = 0; i < 10; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "doc" + i);
            doc.addField("created", date);
            doc.addField("string_single", "single" + i + ".1");
            doc.addField("string_multi", "multi" + i + ".1");
            doc.addField("string_multi", "multi" + i + ".2");
            doc.addField("integer_single", i);
            doc.addField("integer_multi", 1);
            doc.addField("integer_multi", 2);
            doc.addField("integer_multi", 3);
            doc.addField("double_single", 0.5 + i);
            embeddedSolrClient.add(doc);

        }
        embeddedSolrClient.commit();
        return embeddedSolrClient;
    }

    private static TestRunner createDefaultTestRunner(GetSolr processor) {
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(GetSolr.RETURN_TYPE, GetSolr.MODE_XML.getValue());
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(GetSolr.DATE_FIELD, "created");
        runner.setProperty(GetSolr.RETURN_FIELDS, "id,created");
        runner.setProperty(SolrUtils.COLLECTION, "testCollection");
        runner.setValidateExpressionUsage(false);
        return runner;
    }

    @Test
    public void testLessThanBatchSizeShouldProduceOneFlowFile() {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(GetSolr.BATCH_SIZE, "20");

        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 1);
    }

    @Test
    public void testNoResultsShouldProduceNoOutput() {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(GetSolr.SOLR_QUERY, "integer_single:1000");
        runner.setProperty(GetSolr.BATCH_SIZE, "1");

        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 0);
    }

    @Test(expected = java.lang.AssertionError.class)
    public void testValidation() {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(GetSolr.BATCH_SIZE, "2");
        runner.setProperty(GetSolr.RETURN_TYPE, GetSolr.MODE_REC.getValue());

        runner.run(1, false);
    }

    @Test
    public void testCompletenessDespiteUpdates() throws Exception {
        final SolrClient testSolrClient = createEmbeddedSolrClient();
        final TestableProcessor proc = new TestableProcessor(testSolrClient);

        TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(GetSolr.BATCH_SIZE, "1");

        runner.run(1,false, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 10);
        runner.clearTransferState();

        SolrInputDocument doc0 = new SolrInputDocument();
        doc0.addField("id", "doc0");
        doc0.addField("created", new Date());
        SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField("id", "doc1");
        doc1.addField("created", new Date());

        testSolrClient.add(doc0);
        testSolrClient.add(doc1);
        testSolrClient.commit();

        runner.run(1,true, false);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 2);
        runner.assertAllFlowFilesContainAttribute(CoreAttributes.MIME_TYPE.key());

        testSolrClient.close();
    }

    @Test
    public void testCompletenessDespiteDeletions() throws IOException, SolrServerException {
        final SolrClient testSolrClient = createEmbeddedSolrClient();
        final TestableProcessor proc = new TestableProcessor(testSolrClient);

        TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(GetSolr.BATCH_SIZE, "1");

        runner.run(1,false, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 10);
        runner.clearTransferState();

        SolrInputDocument doc10 = new SolrInputDocument();
        doc10.addField("id", "doc10");
        doc10.addField("created", new Date());
        SolrInputDocument doc11 = new SolrInputDocument();
        doc11.addField("id", "doc11");
        doc11.addField("created", new Date());

        testSolrClient.add(doc10);
        testSolrClient.add(doc11);
        testSolrClient.deleteById("doc0");
        testSolrClient.deleteById("doc1");
        testSolrClient.deleteById("doc2");
        testSolrClient.commit();

        runner.run(1,true, false);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 2);
        runner.assertAllFlowFilesContainAttribute(CoreAttributes.MIME_TYPE.key());

        testSolrClient.close();
    }

    @Test
    public void testInitialDateFilter() throws IOException, SolrServerException {
        final SolrClient testSolrClient = createEmbeddedSolrClient();
        final Date dateToFilter = Date.from(Instant.parse(DATE_STRING_LATER));
        final TestableProcessor proc = new TestableProcessor(testSolrClient);

        TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(GetSolr.DATE_FILTER, DATE_STRING_LATER);
        runner.setProperty(GetSolr.BATCH_SIZE, "1");

        SolrInputDocument doc10 = new SolrInputDocument();
        doc10.addField("id", "doc10");
        doc10.addField("created", dateToFilter);
        SolrInputDocument doc11 = new SolrInputDocument();
        doc11.addField("id", "doc11");
        doc11.addField("created", dateToFilter);

        testSolrClient.add(doc10);
        testSolrClient.add(doc11);
        testSolrClient.commit();

        runner.run(1,true, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 2);
        runner.assertAllFlowFilesContainAttribute(CoreAttributes.MIME_TYPE.key());

        testSolrClient.close();
    }

    @Test
    public void testPropertyModified() {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(GetSolr.BATCH_SIZE, "1");

        runner.run(1,false, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 10);
        runner.clearTransferState();

        // Change property contained in propertyNamesForActivatingClearState
        runner.setProperty(GetSolr.RETURN_FIELDS, "id,created,string_multi");
        runner.run(1, false, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 10);
        runner.clearTransferState();

        // Change property not contained in propertyNamesForActivatingClearState
        runner.setProperty(GetSolr.BATCH_SIZE, "2");
        runner.run(1, false, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testStateCleared() throws IOException {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(GetSolr.BATCH_SIZE, "1");

        runner.run(1,false, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 10);
        runner.clearTransferState();

        // run without clearing statemanager
        runner.run(1,false, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 0);
        runner.clearTransferState();

        // run with cleared statemanager
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.run(1, false, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 10);
        runner.clearTransferState();
    }

    @Test
    public void testRecordWriter() throws IOException, InitializationException {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(GetSolr.RETURN_TYPE, GetSolr.MODE_REC.getValue());
        runner.setProperty(GetSolr.RETURN_FIELDS, "id,created,integer_single");
        runner.setProperty(GetSolr.BATCH_SIZE, "10");

        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/test-schema.avsc")));

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
        runner.setProperty(SolrUtils.RECORD_WRITER, "writer");

        runner.run(1,false, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(CoreAttributes.MIME_TYPE.key());

        final MockFlowFile resultsFlowFile = runner.getFlowFilesForRelationship(GetSolr.REL_SUCCESS).get(0);
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode resultsNode = objectMapper.readTree(resultsFlowFile.getContent());

        final int total = StreamSupport.stream(resultsNode.spliterator(), false)
                .mapToInt(node -> node.get("integer_single").asInt())
                .sum();
        assertEquals(45, total);
    }

    @Test
    public void testForValidXml() {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(GetSolr.SOLR_QUERY, "id:doc1");
        runner.setProperty(GetSolr.RETURN_FIELDS, "id");
        runner.setProperty(GetSolr.BATCH_SIZE, "10");

        runner.run(1,false, true);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(GetSolr.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(CoreAttributes.MIME_TYPE.key());

        String expectedXml = "<docs><doc><field name=\"id\">doc1</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GetSolr.REL_SUCCESS).get(0)))));
    }

    private static class TestableProcessor extends GetSolr {
        private final SolrClient testSolrClient;

        public TestableProcessor(SolrClient testSolrClient) {
            this.testSolrClient = testSolrClient;
        }

        @Override
        protected SolrClient createSolrClient(ProcessContext context, String solrLocation) {
            return testSolrClient;
        }
    }
}
