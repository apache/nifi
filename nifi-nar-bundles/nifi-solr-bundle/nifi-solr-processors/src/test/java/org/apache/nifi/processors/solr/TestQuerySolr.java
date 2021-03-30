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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xmlunit.matchers.CompareMatcher;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestQuerySolr {
    private static final String DEFAULT_SOLR_CORE = "testCollection";

    private static final String SOLR_CONNECT = "http://localhost:8443/solr";

    private static final String CREATED_DATETIME = "1970-01-01T12:00:00.000Z";

    private static final String FACET_RANGE_END = "1970-01-01T13:00:00.000Z";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SolrClient solrClient;

    @BeforeClass
    public static void createSolrClient() throws Exception {
        final String relPath = TestQuerySolr.class.getResource("/").getPath();

        solrClient = EmbeddedSolrServerFactory.create(EmbeddedSolrServerFactory.DEFAULT_SOLR_HOME,
                DEFAULT_SOLR_CORE, relPath);

        for (int i = 0; i < 10; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "doc" + i);
            doc.addField("created", CREATED_DATETIME);
            doc.addField("string_single", "single" + i + ".1");
            doc.addField("string_multi", "multi" + i + ".1");
            doc.addField("string_multi", "multi" + i + ".2");
            doc.addField("integer_single", i);
            doc.addField("integer_multi", 1);
            doc.addField("integer_multi", 2);
            doc.addField("integer_multi", 3);
            doc.addField("double_single", 0.5 + i);

            solrClient.add(doc);
        }
        solrClient.commit();
    }

    @AfterClass
    public static void closeSolrClient() throws IOException {
        solrClient.close();
    }

    private TestRunner createRunnerWithSolrClient(SolrClient solrClient) {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, SOLR_CONNECT);
        runner.setProperty(SolrUtils.COLLECTION, DEFAULT_SOLR_CORE);

        return runner;
    }

    private TestRunner createRunnerWithSolrCloudClient(SolrClient solrClient) {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, SOLR_CONNECT);
        runner.setProperty(SolrUtils.COLLECTION, DEFAULT_SOLR_CORE);

        return runner;
    }

    private TestRunner createRunner() {
        final TestableProcessor proc = new TestableProcessor(null);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, SOLR_CONNECT);
        runner.setProperty(SolrUtils.COLLECTION, DEFAULT_SOLR_CORE);

        return runner;
    }

    @Test
    public void testRepeatingParams() {
        TestRunner runner = createRunner();
        runner.enqueue(new byte[0]);

        runner.setProperty("facet.field.1", "123");
        runner.setProperty("facet.field.2", "other_field");
        runner.setProperty("f.123.facet.prefix", "pre");

        ProcessContext context = runner.getProcessContext();
        FlowFile flowFile = runner.getProcessSessionFactory().createSession().get();

        Map<String,String[]> solrParams = SolrUtils.getRequestParams(context, flowFile);

        String[] facet_fields = solrParams.get("facet.field");
        assertEquals(2, facet_fields.length);
        assertEquals("123", facet_fields[0]);
        assertEquals("other_field", facet_fields[1]);
        assertEquals("pre", solrParams.get("f.123.facet.prefix")[0]);
    }

    @Test
    public void testAllFacetCategories() throws IOException {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty("facet", "true");
        runner.setProperty("facet.field", "integer_multi");
        runner.setProperty("facet.interval", "integer_single");
        runner.setProperty("facet.interval.set.1", "[4,7]");
        runner.setProperty("facet.interval.set.2", "[5,7]");
        runner.setProperty("facet.range", "created");
        runner.setProperty("facet.range.start", CREATED_DATETIME);
        runner.setProperty("facet.range.end", FACET_RANGE_END);
        runner.setProperty("facet.range.gap", "+1SECOND");
        runner.setProperty("facet.query.1", "*:*");
        runner.setProperty("facet.query.2", "integer_multi:2");
        runner.setProperty("facet.query.3", "integer_multi:3");

        runner.enqueue(new ByteArrayInputStream(new byte[0]));
        runner.run(1, false);
        runner.assertTransferCount(QuerySolr.FACETS, 1);

        final MockFlowFile facetsFlowFile = runner.getFlowFilesForRelationship(QuerySolr.FACETS).get(0);
        final JsonNode facetsNode = OBJECT_MAPPER.readTree(facetsFlowFile.getContent());

        final int facetQueriesCount = StreamSupport.stream(facetsNode.get("facet_queries").spliterator(), false)
                .mapToInt(node -> node.get("count").asInt())
                .sum();
        assertEquals("Facet Queries Count not matched", 30, facetQueriesCount);

        final int facetFieldsCount = StreamSupport.stream(facetsNode.get("facet_fields").get("integer_multi").spliterator(), false)
                .mapToInt(node -> node.get("count").asInt())
                .sum();
        assertEquals("Facet Fields Count not matched", 30, facetFieldsCount);

        final int facetRangesCount = StreamSupport.stream(facetsNode.get("facet_ranges").get("created").spliterator(), false)
                .mapToInt(node -> node.get("count").asInt())
                .sum();
        assertEquals("Facet Ranges Count not matched",10, facetRangesCount);

        final int facetIntervalsCount = StreamSupport.stream(facetsNode.get("facet_intervals").get("integer_single").spliterator(), false)
                .mapToInt(node -> node.get("count").asInt())
                .sum();
        assertEquals("Facet Intervals Count not matched", 7, facetIntervalsCount);
    }

    @Test
    public void testFacetTrueButNull() throws IOException {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");

        runner.enqueue(new ByteArrayInputStream(new byte[0]));
        runner.run(1, false);

        runner.assertTransferCount(QuerySolr.RESULTS, 1);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);

        final MockFlowFile facetsFlowFile = runner.getFlowFilesForRelationship(QuerySolr.FACETS).get(0);
        final JsonNode facetsNode = OBJECT_MAPPER.readTree(facetsFlowFile.getContent());
        assertTrue("Facet Queries found", facetsNode.get("facet_queries").isEmpty());

        final MockFlowFile statsFlowFile = runner.getFlowFilesForRelationship(QuerySolr.STATS).get(0);
        final JsonNode statsNode = OBJECT_MAPPER.readTree(statsFlowFile.getContent());
        assertTrue("Stats found", statsNode.get("stats_fields").isEmpty());
    }

    @Test
    public void testStats() throws IOException {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty("stats", "true");

        final String statsField = "integer_single";
        runner.setProperty("stats.field",statsField);

        runner.enqueue(new ByteArrayInputStream(new byte[0]));
        runner.run(1, false);

        runner.assertTransferCount(QuerySolr.STATS, 1);

        final MockFlowFile statsFlowFile = runner.getFlowFilesForRelationship(QuerySolr.STATS).get(0);
        final JsonNode statsNode = OBJECT_MAPPER.readTree(statsFlowFile.getContent());

        final JsonNode statsFieldsNode = statsNode.get("stats_fields");
        assertNotNull("Stats Fields not found", statsFieldsNode);
        final JsonNode configuredStatsFieldNode = statsFieldsNode.get(statsField);

        assertEquals("0.0", configuredStatsFieldNode.get("min").asText());
        assertEquals("9.0", configuredStatsFieldNode.get("max").asText());
        assertEquals("10", configuredStatsFieldNode.get("count").asText());
        assertEquals("45.0", configuredStatsFieldNode.get("sum").asText());
        assertEquals("4.5", configuredStatsFieldNode.get("mean").asText());
    }

    @Test
    public void testRelationshipRoutings() {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");

        // Set request handler for request failure
        runner.setProperty(QuerySolr.SOLR_PARAM_REQUEST_HANDLER, "/nonexistentrequesthandler");

        // Processor has no input connection and fails
        runner.setNonLoopConnection(false);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(QuerySolr.FAILURE, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QuerySolr.FAILURE).get(0);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION_MESSAGE);
        runner.clearTransferState();

        // Processor has an input connection and fails
        runner.setNonLoopConnection(true);
        runner.enqueue(new byte[0]);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(QuerySolr.FAILURE, 1);

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.FAILURE).get(0);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION_MESSAGE);
        runner.clearTransferState();

        // Set request handler for successful request
        runner.setProperty(QuerySolr.SOLR_PARAM_REQUEST_HANDLER, "/select");

        // Processor has no input connection and succeeds
        runner.setNonLoopConnection(false);
        runner.run(1, false);
        runner.assertTransferCount(QuerySolr.RESULTS, 1);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        runner.clearTransferState();

        // Processor has an input connection and succeeds
        runner.setNonLoopConnection(true);
        runner.enqueue(new byte[0]);
        runner.run(1, false);
        runner.assertTransferCount(QuerySolr.RESULTS, 1);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 1);
        runner.assertAllFlowFilesContainAttribute(QuerySolr.ATTRIBUTE_SOLR_CONNECT);

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        flowFile = runner.getFlowFilesForRelationship(QuerySolr.FACETS).get(0);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        flowFile = runner.getFlowFilesForRelationship(QuerySolr.STATS).get(0);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        runner.clearTransferState();
    }

    @Test
    public void testExpressionLanguageForProperties() {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(QuerySolr.SOLR_PARAM_QUERY, "${query}");
        runner.setProperty(QuerySolr.SOLR_PARAM_REQUEST_HANDLER, "${handler}");
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "${fields}");
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "${sort}");
        runner.setProperty(QuerySolr.SOLR_PARAM_START, "${start}");
        runner.setProperty(QuerySolr.SOLR_PARAM_ROWS, "${rows}");

        runner.enqueue(new byte[0], new HashMap<String, String>(){{
            put("query", "id:(doc0 OR doc1 OR doc2 OR doc3)");
            put("handler", "/select");
            put("fields", "id");
            put("sort", "id desc");
            put("start", "1");
            put("rows", "2");
        }});
        runner.run(1, false);
        runner.assertTransferCount(QuerySolr.RESULTS, 1);

        String expectedXml = "<docs><doc><field name=\"id\">doc2</field></doc><doc><field name=\"id\">doc1</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0)))));
    }

    @Test
    public void testSingleFilterQuery() {
        TestRunner runner = createRunnerWithSolrClient(solrClient);
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id asc");
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");

        runner.setProperty("fq", "id:(doc2 OR doc3)");

        runner.enqueue(new byte[0]);
        runner.run(1, false);
        runner.assertTransferCount(QuerySolr.RESULTS, 1);

        String expectedXml = "<docs><doc><field name=\"id\">doc2</field></doc><doc><field name=\"id\">doc3</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0)))));
    }


    @Test
    public void testMultipleFilterQueries() {
        TestRunner runner = createRunnerWithSolrClient(solrClient);
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id asc");
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");

        runner.setProperty("fq.1", "id:(doc0 OR doc1 OR doc2 OR doc3)");
        runner.setProperty("fq.2", "id:(doc1 OR doc2 OR doc3 OR doc4)");
        runner.setProperty("fq.3", "id:(doc2 OR doc3 OR doc4 OR doc5)");

        runner.enqueue(new byte[0]);
        runner.run(1, false);
        runner.assertTransferCount(QuerySolr.RESULTS, 1);

        String expectedXml = "<docs><doc><field name=\"id\">doc2</field></doc><doc><field name=\"id\">doc3</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0)))));
    }

    @Test
    public void testStandardResponse() {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_QUERY, "id:(doc0 OR doc1)");
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id desc");

        runner.setNonLoopConnection(false);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(QuerySolr.RESULTS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);

        String expectedXml = "<docs><doc><field name=\"id\">doc1</field></doc><doc><field name=\"id\">doc0</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(flowFile))));
    }

    @Test
    public void testPreserveOriginalContent() {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_QUERY, "id:doc0");
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");

        String content = "test content 123";

        runner.enqueue(content);
        runner.run(1, false);
        runner.assertTransferCount(QuerySolr.RESULTS, 1);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 1);

        String expectedXml = "<docs><doc><field name=\"id\">doc0</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0)))));
        assertEquals(content, new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.ORIGINAL).get(0))));
    }

    @Test
    public void testRetrievalOfFullResults() {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id asc");
        runner.setProperty(QuerySolr.SOLR_PARAM_ROWS, "2");
        runner.setProperty(QuerySolr.AMOUNT_DOCUMENTS_TO_RETURN, QuerySolr.RETURN_ALL_RESULTS);

        runner.enqueue(new byte[0]);
        runner.run(1, false);
        runner.assertTransferCount(QuerySolr.RESULTS, 5);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 1);
        runner.assertTransferCount(QuerySolr.STATS, 0);
        runner.assertTransferCount(QuerySolr.FACETS, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QuerySolr.RESULTS);
        int documentCounter = 0;
        int startParam = 0;

        for (MockFlowFile flowFile : flowFiles) {
            Map<String,String> attributes = flowFile.getAttributes();
            assertEquals(attributes.get(QuerySolr.ATTRIBUTE_SOLR_START), Integer.toString(startParam));
            startParam += 2;

            StringBuilder expectedXml = new StringBuilder()
                    .append("<docs><doc><field name=\"id\">doc")
                    .append(documentCounter++)
                    .append("</field></doc><doc><field name=\"id\">doc")
                    .append(documentCounter++)
                    .append("</field></doc></docs>");
            assertThat(expectedXml.toString(), CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(flowFile))));
        }
    }

    @Test
    public void testRetrievalOfFullResults2() {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id asc");
        runner.setProperty(QuerySolr.SOLR_PARAM_ROWS, "3");
        runner.setProperty(QuerySolr.AMOUNT_DOCUMENTS_TO_RETURN, QuerySolr.RETURN_ALL_RESULTS);
        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");

        runner.enqueue(new byte[0]);
        runner.run(1, false);

        runner.assertTransferCount(QuerySolr.RESULTS, 4);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 1);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);
    }

    @Test
    public void testRetrievalOfFullResults3() {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id asc");
        runner.setProperty(QuerySolr.SOLR_PARAM_ROWS, "3");
        runner.setProperty(QuerySolr.AMOUNT_DOCUMENTS_TO_RETURN, QuerySolr.RETURN_ALL_RESULTS);
        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");

        runner.setNonLoopConnection(false);
        runner.run(1, false);

        runner.assertTransferCount(QuerySolr.RESULTS, 4);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 0);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);
    }


    @Test
    public void testRecordResponse() throws IOException, InitializationException {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.RETURN_TYPE, QuerySolr.MODE_REC.getValue());
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id,created,integer_single");
        runner.setProperty(QuerySolr.SOLR_PARAM_ROWS, "10");

        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/test-schema.avsc")));

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
        runner.setProperty(SolrUtils.RECORD_WRITER, "writer");

        runner.setNonLoopConnection(false);

        runner.run(1, false);
        runner.assertQueueEmpty();
        runner.assertTransferCount(QuerySolr.RESULTS, 1);

        final MockFlowFile resultsFlowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);
        final JsonNode resultsNode = OBJECT_MAPPER.readTree(resultsFlowFile.getContent());

        final int total = StreamSupport.stream(resultsNode.spliterator(), false)
                .mapToInt(node -> node.get("integer_single").asInt())
                .sum();
        assertEquals(45, total);
    }

    @Test
    public void testExceedStartParam() {
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_START, "10001");

        runner.setNonLoopConnection(false);

        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(QuerySolr.RESULTS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);

        assertEquals("10001", flowFile.getAttribute(QuerySolr.ATTRIBUTE_SOLR_START));
        assertEquals(0, runner.getContentAsByteArray(flowFile).length);
    }

    @Test
    public void testAttributesFailure() {
        TestRunner runner = createRunnerWithSolrCloudClient(solrClient);

        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");
        runner.setProperty(QuerySolr.SOLR_PARAM_REQUEST_HANDLER, "/nonexistentrequesthandler");

        runner.enqueue("");
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(QuerySolr.FAILURE, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QuerySolr.FAILURE).get(0);

        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_COLLECTION);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_QUERY);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION_MESSAGE);
    }

    @Test
    public void testAttributes() {
        TestRunner runner = createRunnerWithSolrCloudClient(solrClient);

        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");

        runner.enqueue("");
        runner.run(1, false);

        runner.assertTransferCount(QuerySolr.RESULTS, 1);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);
        Map<String, String> attributes = flowFile.getAttributes();

        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_COLLECTION);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_QUERY);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_START);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_ROWS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());

        assertEquals(SOLR_CONNECT, attributes.get(QuerySolr.ATTRIBUTE_SOLR_CONNECT));
        assertEquals(DEFAULT_SOLR_CORE, attributes.get(QuerySolr.ATTRIBUTE_SOLR_COLLECTION));

        assertEquals("q=*:*&qt=/select&start=0&rows=10&stats=true&facet=true", attributes.get(QuerySolr.ATTRIBUTE_SOLR_QUERY));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_STATUS));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_START));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_ROWS));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS));
        assertEquals(QuerySolr.MIME_TYPE_XML, attributes.get(CoreAttributes.MIME_TYPE.key()));

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.FACETS).get(0);
        attributes = flowFile.getAttributes();

        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_COLLECTION);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_QUERY);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_START);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_ROWS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());

        assertEquals(SOLR_CONNECT, attributes.get(QuerySolr.ATTRIBUTE_SOLR_CONNECT));
        assertEquals(DEFAULT_SOLR_CORE, attributes.get(QuerySolr.ATTRIBUTE_SOLR_COLLECTION));

        assertEquals("q=*:*&qt=/select&start=0&rows=10&stats=true&facet=true", attributes.get(QuerySolr.ATTRIBUTE_SOLR_QUERY));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_STATUS));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_START));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_ROWS));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS));
        assertEquals(QuerySolr.MIME_TYPE_JSON, attributes.get(CoreAttributes.MIME_TYPE.key()));

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.STATS).get(0);
        attributes = flowFile.getAttributes();

        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_COLLECTION);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_QUERY);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_START);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_ROWS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());

        assertEquals(SOLR_CONNECT, attributes.get(QuerySolr.ATTRIBUTE_SOLR_CONNECT));
        assertEquals(DEFAULT_SOLR_CORE, attributes.get(QuerySolr.ATTRIBUTE_SOLR_COLLECTION));

        assertEquals("q=*:*&qt=/select&start=0&rows=10&stats=true&facet=true", attributes.get(QuerySolr.ATTRIBUTE_SOLR_QUERY));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_STATUS));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_START));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_ROWS));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS));
        assertEquals(QuerySolr.MIME_TYPE_JSON, attributes.get(CoreAttributes.MIME_TYPE.key()));

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.ORIGINAL).get(0);
        attributes = flowFile.getAttributes();

        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_COLLECTION);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_QUERY);

        assertEquals(SOLR_CONNECT, attributes.get(QuerySolr.ATTRIBUTE_SOLR_CONNECT));
        assertEquals(DEFAULT_SOLR_CORE, attributes.get(QuerySolr.ATTRIBUTE_SOLR_COLLECTION));
    }

    private static class TestableProcessor extends QuerySolr {
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
