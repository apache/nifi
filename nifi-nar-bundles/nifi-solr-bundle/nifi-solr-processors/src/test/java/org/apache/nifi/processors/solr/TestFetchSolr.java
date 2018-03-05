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

import com.google.gson.stream.JsonReader;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xmlunit.matchers.CompareMatcher;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class TestFetchSolr {
    static final String DEFAULT_SOLR_CORE = "testCollection";

    private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
    static {
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    private SolrClient solrClient;

    @Before
    public void setup() {

        try {

            // create an EmbeddedSolrServer for the processor to use
            String relPath = getClass().getProtectionDomain().getCodeSource()
                    .getLocation().getFile() + "../../target";

            solrClient = EmbeddedSolrServerFactory.create(EmbeddedSolrServerFactory.DEFAULT_SOLR_HOME,
                    DEFAULT_SOLR_CORE, relPath);

            for (int i = 0; i < 10; i++) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("id", "doc" + i);
                Date date = new Date();
                doc.addField("created", df.format(date));
                doc.addField("string_single", "single" + i + ".1");
                doc.addField("string_multi", "multi" + i + ".1");
                doc.addField("string_multi", "multi" + i + ".2");
                doc.addField("integer_single", i);
                doc.addField("integer_multi", 1);
                doc.addField("integer_multi", 2);
                doc.addField("integer_multi", 3);
                doc.addField("double_single", 0.5 + i);

                solrClient.add(doc);
                System.out.println(doc.getField("created").getValue());

            }
            solrClient.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @After
    public void teardown() {
        try {
            solrClient.close();
        } catch (Exception e) {
        }
    }

    @Test
    public void testAllFacetCategories() throws IOException {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(SolrUtils.COLLECTION, "testCollection");
        runner.setProperty(FetchSolr.SOLR_QUERY_STRING, "q=*:*" +
                "&facet=true&facet.interval=integer_single&facet.interval.set=[4,7]&facet.interval.set=[5,7]" +
                "&facet.field=integer_multi&facet.query=integer_multi:2&stats=true&stats.field=integer_multi" +
                "&facet.range=created&facet.range.start=NOW/MINUTE&facet.range.end=NOW/MINUTE%2B1MINUTE&facet.range.gap=%2B20SECOND" +
                "&facet.query=*:*&facet.query=integer_multi:2&facet.query=integer_multi:3"
        );
        runner.enqueue(new ByteArrayInputStream("test".getBytes()));
        runner.run();
        runner.assertTransferCount(FetchSolr.FACETS, 1);

        JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(
                runner.getContentAsByteArray(runner.getFlowFilesForRelationship(FetchSolr.FACETS).get(0)))));
        reader.beginObject();
        while (reader.hasNext()) {
            String name = reader.nextName();
            if (name.equals("facet_queries")) {
                assertEquals(returnCheckSumForArrayOfJsonObjects(reader), 30);
            } else if (name.equals("facet_fields")) {
                reader.beginObject();
                assertEquals(reader.nextName(), "integer_multi");
                assertEquals(returnCheckSumForArrayOfJsonObjects(reader), 30);
                reader.endObject();
            } else if (name.equals("facet_ranges")) {
                reader.beginObject();
                assertEquals(reader.nextName(), "created");
                assertEquals(returnCheckSumForArrayOfJsonObjects(reader), 10);
                reader.endObject();
            } else if (name.equals("facet_intervals")) {
                reader.beginObject();
                assertEquals(reader.nextName(), "integer_single");
                assertEquals(returnCheckSumForArrayOfJsonObjects(reader), 7);
                reader.endObject();
            }
        }
        reader.endObject();
        reader.close();
    }

    private int returnCheckSumForArrayOfJsonObjects(JsonReader reader) throws IOException {
        int checkSum = 0;
        reader.beginArray();
        while (reader.hasNext()) {
            reader.beginObject();
            while (reader.hasNext()) {
                if (reader.nextName().equals("count"))
                    checkSum += reader.nextInt();
                else
                    reader.skipValue();
            }
            reader.endObject();
        }
        reader.endArray();
        return checkSum;
    }

    @Test
    public void testFacetTrueButNull() throws IOException {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(SolrUtils.COLLECTION, "testCollection");
        runner.setProperty(FetchSolr.SOLR_QUERY_STRING, "q=*:*&facet=true&stats=true");
        runner.enqueue(new ByteArrayInputStream("test".getBytes()));
        runner.run();

        runner.assertTransferCount(FetchSolr.FACETS, 1);
        runner.assertTransferCount(FetchSolr.STATS, 1);

        // Check for empty nestet Objects in JSON
        JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(
                runner.getContentAsByteArray(runner.getFlowFilesForRelationship(FetchSolr.FACETS).get(0)))));
        reader.beginObject();
        while (reader.hasNext()) {
            if (reader.nextName().equals("facet_queries")) {
                reader.beginArray();
                assertFalse(reader.hasNext());
                reader.endArray();
            } else {
                reader.beginObject();
                assertFalse(reader.hasNext());
                reader.endObject();
            }
        }
        reader.endObject();

        JsonReader reader_stats = new JsonReader(new InputStreamReader(new ByteArrayInputStream(
                runner.getContentAsByteArray(runner.getFlowFilesForRelationship(FetchSolr.STATS).get(0)))));
        reader_stats.beginObject();
        assertEquals(reader_stats.nextName(), "stats_fields");
        reader_stats.beginObject();
        assertFalse(reader_stats.hasNext());
        reader_stats.endObject();
        reader_stats.endObject();

        reader.close();
        reader_stats.close();
    }

    @Test
    public void testStats() throws IOException {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(SolrUtils.COLLECTION, "testCollection");
        runner.setProperty(FetchSolr.SOLR_QUERY_STRING, "q=*:*&stats=true&stats.field=integer_single");
        runner.enqueue(new ByteArrayInputStream("test".getBytes()));
        runner.run();

        runner.assertTransferCount(FetchSolr.STATS, 1);
        JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(
                runner.getContentAsByteArray(runner.getFlowFilesForRelationship(FetchSolr.STATS).get(0)))));
        reader.beginObject();
        assertEquals(reader.nextName(), "stats_fields");
        reader.beginObject();
        assertEquals(reader.nextName(), "integer_single");
        reader.beginObject();
        while (reader.hasNext()) {
            String name = reader.nextName();
            switch (name) {
                case "min": assertEquals(reader.nextString(), "0.0"); break;
                case "max": assertEquals(reader.nextString(), "9.0"); break;
                case "count": assertEquals(reader.nextInt(), 10); break;
                case "sum": assertEquals(reader.nextString(), "45.0"); break;
                default: reader.skipValue(); break;
            }
        }
        reader.endObject();
        reader.endObject();
        reader.endObject();
        reader.close();
    }

    @Test
    public void testFailure() {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(SolrUtils.COLLECTION, "testCollection");
        runner.setProperty(FetchSolr.REQUEST_HANDLER, "/sel");

        runner.setNonLoopConnection(false);
        runner.run();

        runner.assertTransferCount(FetchSolr.FAILURE, 1);
        runner.assertTransferCount(FetchSolr.ORIGINAL, 0);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchSolr.FAILURE).get(0);
        flowFile.assertAttributeExists(FetchSolr.EXCEPTION);
        flowFile.assertAttributeExists(FetchSolr.EXCEPTION_MESSAGE);
    }

    @Test
    public void testExpressionLanguage() {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        ByteArrayInputStream is = new ByteArrayInputStream(new byte[0]);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(SolrUtils.COLLECTION, "testCollection");
        runner.setProperty(FetchSolr.SOLR_QUERY_STRING, "${query}");

        runner.enqueue(new byte[0], new HashMap<String,String>(){{
            put("query", "q=id:doc0&fl=id");
        }});
        runner.run();
        runner.assertTransferCount(FetchSolr.RESULTS, 1);

        String expectedXml = "<docs><doc boost=\"1.0\"><field name=\"id\">doc0</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(FetchSolr.RESULTS).get(0)))));
    }

    @Test
    public void testStandardResponse() {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(SolrUtils.COLLECTION, "testCollection");
        runner.setProperty(FetchSolr.SOLR_QUERY_STRING, "q=id:(doc0 OR doc1)&fl=id&sort=id desc");

        runner.setNonLoopConnection(false);
        runner.run();
        runner.assertTransferCount(FetchSolr.RESULTS, 1);
        runner.assertTransferCount(FetchSolr.ORIGINAL, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchSolr.RESULTS).get(0);
        flowFile.assertAttributeExists(FetchSolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(FetchSolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(FetchSolr.ATTRIBUTE_QUERY_TIME);

        String expectedXml = "<docs><doc boost=\"1.0\"><field name=\"id\">doc1</field></doc><doc boost=\"1.0\"><field name=\"id\">doc0</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(FetchSolr.RESULTS).get(0)))));
    }

    @Test
    public void testRecordResponse() throws IOException, InitializationException {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(FetchSolr.RETURN_TYPE, FetchSolr.MODE_REC.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.setProperty(SolrUtils.COLLECTION, "testCollection");
        runner.setProperty(FetchSolr.SOLR_QUERY_STRING, "q=*:*&fl=id,created,integer_single&rows=10");

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

        runner.run(1,true, true);
        runner.assertQueueEmpty();
        runner.assertTransferCount(FetchSolr.RESULTS, 1);
        runner.assertTransferCount(FetchSolr.ORIGINAL, 1);
        for (MockFlowFile flowFile : runner.getFlowFilesForRelationship(FetchSolr.RESULTS))
            System.out.println(new String(runner.getContentAsByteArray(flowFile)));

        JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(
                runner.getContentAsByteArray(runner.getFlowFilesForRelationship(FetchSolr.RESULTS).get(0)))));
        reader.beginArray();
        int controlScore = 0;
        while (reader.hasNext()) {
            reader.beginObject();
            while (reader.hasNext()) {
                if (reader.nextName().equals("integer_single"))
                    controlScore += reader.nextInt();
                else
                    reader.skipValue();
            }
            reader.endObject();
        }
        reader.close();
        assertEquals(controlScore, 45);
    }

    // Override createSolrClient and return the passed in SolrClient
    private class TestableProcessor extends FetchSolr {
        private SolrClient solrClient;

        public TestableProcessor(SolrClient solrClient) {
            this.solrClient = solrClient;
        }
        @Override
        protected SolrClient createSolrClient(ProcessContext context, String solrLocation) {
            return solrClient;
        }
    }
}
