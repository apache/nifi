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
package org.apache.nifi.processors.elasticsearch;

import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPutElasticsearchHttp {

    private static byte[] docExample;
    private TestRunner runner;

    @Before
    public void once() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        docExample = IOUtils.toString(classloader.getResourceAsStream("DocumentExample.json"), StandardCharsets.UTF_8).getBytes();
    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testPutElasticSearchOnTriggerIndex() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false)); // no failures
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerUpdate() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false)); // no failures
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchHttp.INDEX_OP, "Update");
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerDelete() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false)); // no failures
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchHttp.INDEX_OP, "DELETE");
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerEL() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false)); // no failures
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "${es.url}");

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(AbstractElasticsearchHttpProcessor.CONNECT_TIMEOUT, "${connect.timeout}");
        runner.assertValid();

        runner.setVariable("es.url", "http://127.0.0.1:9200");
        runner.setVariable("connect.timeout", "5s");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerBadIndexOp() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false)); // no failures
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchHttp.INDEX_OP, "${no.attr}");
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttp.REL_FAILURE).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchInvalidConfig() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false)); // no failures
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.assertValid();
        runner.setProperty(PutElasticsearchHttp.INDEX_OP, "");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearchHttp.INDEX_OP, "index");
        runner.assertValid();
        runner.setProperty(PutElasticsearchHttp.INDEX_OP, "upsert");
        runner.assertNotValid();
    }

    @Test
    public void testPutElasticSearchOnTriggerWithFailures() throws IOException {
        PutElasticsearchTestProcessor processor = new PutElasticsearchTestProcessor(true);
        processor.setStatus(100, "Should fail");
        runner = TestRunners.newTestRunner(processor); // simulate failures
        runner.setValidateExpressionUsage(false);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_FAILURE, 1);
        runner.clearTransferState();

        processor.setStatus(500, "Should retry");
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_RETRY, 1);
    }

    @Test
    public void testPutElasticSearchOnTriggerWithConnectException() throws IOException {
        PutElasticsearchTestProcessor processor = new PutElasticsearchTestProcessor(true);
        processor.setStatus(-1, "Connection Exception");
        runner = TestRunners.newTestRunner(processor); // simulate failures
        runner.setValidateExpressionUsage(false);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_FAILURE, 1);
    }

    @Test
    public void testPutElasticsearchOnTriggerWithNoIdAttribute() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(true)); // simulate failures
        runner.setValidateExpressionUsage(false);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "2");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");

        runner.enqueue(docExample);
        runner.enqueue(docExample);
        runner.run(1, true, true);

        runner.assertTransferCount(PutElasticsearchHttp.REL_FAILURE, 1);
        runner.assertTransferCount(PutElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttp.REL_FAILURE).get(0);
        assertNotNull(out);
    }

    @Test
    public void testPutElasticsearchOnTriggerWithIndexFromAttribute() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false));
        runner.setValidateExpressionUsage(false);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttp.INDEX, "${i}");
        runner.setProperty(PutElasticsearchHttp.TYPE, "${type}");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652144");
            put("i", "doc");
            put("type", "status");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        runner.clearTransferState();

        // Now try an empty attribute value, should fail
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652144");
            put("type", "status");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_FAILURE, 1);
        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutElasticsearchHttp.REL_FAILURE).get(0);
        assertNotNull(out2);
    }

    @Test
    public void testPutElasticSearchOnTriggerWithInvalidIndexOp() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false)); // no failures
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.assertValid();
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.assertValid();

        runner.setProperty(PutElasticsearchHttp.INDEX_OP, "index_fail");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttp.REL_FAILURE).get(0);
        assertNotNull(out);
    }

    @Test
    public void testPutElasticSearchOnTriggerQueryParameter() throws IOException {
        PutElasticsearchTestProcessor p = new PutElasticsearchTestProcessor(false); // no failures
        p.setExpectedUrl("http://127.0.0.1:9200/_bulk?pipeline=my-pipeline");

        runner = TestRunners.newTestRunner(p);
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");

        // Set dynamic property, to be added to the URL as a query parameter
        runner.setProperty("pipeline", "my-pipeline");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
    }

    /**
     * A Test class that extends the processor in order to inject/mock behavior
     */
    private static class PutElasticsearchTestProcessor extends PutElasticsearchHttp {
        boolean responseHasFailures = false;
        OkHttpClient client;
        int statusCode = 200;
        String statusMessage = "OK";
        String expectedUrl = null;

        PutElasticsearchTestProcessor(boolean responseHasFailures) {
            this.responseHasFailures = responseHasFailures;
        }

        void setStatus(int code, String message) {
            statusCode = code;
            statusMessage = message;
        }

        void setExpectedUrl(String url) {
            expectedUrl = url;
        }

        @Override
        protected void createElasticsearchClient(ProcessContext context) throws ProcessException {
            client = mock(OkHttpClient.class);

            when(client.newCall(any(Request.class))).thenAnswer(new Answer<Call>() {

                @Override
                public Call answer(InvocationOnMock invocationOnMock) throws Throwable {
                    final Call call = mock(Call.class);
                    if (statusCode != -1) {
                        Request realRequest = (Request) invocationOnMock.getArguments()[0];
                        assertTrue((expectedUrl == null) || (expectedUrl.equals(realRequest.url().toString())));
                        StringBuilder sb = new StringBuilder("{\"took\": 1, \"errors\": \"");
                        sb.append(responseHasFailures);
                        sb.append("\", \"items\": [");
                        if (responseHasFailures) {
                            // This case is for a status code of 200 for the bulk response itself, but with an error (of 400) inside
                            sb.append("{\"index\":{\"_index\":\"doc\",\"_type\":\"status\",\"_id\":\"28039652140\",\"status\":\"400\",");
                            sb.append("\"error\":{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse [gender]\",");
                            sb.append("\"caused_by\":{\"type\":\"json_parse_exception\",\"reason\":\"Unexpected end-of-input in VALUE_STRING\\n at ");
                            sb.append("[Source: org.elasticsearch.common.io.stream.InputStreamStreamInput@1a2e3ac4; line: 1, column: 39]\"}}}},");
                        }
                        sb.append("{\"index\":{\"_index\":\"doc\",\"_type\":\"status\",\"_id\":\"28039652140\",\"status\":");
                        sb.append(statusCode);
                        sb.append(",\"_source\":{\"text\": \"This is a test document\"}}}");

                        sb.append("]}");
                        Response mockResponse = new Response.Builder()
                                .request(realRequest)
                                .protocol(Protocol.HTTP_1_1)
                                .code(statusCode)
                                .message(statusMessage)
                                .body(ResponseBody.create(MediaType.parse("application/json"), sb.toString()))
                                .build();

                        when(call.execute()).thenReturn(mockResponse);
                    } else {
                        when(call.execute()).thenThrow(ConnectException.class);
                    }
                    return call;
                }
            });
        }

        protected OkHttpClient getClient() {
            return client;
        }
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Integration test section below
    //
    // The tests below are meant to run on real ES instances, and are thus @Ignored during normal test execution.
    // However if you wish to execute them as part of a test phase, comment out the @Ignored line for each
    // desired test.
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Tests basic ES functionality against a local or test ES cluster
     */
    @Test
    @Ignore("Comment this out if you want to run against local or test ES")
    public void testPutElasticSearchBasic() {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticsearchHttp());
        runner.setValidateExpressionUsage(false);

        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});

        runner.enqueue(docExample);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 1);
    }

    @Test
    @Ignore("Comment this out if you want to run against local or test ES")
    public void testPutElasticSearchBatch() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticsearchHttp());
        runner.setValidateExpressionUsage(false);

        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttp.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttp.BATCH_SIZE, "100");
        runner.setProperty(PutElasticsearchHttp.TYPE, "status");
        runner.setProperty(PutElasticsearchHttp.ID_ATTRIBUTE, "doc_id");
        runner.assertValid();

        for (int i = 0; i < 100; i++) {
            long newId = 28039652140L + i;
            final String newStrId = Long.toString(newId);
            runner.enqueue(docExample, new HashMap<String, String>() {{
                put("doc_id", newStrId);
            }});
        }
        runner.run();
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttp.REL_SUCCESS, 100);
    }
}
