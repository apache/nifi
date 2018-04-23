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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

public class TestQueryElasticsearchHttpNoHits {

        private TestRunner runner;

        @After
        public void teardown() {
                runner = null;
        }


        @Test
        public void testQueryElasticsearchOnTrigger_NoHits_NoHits() throws IOException {
                runner = TestRunners.newTestRunner(new QueryElasticsearchHttpTestProcessor());
                runner.setValidateExpressionUsage(true);
                runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

                runner.setProperty(QueryElasticsearchHttp.INDEX, "doc");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.TYPE, "status");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.QUERY,
                        "source:Twitter AND identifier:\"${identifier}\"");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.PAGE_SIZE, "2");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.ROUTING_QUERY_INFO_STRATEGY, QueryElasticsearchHttp.QueryInfoRouteStrategy.NOHIT.name());
                runner.assertValid();

                runner.setIncomingConnection(false);
                runAndVerify(0,1,0,true);
        }

        @Test
        public void testQueryElasticsearchOnTrigger_NoHits_Never() throws IOException {
                runner = TestRunners.newTestRunner(new QueryElasticsearchHttpTestProcessor());
                runner.setValidateExpressionUsage(true);
                runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

                runner.setProperty(QueryElasticsearchHttp.INDEX, "doc");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.TYPE, "status");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.QUERY,
                        "source:Twitter AND identifier:\"${identifier}\"");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.PAGE_SIZE, "2");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.ROUTING_QUERY_INFO_STRATEGY, QueryElasticsearchHttp.QueryInfoRouteStrategy.NEVER.name());
                runner.assertValid();

                runner.setIncomingConnection(false);
                runAndVerify(0,0,0,true);
        }

        @Test
        public void testQueryElasticsearchOnTrigger_NoHits_Always() throws IOException {
                runner = TestRunners.newTestRunner(new QueryElasticsearchHttpTestProcessor());
                runner.setValidateExpressionUsage(true);
                runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

                runner.setProperty(QueryElasticsearchHttp.INDEX, "doc");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.TYPE, "status");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.QUERY,
                        "source:Twitter AND identifier:\"${identifier}\"");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.PAGE_SIZE, "2");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.ROUTING_QUERY_INFO_STRATEGY, QueryElasticsearchHttp.QueryInfoRouteStrategy.ALWAYS.name());
                runner.assertValid();

                runner.setIncomingConnection(false);
                runAndVerify(0,1,0,true);
        }

        @Test
        public void testQueryElasticsearchOnTrigger_Hits_NoHits() throws IOException {
                runner = TestRunners.newTestRunner(new QueryElasticsearchHttpTestProcessor(true));
                runner.setValidateExpressionUsage(true);
                runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

                runner.setProperty(QueryElasticsearchHttp.INDEX, "doc");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.TYPE, "status");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.QUERY,
                        "source:Twitter AND identifier:\"${identifier}\"");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.PAGE_SIZE, "2");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.ROUTING_QUERY_INFO_STRATEGY, QueryElasticsearchHttp.QueryInfoRouteStrategy.NOHIT.name());
                runner.assertValid();

                runner.setIncomingConnection(false);
                runAndVerify(3,0,0,true);
        }

        @Test
        public void testQueryElasticsearchOnTrigger_Hits_Never() throws IOException {
                runner = TestRunners.newTestRunner(new QueryElasticsearchHttpTestProcessor(true));
                runner.setValidateExpressionUsage(true);
                runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

                runner.setProperty(QueryElasticsearchHttp.INDEX, "doc");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.TYPE, "status");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.QUERY,
                        "source:Twitter AND identifier:\"${identifier}\"");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.PAGE_SIZE, "2");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.ROUTING_QUERY_INFO_STRATEGY, QueryElasticsearchHttp.QueryInfoRouteStrategy.NEVER.name());
                runner.assertValid();

                runner.setIncomingConnection(false);
                runAndVerify(3,0,0,true);
        }

        @Test
        public void testQueryElasticsearchOnTrigger_Hits_Always() throws IOException {
                runner = TestRunners.newTestRunner(new QueryElasticsearchHttpTestProcessor(true));
                runner.setValidateExpressionUsage(true);
                runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

                runner.setProperty(QueryElasticsearchHttp.INDEX, "doc");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.TYPE, "status");
                runner.assertNotValid();
                runner.setProperty(QueryElasticsearchHttp.QUERY,
                        "source:Twitter AND identifier:\"${identifier}\"");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.PAGE_SIZE, "2");
                runner.assertValid();
                runner.setProperty(QueryElasticsearchHttp.ROUTING_QUERY_INFO_STRATEGY, QueryElasticsearchHttp.QueryInfoRouteStrategy.ALWAYS.name());
                runner.assertValid();

                runner.setIncomingConnection(false);
                runAndVerify(3,3,2,true);
        }



        private void runAndVerify(int expectedResults,int expectedQueryInfoResults,int expectedHits, boolean targetIsContent) {
                runner.enqueue("blah".getBytes(), new HashMap<String, String>() {
                        {
                                put("identifier", "28039652140");
                        }
                });

                // Running once should page through the no hit doc
                runner.run(1, true, true);

                runner.assertTransferCount(QueryElasticsearchHttp.REL_QUERY_INFO, expectedQueryInfoResults);
                if (expectedQueryInfoResults > 0) {
                        final MockFlowFile out = runner.getFlowFilesForRelationship(QueryElasticsearchHttp.REL_QUERY_INFO).get(0);
                        assertNotNull(out);
                        if (targetIsContent) {
                                out.assertAttributeEquals("es.query.hitcount", String.valueOf(expectedHits));
                                Assert.assertTrue(out.getAttribute("es.query.url").startsWith("http://127.0.0.1:9200/doc/status/_search?q=source:Twitter%20AND%20identifier:%22%22&size=2"));
                        }
                }

                runner.assertTransferCount(QueryElasticsearchHttp.REL_SUCCESS, expectedResults);
                if (expectedResults > 0) {
                        final MockFlowFile out = runner.getFlowFilesForRelationship(QueryElasticsearchHttp.REL_SUCCESS).get(0);
                        assertNotNull(out);
                        if (targetIsContent) {
                                out.assertAttributeEquals("filename", "abc-97b-ASVsZu_" + "vShwtGCJpGOObmuSqUJRUC3L_-SEND-S3");
                        }
                }
        }

        // By default, 3 files should go to Success
        private void runAndVerify(boolean targetIsContent) {
                runAndVerify(0,1,0, targetIsContent);
        }



        /**
         * A Test class that extends the processor in order to inject/mock behavior
         */
        private static class QueryElasticsearchHttpTestProcessor extends QueryElasticsearchHttp {
                Exception exceptionToThrow = null;
                OkHttpClient client;
                int goodStatusCode = 200;
                String goodStatusMessage = "OK";

                int badStatusCode;
                String badStatusMessage;
                int runNumber;

                boolean useHitPages;

                // query-page3 has no hits
                List<String> noHitPages = Arrays.asList(getDoc("query-page3.json"));
                List<String> hitPages = Arrays.asList(getDoc("query-page1.json"), getDoc("query-page2.json"),
                        getDoc("query-page3.json"));

                String expectedParam = null;

                public QueryElasticsearchHttpTestProcessor() {
                        this(false);
                }
                public QueryElasticsearchHttpTestProcessor(boolean useHitPages) {
                        this.useHitPages = useHitPages;
                }

                public void setExceptionToThrow(Exception exceptionToThrow) {
                        this.exceptionToThrow = exceptionToThrow;
                }

                /**
                 * Sets the status code and message for the 1st query
                 *
                 * @param code
                 *            The status code to return
                 * @param message
                 *            The status message
                 */
                void setStatus(int code, String message) {
                        this.setStatus(code, message, 1);
                }

                /**
                 * Sets an query parameter (name=value) expected to be at the end of the URL for the query operation
                 *
                 * @param param
                 *            The parameter to expect
                 */
                void setExpectedParam(String param) {
                        expectedParam = param;
                }

                /**
                 * Sets the status code and message for the runNumber-th query
                 *
                 * @param code
                 *            The status code to return
                 * @param message
                 *            The status message
                 * @param runNumber
                 *            The run number for which to set this status
                 */
                void setStatus(int code, String message, int runNumber) {
                        badStatusCode = code;
                        badStatusMessage = message;
                        this.runNumber = runNumber;
                }

                @Override
                protected void createElasticsearchClient(ProcessContext context) throws ProcessException {
                        client = mock(OkHttpClient.class);

                        OngoingStubbing<Call> stub = when(client.newCall(any(Request.class)));
                        List<String> pages;
                        if(useHitPages) {
                                pages = hitPages;
                        } else {
                                pages = noHitPages;
                        }

                        for (int i = 0; i < pages.size(); i++) {
                                String page = pages.get(i);
                                if (runNumber == i + 1) {
                                        stub = mockReturnDocument(stub, page, badStatusCode, badStatusMessage);
                                } else {
                                        stub = mockReturnDocument(stub, page, goodStatusCode, goodStatusMessage);
                                }
                        }
                }

                private OngoingStubbing<Call> mockReturnDocument(OngoingStubbing<Call> stub,
                        final String document, int statusCode, String statusMessage) {
                        return stub.thenAnswer(new Answer<Call>() {

                                @Override
                                public Call answer(InvocationOnMock invocationOnMock) throws Throwable {
                                        Request realRequest = (Request) invocationOnMock.getArguments()[0];
                                        assertTrue((expectedParam == null) || (realRequest.url().toString().endsWith(expectedParam)));
                                        Response mockResponse = new Response.Builder()
                                                .request(realRequest)
                                                .protocol(Protocol.HTTP_1_1)
                                                .code(statusCode)
                                                .message(statusMessage)
                                                .body(ResponseBody.create(MediaType.parse("application/json"), document))
                                                .build();
                                        final Call call = mock(Call.class);
                                        if (exceptionToThrow != null) {
                                                when(call.execute()).thenThrow(exceptionToThrow);
                                        } else {
                                                when(call.execute()).thenReturn(mockResponse);
                                        }
                                        return call;
                                }
                        });
                }

                protected OkHttpClient getClient() {
                        return client;
                }
        }

        private static String getDoc(String filename) {
                try {
                        return IOUtils.toString(QueryElasticsearchHttp.class.getClassLoader().getResourceAsStream(filename), StandardCharsets.UTF_8);
                } catch (IOException e) {
                        System.out.println("Error reading document " + filename);
                        return "";
                }
        }
}
