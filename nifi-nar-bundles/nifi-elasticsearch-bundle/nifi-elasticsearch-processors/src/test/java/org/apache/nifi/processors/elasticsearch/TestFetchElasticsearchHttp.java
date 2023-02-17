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
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFetchElasticsearchHttp {

    private InputStream docExample;
    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        docExample = classloader.getResourceAsStream("DocumentExample.json");
    }

    @AfterEach
    public void teardown() {
        runner = null;
    }

    @Test
    public void testFetchElasticsearchOnTriggerEL() {
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttpTestProcessor(true)); // all docs are found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "${es.url}");

        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        runner.removeProperty(FetchElasticsearchHttp.TYPE);
        runner.assertValid();
        runner.setProperty(FetchElasticsearchHttp.TYPE, "");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.assertValid();
        runner.setProperty(FetchElasticsearchHttp.TYPE, "${type}");
        runner.assertValid();
        runner.setProperty(FetchElasticsearchHttp.TYPE, "_doc");
        runner.assertValid(); // Valid because type can be _doc for 7.0+
        runner.setProperty(AbstractElasticsearchHttpProcessor.CONNECT_TIMEOUT, "${connect.timeout}");
        runner.assertValid();

        runner.setVariable("es.url", "http://127.0.0.1:9200");
        runner.setVariable("connect.timeout", "5s");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTrigger() {
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttpTestProcessor(true)); // all docs are found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerNoType() {
        final String ES_URL = "http://127.0.0.1:9200";
        final String DOC_ID = "28039652140";
        FetchElasticsearchHttpTestProcessor processor = new FetchElasticsearchHttpTestProcessor(true);
        runner = TestRunners.newTestRunner(processor); // all docs are found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, ES_URL);

        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.removeProperty(FetchElasticsearchHttp.TYPE);
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", DOC_ID);
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", DOC_ID);
        assertEquals("http://127.0.0.1:9200" + "/doc/_all/" + DOC_ID,
                processor.getURL().toString(), "URL doesn't match expected value when type is not supplied");
    }

    @Test
    public void testFetchElasticsearchOnTriggerWithFields() {
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttpTestProcessor(true)); // all docs are found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        runner.assertValid();
        runner.setProperty(FetchElasticsearchHttp.FIELDS, "id,, userinfo.location");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerWithDocNotFound() {
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttpTestProcessor(false)); // simulate doc not found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.removeProperty(FetchElasticsearchHttp.TYPE);
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");

        runner.setIncomingConnection(true);
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        // This test generates a "document not found"
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_NOT_FOUND, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_NOT_FOUND).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerWithServerErrorRetry() {
        FetchElasticsearchHttpTestProcessor processor = new FetchElasticsearchHttpTestProcessor(false);
        processor.setStatus(500, "Server error");
        runner = TestRunners.newTestRunner(processor); // simulate doc not found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        // This test generates a HTTP 500 "Server error"
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_RETRY, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_RETRY).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerWithServerFail() {
        FetchElasticsearchHttpTestProcessor processor = new FetchElasticsearchHttpTestProcessor(false);
        processor.setStatus(100, "Should fail");
        runner = TestRunners.newTestRunner(processor); // simulate doc not found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        // This test generates a HTTP 100
        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_FAILURE).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerWithServerFailNoIncomingFlowFile() {
        FetchElasticsearchHttpTestProcessor processor = new FetchElasticsearchHttpTestProcessor(false);
        processor.setStatus(100, "Should fail");
        runner = TestRunners.newTestRunner(processor); // simulate doc not found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");

        runner.setIncomingConnection(false);
        runner.run(1, true, true);

        // This test generates a HTTP 100 with no incoming flow file, so nothing should be transferred
        processor.getRelationships().forEach(relationship -> runner.assertTransferCount(relationship, 0));
        runner.assertTransferCount(FetchElasticsearchHttp.REL_FAILURE, 0);
    }

    @Test
    public void testFetchElasticsearchWithBadHosts() {
        runner = TestRunners.newTestRunner(new FetchElasticsearchHttpTestProcessor(false)); // simulate doc not found
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "127.0.0.1:9200");
        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");

        runner.assertNotValid();
    }

    @Test
    public void testSetupSecureClient() throws Exception {
        FetchElasticsearchHttpTestProcessor processor = new FetchElasticsearchHttpTestProcessor(true);
        runner = TestRunners.newTestRunner(processor);
        SSLContextService sslService = mock(SSLContextService.class);
        when(sslService.getIdentifier()).thenReturn("ssl-context");
        runner.addControllerService("ssl-context", sslService);
        runner.enableControllerService(sslService);
        runner.setProperty(FetchElasticsearchHttp.PROP_SSL_CONTEXT_SERVICE, "ssl-context");
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.removeProperty(FetchElasticsearchHttp.TYPE);
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");

        // Allow time for the controller service to fully initialize
        Thread.sleep(500);

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

    }

    @Test
    public void testFetchElasticsearchOnTriggerQueryParameter() {
        FetchElasticsearchHttpTestProcessor p = new FetchElasticsearchHttpTestProcessor(true); // all docs are found
        p.setExpectedUrl("http://127.0.0.1:9200/doc/status/28039652140?_source=id&myparam=myvalue");
        runner = TestRunners.newTestRunner(p);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.setProperty(FetchElasticsearchHttp.TYPE, "status");
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        runner.setProperty(FetchElasticsearchHttp.FIELDS, "id");

        // Set dynamic property, to be added to the URL as a query parameter
        runner.setProperty("myparam", "myvalue");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerQueryParameterNoType() {
        FetchElasticsearchHttpTestProcessor p = new FetchElasticsearchHttpTestProcessor(true); // all docs are found
        p.setExpectedUrl("http://127.0.0.1:9200/doc/_all/28039652140?_source=id&myparam=myvalue");
        runner = TestRunners.newTestRunner(p);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(FetchElasticsearchHttp.INDEX, "doc");
        runner.removeProperty(FetchElasticsearchHttp.TYPE);
        runner.setProperty(FetchElasticsearchHttp.DOC_ID, "${doc_id}");
        runner.setProperty(FetchElasticsearchHttp.FIELDS, "id");

        // Set dynamic property, to be added to the URL as a query parameter
        runner.setProperty("myparam", "myvalue");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearchHttp.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearchHttp.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    /**
     * A Test class that extends the processor in order to inject/mock behavior
     */
    private static class FetchElasticsearchHttpTestProcessor extends FetchElasticsearchHttp {
        boolean documentExists = true;
        Exception exceptionToThrow = null;
        OkHttpClient client;
        int statusCode = 200;
        String statusMessage = "OK";
        URL url = null;
        String expectedUrl = null;

        FetchElasticsearchHttpTestProcessor(boolean documentExists) {
            this.documentExists = documentExists;
        }

        public void setExceptionToThrow(Exception exceptionToThrow) {
            this.exceptionToThrow = exceptionToThrow;
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

            when(client.newCall(any(Request.class))).thenAnswer((Answer<Call>) invocationOnMock -> {
                Request realRequest = (Request) invocationOnMock.getArguments()[0];
                assertTrue((expectedUrl == null) || (expectedUrl.equals(realRequest.url().toString())));
                StringBuilder sb = new StringBuilder("{\"_index\":\"randomuser.me\",\"_type\":\"user\",\"_id\":\"0\",\"_version\":2,");
                if (documentExists) {
                    sb.append("\"found\":true,\"_source\":{\"gender\":\"female\",\"name\":{\"title\":\"Ms\",\"first\":\"Joan\",\"last\":\"Smith\"}}");
                } else {
                    sb.append("\"found\": false");
                }
                sb.append("}");
                Response mockResponse = new Response.Builder()
                        .request(realRequest)
                        .protocol(Protocol.HTTP_1_1)
                        .code(statusCode)
                        .message(statusMessage)
                        .body(ResponseBody.create(MediaType.parse("application/json"), sb.toString()))
                        .build();
                final Call call = mock(Call.class);
                when(call.execute()).thenReturn(mockResponse);
                return call;
            });
        }

        @Override
        protected Response sendRequestToElasticsearch(OkHttpClient client, URL url, String username, String password, String verb, RequestBody body) throws IOException {
            this.url = url;
            return super.sendRequestToElasticsearch(client, url, username, password, verb, body);
        }

        public URL getURL() {
            return url;
        }

        @Override
        protected OkHttpClient getClient() {
            return client;
        }
    }
}
