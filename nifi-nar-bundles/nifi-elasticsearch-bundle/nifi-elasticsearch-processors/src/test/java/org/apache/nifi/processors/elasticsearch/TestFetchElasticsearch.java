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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.VariableRegistryUtils;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestFetchElasticsearch {

    private InputStream docExample;
    private TestRunner runner;
    private VariableRegistry variableRegistry;

    @Before
    public void setUp() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        docExample = classloader.getResourceAsStream("DocumentExample.json");
        variableRegistry = VariableRegistryUtils.createSystemVariableRegistry();

    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testFetchElasticsearchOnTrigger() throws IOException {
        runner = TestRunners.newTestRunner(new FetchElasticsearchTestProcessor(true)); // all docs are found
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerWithFailures() throws IOException {
        runner = TestRunners.newTestRunner(new FetchElasticsearchTestProcessor(false)); // simulate doc not found
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        // This test generates a "document not found"
        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_NOT_FOUND, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_NOT_FOUND).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchWithBadHosts() throws IOException {
        runner = TestRunners.newTestRunner(new FetchElasticsearchTestProcessor(false)); // simulate doc not found
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "http://127.0.0.1:9300,127.0.0.2:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");

        runner.assertNotValid();
    }

    @Test
    public void testFetchElasticsearchOnTriggerWithExceptions() throws IOException {
        FetchElasticsearchTestProcessor processor = new FetchElasticsearchTestProcessor(true);
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");

        // No Node Available exception
        processor.setExceptionToThrow(new NoNodeAvailableException("test"));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        runner.clearTransferState();

        // Elasticsearch Timeout exception
        processor.setExceptionToThrow(new ElasticsearchTimeoutException("test"));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652141");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        runner.clearTransferState();

        // Receive Timeout Transport exception
        processor.setExceptionToThrow(new ReceiveTimeoutTransportException(mock(StreamInput.class)));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652141");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        runner.clearTransferState();

        // Node Closed exception
        processor.setExceptionToThrow(new NodeClosedException(mock(StreamInput.class)));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652141");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        runner.clearTransferState();

        // Elasticsearch Parse exception
        processor.setExceptionToThrow(new ElasticsearchParseException("test"));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652141");
        }});
        runner.run(1, true, true);

        // This test generates an exception on execute(),routes to failure
        runner.assertTransferCount(FetchElasticsearch.REL_FAILURE, 1);
    }

    @Test(expected = ProcessException.class)
    public void testCreateElasticsearchClientWithException() throws ProcessException {
        FetchElasticsearchTestProcessor processor = new FetchElasticsearchTestProcessor(true) {
            @Override
            protected TransportClient getTransportClient(Settings.Builder settingsBuilder, String shieldUrl,
                                                         String username, String password)
                    throws MalformedURLException {
                throw new MalformedURLException();
            }
        };

        MockProcessContext context = new MockProcessContext(processor, variableRegistry);
        processor.initialize(new MockProcessorInitializationContext(processor, context));
        processor.callCreateElasticsearchClient(context);
    }

    @Test
    public void testSetupSecureClient() throws Exception {
        FetchElasticsearchTestProcessor processor = new FetchElasticsearchTestProcessor(true);
        runner = TestRunners.newTestRunner(processor);
        SSLContextService sslService = mock(SSLContextService.class);
        when(sslService.getIdentifier()).thenReturn("ssl-context");
        runner.addControllerService("ssl-context", sslService);
        runner.enableControllerService(sslService);
        runner.setProperty(FetchElasticsearch.PROP_SSL_CONTEXT_SERVICE, "ssl-context");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "doc");
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");

        // Allow time for the controller service to fully initialize
        Thread.sleep(500);

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

    }

    /**
     * A Test class that extends the processor in order to inject/mock behavior
     */
    private static class FetchElasticsearchTestProcessor extends FetchElasticsearch {
        boolean documentExists = true;
        Exception exceptionToThrow = null;

        public FetchElasticsearchTestProcessor(boolean documentExists) {
            this.documentExists = documentExists;
        }

        public void setExceptionToThrow(Exception exceptionToThrow) {
            this.exceptionToThrow = exceptionToThrow;
        }

        @Override
        protected TransportClient getTransportClient(Settings.Builder settingsBuilder, String shieldUrl,
                                                     String username, String password)
                throws MalformedURLException {
            TransportClient mockClient = mock(TransportClient.class);
            GetRequestBuilder getRequestBuilder = spy(new GetRequestBuilder(mockClient, GetAction.INSTANCE));
            if (exceptionToThrow != null) {
                doThrow(exceptionToThrow).when(getRequestBuilder).execute();
            } else {
                doReturn(new MockGetRequestBuilderExecutor(documentExists)).when(getRequestBuilder).execute();
            }
            when(mockClient.prepareGet(anyString(), anyString(), anyString())).thenReturn(getRequestBuilder);

            return mockClient;
        }

        public void callCreateElasticsearchClient(ProcessContext context) {
            createElasticsearchClient(context);
        }

        private static class MockGetRequestBuilderExecutor
                extends AdapterActionFuture<GetResponse, ActionListener<GetResponse>>
                implements ListenableActionFuture<GetResponse> {

            boolean documentExists = true;

            public MockGetRequestBuilderExecutor(boolean documentExists) {
                this.documentExists = documentExists;
            }


            @Override
            protected GetResponse convert(ActionListener<GetResponse> bulkResponseActionListener) {
                return null;
            }

            @Override
            public void addListener(ActionListener<GetResponse> actionListener) {

            }

            @Override
            public GetResponse get() throws InterruptedException, ExecutionException {
                GetResponse response = mock(GetResponse.class);
                when(response.isExists()).thenReturn(documentExists);
                when(response.getSourceAsBytes()).thenReturn("Success".getBytes());
                when(response.getSourceAsString()).thenReturn("Success");
                return response;
            }

            @Override
            public GetResponse actionGet() {
                try {
                    return get();
                } catch (Exception e) {
                    fail(e.getMessage());
                }
                return null;
            }
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
    public void testFetchElasticsearchBasic() {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new FetchElasticsearch());
        runner.setValidateExpressionUsage(true);

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(FetchElasticsearch.INDEX, "doc");

        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});


        runner.enqueue(docExample);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 1);
    }

    @Test
    @Ignore("Comment this out if you want to run against local or test ES")
    public void testFetchElasticsearchBatch() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new FetchElasticsearch());
        runner.setValidateExpressionUsage(true);

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "doc");

        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${doc_id}");
        runner.assertValid();


        String message = convertStreamToString(docExample);
        for (int i = 0; i < 100; i++) {

            long newId = 28039652140L + i;
            final String newStrId = Long.toString(newId);
            runner.enqueue(message.getBytes(), new HashMap<String, String>() {{
                put("doc_id", newStrId);
            }});

        }

        runner.run();

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 100);
    }

    /**
     * Convert an input stream to a stream
     *
     * @param is input the input stream
     * @return return the converted input stream as a string
     */
    static String convertStreamToString(InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
