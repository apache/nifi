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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestPutElasticsearch {

    private InputStream docExample;
    private TestRunner runner;

    @Before
    public void setUp() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        docExample = classloader.getResourceAsStream("DocumentExample.json");
    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testPutElasticSearchOnTrigger() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false)); // no failures
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearch.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerWithFailures() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(true)); // simulate failures
        runner.setValidateExpressionUsage(false);
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearch.REL_FAILURE).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticsearchOnTriggerWithExceptions() throws IOException {
        PutElasticsearchTestProcessor processor = new PutElasticsearchTestProcessor(false);
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");

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
            put("doc_id", "28039652142");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        runner.clearTransferState();

        // Node Closed exception
        processor.setExceptionToThrow(new NodeClosedException(mock(StreamInput.class)));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652143");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        runner.clearTransferState();

        // Elasticsearch Parse exception
        processor.setExceptionToThrow(new ElasticsearchParseException("test"));
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652144");
        }});
        runner.run(1, true, true);

        // This test generates an exception on execute(),routes to failure
        runner.assertTransferCount(PutElasticsearch.REL_FAILURE, 1);
    }

    @Test
    public void testPutElasticsearchOnTriggerWithNoIdAttribute() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(true)); // simulate failures
        runner.setValidateExpressionUsage(false);
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");

        runner.enqueue(docExample);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearch.REL_FAILURE).get(0);
        assertNotNull(out);
    }

    /**
     * A Test class that extends the processor in order to inject/mock behavior
     */
    private static class PutElasticsearchTestProcessor extends PutElasticsearch {
        boolean responseHasFailures = false;
        Exception exceptionToThrow = null;

        public PutElasticsearchTestProcessor(boolean responseHasFailures) {
            this.responseHasFailures = responseHasFailures;
        }

        public void setExceptionToThrow(Exception exceptionToThrow) {
            this.exceptionToThrow = exceptionToThrow;
        }

        @Override
        public void createElasticsearchClient(ProcessContext context) throws ProcessException {
            Client mockClient = mock(Client.class);
            BulkRequestBuilder bulkRequestBuilder = spy(new BulkRequestBuilder(mockClient, BulkAction.INSTANCE));
            if (exceptionToThrow != null) {
                doThrow(exceptionToThrow).when(bulkRequestBuilder).execute();
            } else {
                doReturn(new MockBulkRequestBuilderExecutor(responseHasFailures)).when(bulkRequestBuilder).execute();
            }
            when(mockClient.prepareBulk()).thenReturn(bulkRequestBuilder);

            IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(mockClient, IndexAction.INSTANCE);
            when(mockClient.prepareIndex(anyString(), anyString(), anyString())).thenReturn(indexRequestBuilder);

            esClient.set(mockClient);
        }

        private static class MockBulkRequestBuilderExecutor
                extends AdapterActionFuture<BulkResponse, ActionListener<BulkResponse>>
                implements ListenableActionFuture<BulkResponse> {

            boolean responseHasFailures = false;

            public MockBulkRequestBuilderExecutor(boolean responseHasFailures) {
                this.responseHasFailures = responseHasFailures;
            }

            @Override
            protected BulkResponse convert(ActionListener<BulkResponse> bulkResponseActionListener) {
                return null;
            }

            @Override
            public void addListener(ActionListener<BulkResponse> actionListener) {

            }

            @Override
            public BulkResponse get() throws InterruptedException, ExecutionException {
                BulkResponse response = mock(BulkResponse.class);
                when(response.hasFailures()).thenReturn(responseHasFailures);
                BulkItemResponse item = mock(BulkItemResponse.class);
                when(item.getItemId()).thenReturn(1);
                when(item.isFailed()).thenReturn(true);
                when(response.getItems()).thenReturn(new BulkItemResponse[]{item});
                return response;
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
    public void testPutElasticSearchBasic() {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticsearch());
        runner.setValidateExpressionUsage(false);

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");

        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});


        runner.enqueue(docExample);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_SUCCESS, 1);
    }

    @Test
    @Ignore("Comment this out if you want to run against local or test ES")
    public void testPutElasticSearchBatch() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticsearch());
        runner.setValidateExpressionUsage(false);

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "100");

        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");
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

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_SUCCESS, 100);
    }

    /**
     * Convert an input stream to a stream
     *
     * @param is input the input stream
     * @return return the converted input stream as a string
     */
    static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
