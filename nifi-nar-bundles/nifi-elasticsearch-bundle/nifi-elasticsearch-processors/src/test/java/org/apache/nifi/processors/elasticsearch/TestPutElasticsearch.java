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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventType;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");

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
        assertFalse(runner.getProvenanceEvents().isEmpty());
        runner.getProvenanceEvents().forEach(event -> assertEquals(event.getEventType(), ProvenanceEventType.SEND));
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearch.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerEL() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false)); // no failures
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "${cluster.name}");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "${hosts}");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "${ping.timeout}");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "${sampler.interval}");

        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");
        runner.assertValid();
        runner.setVariable("cluster.name", "elasticsearch");
        runner.setVariable("hosts", "127.0.0.1:9300");
        runner.setVariable("ping.timeout", "5s");
        runner.setVariable("sampler.interval", "5s");


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
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "2");
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652141");
        }});
        runner.run(1, true, true);

        runner.assertTransferCount(PutElasticsearch.REL_FAILURE, 1);
        runner.assertTransferCount(PutElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearch.REL_FAILURE).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticsearchOnTriggerWithExceptions() throws IOException {
        PutElasticsearchTestProcessor processor = new PutElasticsearchTestProcessor(false);
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.setProperty(PutElasticsearch.TYPE, "status");
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
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
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

    @Test
    public void testPutElasticsearchOnTriggerWithIndexFromAttribute() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false));
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(PutElasticsearch.INDEX, "${i}");
        runner.setProperty(PutElasticsearch.TYPE, "${type}");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652144");
            put("i", "doc");
            put("type", "status");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearch.REL_SUCCESS).get(0);
        assertNotNull(out);
        runner.clearTransferState();

        // Now try an empty attribute value, should fail
        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652144");
            put("type", "status");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_RETRY, 1);
        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutElasticsearch.REL_RETRY).get(0);
        assertNotNull(out2);
    }

    @Test
    public void testPutElasticSearchOnTriggerWithInvalidIndexOp() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchTestProcessor(false)); // no failures
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(PutElasticsearch.INDEX, "doc");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "doc_id");
        runner.assertValid();

        runner.setProperty(PutElasticsearch.INDEX_OP, "index_fail");
        runner.assertValid();

        runner.enqueue(docExample, new HashMap<String, String>() {{
            put("doc_id", "28039652140");
        }});
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
            final Client mockClient = mock(Client.class);
            BulkRequestBuilder bulkRequestBuilder = spy(new BulkRequestBuilder(mockClient, BulkAction.INSTANCE));
            if (exceptionToThrow != null) {
                doThrow(exceptionToThrow).when(bulkRequestBuilder).execute();
            } else {
                doReturn(new MockBulkRequestBuilderExecutor(responseHasFailures)).when(bulkRequestBuilder).execute();
            }
            when(mockClient.prepareBulk()).thenReturn(bulkRequestBuilder);

            when(mockClient.prepareIndex(anyString(), anyString(), anyString())).thenAnswer(new Answer<IndexRequestBuilder>() {
                @Override
                public IndexRequestBuilder answer(InvocationOnMock invocationOnMock) throws Throwable {
                    Object[] args = invocationOnMock.getArguments();
                    String arg1 = (String) args[0];
                    if (arg1.isEmpty()) {
                        throw new NoNodeAvailableException("Needs index");
                    }
                    String arg2 = (String) args[1];
                    if (arg2.isEmpty()) {
                        throw new NoNodeAvailableException("Needs doc type");
                    } else {
                        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(mockClient, IndexAction.INSTANCE);
                        return indexRequestBuilder;
                    }
                }
            });

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
                BulkItemResponse item1 = mock(BulkItemResponse.class);
                BulkItemResponse item2 = mock(BulkItemResponse.class);
                when(item1.getItemId()).thenReturn(1);
                when(item1.isFailed()).thenReturn(true);
                BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
                when(failure.getMessage()).thenReturn("Bad message");
                when(item1.getFailure()).thenReturn(failure);
                when(item2.getItemId()).thenReturn(2);
                when(item2.isFailed()).thenReturn(false);
                when(response.getItems()).thenReturn(new BulkItemResponse[]{item1, item2});
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

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");

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

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchTransportClientProcessor.SAMPLER_INTERVAL, "5s");
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
