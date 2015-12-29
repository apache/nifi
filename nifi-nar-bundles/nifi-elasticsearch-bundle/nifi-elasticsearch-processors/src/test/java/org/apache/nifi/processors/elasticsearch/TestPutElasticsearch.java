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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.AbstractListenableActionFuture;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestPutElasticsearch {

    private InputStream twitterExample;
    private TestRunner runner;

    @Before
    public void setUp() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        twitterExample = classloader
                .getResourceAsStream("TweetExample.json");

    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testPutElasticSearchOnTrigger() throws IOException {
        runner = TestRunners.newTestRunner(new ElasticsearchTestProcessor(false)); // no failures
        runner.setValidateExpressionUsage(false);
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(PutElasticsearch.INDEX, "tweet");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "tweet_id");
        runner.assertValid();

        runner.enqueue(twitterExample, new HashMap<String, String>() {{
            put("tweet_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearch.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("tweet_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerWithFailures() throws IOException {
        runner = TestRunners.newTestRunner(new ElasticsearchTestProcessor(true)); // simulate failures
        runner.setValidateExpressionUsage(false);
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(PutElasticsearch.INDEX, "tweet");
        runner.setProperty(PutElasticsearch.TYPE, "status");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearch.ID_ATTRIBUTE, "tweet_id");

        runner.enqueue(twitterExample, new HashMap<String, String>() {{
            put("tweet_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearch.REL_FAILURE).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("tweet_id", "28039652140");
    }

    /**
     * A Test class that extends the processor in order to inject/mock behavior
     */
    private static class ElasticsearchTestProcessor extends PutElasticsearch {
        boolean responseHasFailures = false;

        public ElasticsearchTestProcessor(boolean responseHasFailures) {
            this.responseHasFailures = responseHasFailures;
        }

        @Override
        @OnScheduled
        public void createClient(ProcessContext context) throws IOException {
            esClient = mock(Client.class);
            BulkRequestBuilder bulkRequestBuilder = spy(new BulkRequestBuilder(esClient, BulkAction.INSTANCE));
            doReturn(new MockBulkRequestBuilderExecutor(responseHasFailures)).when(bulkRequestBuilder).execute();
            when(esClient.prepareBulk()).thenReturn(bulkRequestBuilder);

            IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(esClient, IndexAction.INSTANCE);
            when(esClient.prepareIndex(anyString(), anyString(), anyString())).thenReturn(indexRequestBuilder);
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
     * @throws IOException
     */
    @Test
    @Ignore("Comment this out if you want to run against local or test ES")
    public void testPutElasticSearchBasic() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticsearch());
        runner.setValidateExpressionUsage(false);
        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(PutElasticsearch.INDEX, "tweet");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "1");


        runner.enqueue(twitterExample);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearch.REL_SUCCESS).get(0);

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
        runner.setProperty(PutElasticsearch.INDEX, "tweet");
        runner.setProperty(PutElasticsearch.BATCH_SIZE, "100");

        JsonParser parser = new JsonParser();
        JsonObject json;
        String message = convertStreamToString(twitterExample);
        for (int i = 0; i < 100; i++) {

            json = parser.parse(message).getAsJsonObject();
            String id = json.get("id").getAsString();
            long newId = Long.parseLong(id) + i;
            json.addProperty("id", newId);
            runner.enqueue(message.getBytes());

        }

        runner.run();

        runner.assertAllFlowFilesTransferred(PutElasticsearch.REL_SUCCESS, 100);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearch.REL_SUCCESS).get(0);

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
