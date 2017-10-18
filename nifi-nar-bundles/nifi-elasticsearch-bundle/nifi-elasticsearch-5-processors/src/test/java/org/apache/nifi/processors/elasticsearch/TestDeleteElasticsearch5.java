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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.rest.RestStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestDeleteElasticsearch5 {

    private String documentId;
    private static final String TYPE1 = "type1";
    private static final String INDEX1 = "index1";
    private TestRunner runner;
    protected DeleteResponse deleteResponse;
    protected RestStatus restStatus;
    private DeleteElasticsearch5 mockDeleteProcessor;
    long currentTimeMillis;

    @Before
    public void setUp() throws IOException {
        currentTimeMillis = System.currentTimeMillis();
        documentId = String.valueOf(currentTimeMillis);
        mockDeleteProcessor = new DeleteElasticsearch5() {

            @Override
            protected DeleteRequestBuilder prepareDeleteRequest(String index, String docId, String docType) {
                return null;
            }

            @Override
            protected DeleteResponse doDelete(DeleteRequestBuilder requestBuilder)
                    throws InterruptedException, ExecutionException {
                return deleteResponse;
            }

            @Override
            public void setup(ProcessContext context) {
            }

        };

        runner = TestRunners.newTestRunner(mockDeleteProcessor);

        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(DeleteElasticsearch5.INDEX, INDEX1);
        runner.assertNotValid();
        runner.setProperty(DeleteElasticsearch5.TYPE, TYPE1);
        runner.assertNotValid();
        runner.setProperty(DeleteElasticsearch5.DOCUMENT_ID, "${documentId}");
        runner.assertValid();
    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testDeleteWithNoDocumentId() throws IOException {

        runner.enqueue(new byte [] {});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DeleteElasticsearch5.REL_FAILURE).get(0);
        assertNotNull(out);
        assertEquals("Document id is required but was empty",out.getAttribute(DeleteElasticsearch5.ES_ERROR_MESSAGE));
    }

    @Test
    public void testDeleteWithNoIndex() throws IOException {
        runner.setProperty(DeleteElasticsearch5.INDEX, "${index}");

        runner.enqueue(new byte [] {});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DeleteElasticsearch5.REL_FAILURE).get(0);
        assertNotNull(out);
        assertEquals("Index is required but was empty",out.getAttribute(DeleteElasticsearch5.ES_ERROR_MESSAGE));
    }

    @Test
    public void testDeleteWithNoType() throws IOException {
        runner.setProperty(DeleteElasticsearch5.TYPE, "${type}");

        runner.enqueue(new byte [] {});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DeleteElasticsearch5.REL_FAILURE).get(0);
        assertNotNull(out);
        assertEquals("Document type is required but was empty",out.getAttribute(DeleteElasticsearch5.ES_ERROR_MESSAGE));
    }

    @Test
    public void testDeleteSuccessful() throws IOException {
        restStatus = RestStatus.OK;
        deleteResponse = new DeleteResponse(null, TYPE1, documentId, 1, true) {

            @Override
            public RestStatus status() {
                return restStatus;
            }

        };
        runner.enqueue(new byte [] {}, new HashMap<String, String>() {{
            put("documentId", documentId);
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DeleteElasticsearch5.REL_SUCCESS).get(0);
        assertNotNull(out);
        assertEquals(null,out.getAttribute(DeleteElasticsearch5.ES_ERROR_MESSAGE));
        out.assertAttributeEquals(DeleteElasticsearch5.ES_FILENAME, documentId);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_INDEX, INDEX1);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_TYPE, TYPE1);
    }

    @Test
    public void testDeleteNotFound() throws IOException {
        restStatus = RestStatus.NOT_FOUND;
        deleteResponse = new DeleteResponse(null, TYPE1, documentId, 1, true) {

            @Override
            public RestStatus status() {
                return restStatus;
            }

        };
        runner.enqueue(new byte [] {}, new HashMap<String, String>() {{
            put("documentId", documentId);
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_NOT_FOUND, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DeleteElasticsearch5.REL_NOT_FOUND).get(0);
        assertNotNull(out);
        assertEquals(DeleteElasticsearch5.UNABLE_TO_DELETE_DOCUMENT_MESSAGE,out.getAttribute(DeleteElasticsearch5.ES_ERROR_MESSAGE));
        out.assertAttributeEquals(DeleteElasticsearch5.ES_REST_STATUS, restStatus.toString());
        out.assertAttributeEquals(DeleteElasticsearch5.ES_FILENAME, documentId);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_INDEX, INDEX1);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_TYPE, TYPE1);
    }

    @Test
    public void testDeleteServerFailure() throws IOException {
        restStatus = RestStatus.SERVICE_UNAVAILABLE;
        deleteResponse = new DeleteResponse(null, TYPE1, documentId, 1, true) {

            @Override
            public RestStatus status() {
                return restStatus;
            }

        };
        runner.enqueue(new byte [] {}, new HashMap<String, String>() {{
            put("documentId", documentId);
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DeleteElasticsearch5.REL_FAILURE).get(0);
        assertNotNull(out);
        assertEquals(DeleteElasticsearch5.UNABLE_TO_DELETE_DOCUMENT_MESSAGE,out.getAttribute(DeleteElasticsearch5.ES_ERROR_MESSAGE));
        out.assertAttributeEquals(DeleteElasticsearch5.ES_REST_STATUS, restStatus.toString());
        out.assertAttributeEquals(DeleteElasticsearch5.ES_FILENAME, documentId);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_INDEX, INDEX1);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_TYPE, TYPE1);
    }

    @Test
    public void testDeleteRetryableException() throws IOException {
        mockDeleteProcessor = new DeleteElasticsearch5() {

            @Override
            protected DeleteRequestBuilder prepareDeleteRequest(String index, String docId, String docType) {
                return null;
            }

            @Override
            protected DeleteResponse doDelete(DeleteRequestBuilder requestBuilder)
                    throws InterruptedException, ExecutionException {
                throw new ElasticsearchTimeoutException("timeout");
            }

            @Override
            public void setup(ProcessContext context) {
            }

        };
        runner = TestRunners.newTestRunner(mockDeleteProcessor);

        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(DeleteElasticsearch5.INDEX, INDEX1);
        runner.setProperty(DeleteElasticsearch5.TYPE, TYPE1);
        runner.setProperty(DeleteElasticsearch5.DOCUMENT_ID, "${documentId}");
        runner.assertValid();

        runner.enqueue(new byte [] {}, new HashMap<String, String>() {{
            put("documentId", documentId);
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_RETRY, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DeleteElasticsearch5.REL_RETRY).get(0);
        assertNotNull(out);
        assertEquals("timeout",out.getAttribute(DeleteElasticsearch5.ES_ERROR_MESSAGE));
        out.assertAttributeEquals(DeleteElasticsearch5.ES_REST_STATUS, null);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_FILENAME, documentId);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_INDEX, INDEX1);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_TYPE, TYPE1);
    }

    @Test
    public void testDeleteNonRetryableException() throws IOException {
        mockDeleteProcessor = new DeleteElasticsearch5() {

            @Override
            protected DeleteRequestBuilder prepareDeleteRequest(String index, String docId, String docType) {
                return null;
            }

            @Override
            protected DeleteResponse doDelete(DeleteRequestBuilder requestBuilder)
                    throws InterruptedException, ExecutionException {
                throw new InterruptedException("exception");
            }

            @Override
            public void setup(ProcessContext context) {
            }

        };
        runner = TestRunners.newTestRunner(mockDeleteProcessor);

        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.CLUSTER_NAME, "elasticsearch");
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearch5TransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(DeleteElasticsearch5.INDEX, INDEX1);
        runner.setProperty(DeleteElasticsearch5.TYPE, TYPE1);
        runner.setProperty(DeleteElasticsearch5.DOCUMENT_ID, "${documentId}");
        runner.assertValid();

        runner.enqueue(new byte [] {}, new HashMap<String, String>() {{
            put("documentId", documentId);
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(DeleteElasticsearch5.REL_FAILURE).get(0);
        assertNotNull(out);
        assertEquals("exception",out.getAttribute(DeleteElasticsearch5.ES_ERROR_MESSAGE));
        out.assertAttributeEquals(DeleteElasticsearch5.ES_REST_STATUS, null);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_FILENAME, documentId);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_INDEX, INDEX1);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_TYPE, TYPE1);
    }

}
