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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.rest.RestStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

/**
 * Integration test for delete processor. Please set the hosts, cluster name, index and type etc before running the integrations.
 */
@Ignore("Comment this out for es delete integration testing and set the appropriate cluster name, hosts, etc")
public class ITDeleteElasticsearch5Test {

    private static final String TYPE1 = "type1";
    private static final String INDEX1 = "index1";
    protected DeleteResponse deleteResponse;
    protected RestStatus restStatus;
    private InputStream inputDocument;
    protected String clusterName = "elasticsearch";
    private String documentId;

    @Before
    public void setUp() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        inputDocument = classloader.getResourceAsStream("DocumentExample.json");
        long currentTimeMillis = System.currentTimeMillis();
        documentId = String.valueOf(currentTimeMillis);
    }

    @After
    public void teardown() {
    }

    @Test
    public void testPutAndDeleteIntegrationTestSuccess() {
        final TestRunner runnerPut = TestRunners.newTestRunner(new PutElasticsearch5());
        runnerPut.setValidateExpressionUsage(false);
        runnerPut.setProperty(AbstractElasticsearch5TransportClientProcessor.CLUSTER_NAME, clusterName);
        runnerPut.setProperty(AbstractElasticsearch5TransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runnerPut.setProperty(AbstractElasticsearch5TransportClientProcessor.PING_TIMEOUT, "5s");
        runnerPut.setProperty(AbstractElasticsearch5TransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runnerPut.setProperty(PutElasticsearch5.INDEX, INDEX1);
        runnerPut.setProperty(PutElasticsearch5.BATCH_SIZE, "1");

        runnerPut.setProperty(PutElasticsearch5.TYPE, TYPE1);
        runnerPut.setProperty(PutElasticsearch5.ID_ATTRIBUTE, "id");
        runnerPut.assertValid();

        runnerPut.enqueue(inputDocument, new HashMap<String, String>() {{
            put("id", documentId);
        }});

        runnerPut.enqueue(inputDocument);
        runnerPut.run(1, true, true);

        runnerPut.assertAllFlowFilesTransferred(PutElasticsearch5.REL_SUCCESS, 1);

        final TestRunner runnerDelete = TestRunners.newTestRunner(new DeleteElasticsearch5());
        runnerDelete.setValidateExpressionUsage(false);

        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.CLUSTER_NAME, clusterName);
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.PING_TIMEOUT, "5s");
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runnerDelete.setProperty(DeleteElasticsearch5.INDEX, INDEX1);

        runnerDelete.setProperty(DeleteElasticsearch5.TYPE, TYPE1);
        runnerDelete.setProperty(DeleteElasticsearch5.DOCUMENT_ID, "${documentId}");
        runnerDelete.assertValid();

        runnerDelete.enqueue(new byte[] {}, new HashMap<String, String>() {{
            put("documentId", documentId);
        }});

        runnerDelete.enqueue(new byte [] {});
        runnerDelete.run(1, true, true);

        runnerDelete.assertAllFlowFilesTransferred(PutElasticsearch5.REL_SUCCESS, 1);
    }

    @Test
    public void testDeleteIntegrationTestDocumentNotFound() {
        final TestRunner runnerDelete = TestRunners.newTestRunner(new DeleteElasticsearch5());
        runnerDelete.setValidateExpressionUsage(false);

        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.CLUSTER_NAME, clusterName);
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.PING_TIMEOUT, "5s");
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runnerDelete.setProperty(DeleteElasticsearch5.INDEX, INDEX1);

        runnerDelete.setProperty(DeleteElasticsearch5.TYPE, TYPE1);
        runnerDelete.setProperty(DeleteElasticsearch5.DOCUMENT_ID, "${documentId}");
        runnerDelete.assertValid();

        runnerDelete.enqueue(new byte[] {}, new HashMap<String, String>() {{
            put("documentId", documentId);
        }});

        runnerDelete.enqueue(new byte [] {});
        runnerDelete.run(1, true, true);

        runnerDelete.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_NOT_FOUND, 1);
        final MockFlowFile out = runnerDelete.getFlowFilesForRelationship(DeleteElasticsearch5.REL_NOT_FOUND).get(0);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_FILENAME, documentId);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_INDEX, INDEX1);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_TYPE, TYPE1);
    }

    @Test
    public void testDeleteIntegrationTestBadIndex() {
        final TestRunner runnerDelete = TestRunners.newTestRunner(new DeleteElasticsearch5());
        runnerDelete.setValidateExpressionUsage(false);

        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.CLUSTER_NAME, clusterName);
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.PING_TIMEOUT, "5s");
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.SAMPLER_INTERVAL, "5s");

        String index = String.valueOf(System.currentTimeMillis());
        runnerDelete.setProperty(DeleteElasticsearch5.INDEX, index);

        runnerDelete.setProperty(DeleteElasticsearch5.TYPE, TYPE1);
        runnerDelete.setProperty(DeleteElasticsearch5.DOCUMENT_ID, "${documentId}");
        runnerDelete.assertValid();

        runnerDelete.enqueue(new byte[] {}, new HashMap<String, String>() {{
            put("documentId", documentId);
        }});

        runnerDelete.enqueue(new byte [] {});
        runnerDelete.run(1, true, true);
        runnerDelete.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_NOT_FOUND, 1);
        final MockFlowFile out = runnerDelete.getFlowFilesForRelationship(DeleteElasticsearch5.REL_NOT_FOUND).get(0);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_FILENAME, documentId);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_INDEX, index);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_TYPE, TYPE1);
    }

    @Test
    public void testDeleteIntegrationTestBadType() {
        final TestRunner runnerDelete = TestRunners.newTestRunner(new DeleteElasticsearch5());
        runnerDelete.setValidateExpressionUsage(false);

        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.CLUSTER_NAME, clusterName);
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.HOSTS, "127.0.0.1:9300");
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.PING_TIMEOUT, "5s");
        runnerDelete.setProperty(AbstractElasticsearch5TransportClientProcessor.SAMPLER_INTERVAL, "5s");

        runnerDelete.setProperty(DeleteElasticsearch5.INDEX, INDEX1);
        String type = String.valueOf(System.currentTimeMillis());
        runnerDelete.setProperty(DeleteElasticsearch5.TYPE, type);
        runnerDelete.setProperty(DeleteElasticsearch5.DOCUMENT_ID, "${documentId}");
        runnerDelete.assertValid();

        runnerDelete.enqueue(new byte[] {}, new HashMap<String, String>() {{
        put("documentId", documentId);
        }});

        runnerDelete.enqueue(new byte [] {});
        runnerDelete.run(1, true, true);

        runnerDelete.assertAllFlowFilesTransferred(DeleteElasticsearch5.REL_NOT_FOUND, 1);
        final MockFlowFile out = runnerDelete.getFlowFilesForRelationship(DeleteElasticsearch5.REL_NOT_FOUND).get(0);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_FILENAME, documentId);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_INDEX, INDEX1);
        out.assertAttributeEquals(DeleteElasticsearch5.ES_TYPE, type);
    }
}
