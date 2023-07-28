/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractByQueryElasticsearchTest {
    private static final String TEST_DIR = "src/test/resources/AbstractByQueryElasticsearchTest";
    private static final String INDEX = "test_idx";
    private static final String TYPE = "test_type";
    private static final String CLIENT_NAME = "clientService";
    private TestElasticsearchClientService client;
    private static String matchAllQuery;
    private TestRunner runner;

    public abstract String queryAttr();

    public abstract String tookAttr();

    public abstract String errorAttr();

    public abstract Class<? extends AbstractByQueryElasticsearch> getTestProcessor();

    public abstract void expectError(final TestElasticsearchClientService client);

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        matchAllQuery = Files.readString(Paths.get("src/test/resources/common/matchAllQuery.json"));
    }

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(getTestProcessor());
        client = new TestElasticsearchClientService(true);
        runner.addControllerService(CLIENT_NAME, client);
        runner.enableControllerService(client);
        runner.setProperty(AbstractByQueryElasticsearch.CLIENT_SERVICE, CLIENT_NAME);
    }

    private void postTest(TestRunner runner, String queryParam) {
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_SUCCESS, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractByQueryElasticsearch.REL_SUCCESS);
        final String attr = flowFiles.get(0).getAttribute(tookAttr());
        final String query = flowFiles.get(0).getAttribute(queryAttr());
        assertNotNull(attr);
        assertEquals("100", attr);
        assertNotNull(query);
        assertEquals(queryParam, query);
    }

    @Test
    public void testWithFlowfileInput() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.assertValid();
        runner.enqueue(query);
        runner.run();

        postTest(runner, query);

        assertTrue(client.getRequestParameters().isEmpty());
    }

    @Test
    public void testWithFlowfileInputAndRequestParameters() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.setProperty("refresh", "true");
        runner.setProperty("slices", "${slices}");
        runner.assertValid();
        runner.enqueue(query, Collections.singletonMap("slices", "auto"));
        runner.run();

        postTest(runner, query);

        assertEquals(2, client.getRequestParameters().size());
        assertEquals("true", client.getRequestParameters().get("refresh"));
        assertEquals("auto", client.getRequestParameters().get("slices"));
    }

    @Test
    public void testWithQuery() throws Exception {
        final String query = Files.readString(Paths.get(TEST_DIR,"matchUsingExpressionLanguageQuery.json"));
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.assertValid();
        runner.enqueue("", Collections.singletonMap("field.name", "test_field"));
        runner.run();

        postTest(runner, query.replace("${field.name}", "test_field"));

        runner.clearTransferState();

        final String query2 = Files.readString(Paths.get(TEST_DIR, "matchQuery.json"));
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query2);
        runner.setIncomingConnection(false);
        runner.assertValid();
        runner.run();
        postTest(runner, query2);
    }

    @Test
    public void testErrorAttribute() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        expectError(client);
        runner.assertValid();
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_FAILURE, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(AbstractByQueryElasticsearch.REL_FAILURE).get(0);
        final String attr = mockFlowFile.getAttribute(errorAttr());
        assertNotNull(attr);
    }

    @Test
    public void testInputHandling() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_FAILURE, 0);
    }

    @Test
    public void testNoInputHandling() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.setIncomingConnection(false);
        runner.assertValid();
        runner.run();

        postTest(runner, query);

        assertTrue(client.getRequestParameters().isEmpty());
    }

    @Test
    public void testNoInputHandlingWithRequestParameters() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.setProperty("refresh", "true");
        runner.setProperty("slices", "${slices}");
        runner.setVariable("slices", "auto");
        runner.setIncomingConnection(false);
        runner.assertValid();
        runner.run();

        postTest(runner, query);

        assertEquals(2, client.getRequestParameters().size());
        assertEquals("true", client.getRequestParameters().get("refresh"));
        assertEquals("auto", client.getRequestParameters().get("slices"));
    }
}
