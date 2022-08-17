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

package org.apache.nifi.processors.elasticsearch

import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.TestRunner
import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

abstract class AbstractByQueryElasticsearchTest {
    private static final String INDEX = "test_idx"
    private static final String TYPE  = "test_type"
    private static final String CLIENT_NAME = "clientService"

    private TestElasticsearchClientService client

    abstract String queryAttr()

    abstract String tookAttr()

    abstract String errorAttr()

    abstract TestRunner setupRunner()

    abstract void expectError(final TestElasticsearchClientService client)

    private void initClient(TestRunner runner) throws Exception {
        client = new TestElasticsearchClientService(true)
        runner.addControllerService(CLIENT_NAME, client)
        runner.enableControllerService(client)
        runner.setProperty(AbstractByQueryElasticsearch.CLIENT_SERVICE, CLIENT_NAME)
    }

    private void postTest(TestRunner runner, String queryParam) {
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_FAILURE, 0)
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_SUCCESS, 1)

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractByQueryElasticsearch.REL_SUCCESS)
        final String attr = flowFiles.get(0).getAttribute(tookAttr())
        final String query = flowFiles.get(0).getAttribute(queryAttr())
        assertNotNull(attr)
        assertEquals(attr, "100")
        assertNotNull(query)
        assertEquals(queryParam, query)
    }

    @Test
    void testWithFlowfileInput() throws Exception {
        final String query = "{ \"query\": { \"match_all\": {} }}"
        final TestRunner runner = setupRunner()
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX)
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE)
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr())
        initClient(runner)
        runner.assertValid()
        runner.enqueue(query)
        runner.run()

        postTest(runner, query)

        assertTrue(client.getRequestParameters().isEmpty())
    }

    @Test
    void testWithFlowfileInputAndRequestParameters() throws Exception {
        final String query = "{ \"query\": { \"match_all\": {} }}"
        final TestRunner runner = setupRunner()
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX)
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE)
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr())
        runner.setProperty("refresh", "true")
        runner.setProperty("slices", '${slices}')
        initClient(runner)
        runner.assertValid()
        runner.enqueue(query, [slices: "auto"])
        runner.run()

        postTest(runner, query)

        assertEquals(2, client.getRequestParameters().size())
        assertEquals("true", client.getRequestParameters().get("refresh"))
        assertEquals("auto", client.getRequestParameters().get("slices"))
    }

    @Test
    void testWithQuery() throws Exception {
        final String query = "{\n" +
            "\t\"query\": {\n" +
            "\t\t\"match\": {\n" +
            "\t\t\t\"\${field.name}.keyword\": \"test\"\n" +
            "\t\t}\n" +
            "\t}\n" +
            "}"
        final Map<String, String> attrs = new HashMap<String, String>(){{
            put("field.name", "test_field")
        }}
        final TestRunner runner = setupRunner()
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query)
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX)
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE)
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr())
        initClient(runner)
        runner.assertValid()
        runner.enqueue("", attrs)
        runner.run()

        postTest(runner, query.replace('${field.name}', "test_field"))

        runner.clearTransferState()

        final String query2 = "{\n" +
            "\t\"query\": {\n" +
            "\t\t\"match\": {\n" +
            "\t\t\t\"test_field.keyword\": \"test\"\n" +
            "\t\t}\n" +
            "\t}\n" +
            "}"
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query2)
        runner.setIncomingConnection(false)
        runner.assertValid()
        runner.run()
        postTest(runner, query2)
    }

    @Test
    void testErrorAttribute() throws Exception {
        final String query = "{ \"query\": { \"match_all\": {} }}"
        final TestRunner runner = setupRunner()
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query)
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX)
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE)
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr())
        initClient(runner)
        expectError(client)
        runner.assertValid()
        runner.enqueue("")
        runner.run()

        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_SUCCESS, 0)
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_FAILURE, 1)

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(AbstractByQueryElasticsearch.REL_FAILURE).get(0)
        final String attr = mockFlowFile.getAttribute(errorAttr())
        assertNotNull(attr)
    }

    @Test
    void testInputHandling() {
        final String query = "{ \"query\": { \"match_all\": {} }}"
        final TestRunner runner = setupRunner()
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query)
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX)
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE)
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr())
        initClient(runner)
        runner.assertValid()
        runner.run()

        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_SUCCESS, 0)
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_FAILURE, 0)
    }

    @Test
    void testNoInputHandling() {
        final String query = "{ \"query\": { \"match_all\": {} }}"
        final TestRunner runner = setupRunner()
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query)
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX)
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE)
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr())
        initClient(runner)
        runner.setIncomingConnection(false)
        runner.assertValid()
        runner.run()

        postTest(runner, query)

        assertTrue(client.getRequestParameters().isEmpty())
    }

    @Test
    void testNoInputHandlingWithRequestParameters() {
        final String query = "{ \"query\": { \"match_all\": {} }}"
        final TestRunner runner = setupRunner()
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query)
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX)
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE)
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr())
        runner.setProperty("refresh", "true")
        runner.setProperty("slices", '${slices}')
        runner.setVariable("slices", "auto")
        initClient(runner)
        runner.setIncomingConnection(false)
        runner.assertValid()
        runner.run()

        postTest(runner, query)

        assertEquals(2, client.getRequestParameters().size())
        assertEquals("true", client.getRequestParameters().get("refresh"))
        assertEquals("auto", client.getRequestParameters().get("slices"))
    }
}
