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

import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.provenance.ProvenanceEventType
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.hamcrest.MatcherAssert
import org.junit.jupiter.api.Test

import static groovy.json.JsonOutput.toJson
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.is
import static org.hamcrest.MatcherAssert.assertThat
import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertThrows

class GetElasticsearchTest {
    static final String INDEX_NAME = "messages"

    @Test
    void testMandatoryProperties() {
        final TestRunner runner = createRunner()
        runner.removeProperty(GetElasticsearch.CLIENT_SERVICE)
        runner.removeProperty(GetElasticsearch.INDEX)
        runner.removeProperty(GetElasticsearch.TYPE)
        runner.removeProperty(GetElasticsearch.ID)
        runner.removeProperty(GetElasticsearch.DESTINATION)
        runner.removeProperty(GetElasticsearch.ATTRIBUTE_NAME)

        final AssertionError assertionError = assertThrows(AssertionError.class, runner.&run)
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 3 validation failures:\n" +
                "'%s' is invalid because %s is required\n" +
                "'%s' is invalid because %s is required\n" +
                "'%s' is invalid because %s is required\n",
                GetElasticsearch.ID.getDisplayName(), GetElasticsearch.ID.getDisplayName(),
                GetElasticsearch.INDEX.getDisplayName(), GetElasticsearch.INDEX.getDisplayName(),
                GetElasticsearch.CLIENT_SERVICE.getDisplayName(), GetElasticsearch.CLIENT_SERVICE.getDisplayName()
        )))
    }

    @Test
    void testInvalidProperties() {
        final TestRunner runner = createRunner()
        runner.setProperty(GetElasticsearch.CLIENT_SERVICE, "not-a-service")
        runner.setProperty(GetElasticsearch.INDEX, "")
        runner.setProperty(GetElasticsearch.TYPE, "")
        runner.setProperty(GetElasticsearch.ID, "")
        runner.setProperty(GetElasticsearch.DESTINATION, "not-valid")
        runner.setProperty(GetElasticsearch.ATTRIBUTE_NAME, "")

        final AssertionError assertionError = assertThrows(AssertionError.class, runner.&run)
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 6 validation failures:\n" +
                "'%s' validated against '' is invalid because %s cannot be empty\n" +
                "'%s' validated against '' is invalid because %s cannot be empty\n" +
                "'%s' validated against '' is invalid because %s cannot be empty\n" +
                "'%s' validated against 'not-valid' is invalid because Given value not found in allowed set '%s'\n" +
                "'%s' validated against 'not-a-service' is invalid because Property references a Controller Service that does not exist\n" +
                "'%s' validated against 'not-a-service' is invalid because Invalid Controller Service: not-a-service is not a valid Controller Service Identifier\n",
                GetElasticsearch.ID.getName(), GetElasticsearch.ID.getName(),
                GetElasticsearch.INDEX.getName(), GetElasticsearch.INDEX.getName(),
                GetElasticsearch.TYPE.getName(), GetElasticsearch.TYPE.getName(),
                GetElasticsearch.DESTINATION.getName(), [GetElasticsearch.FLOWFILE_CONTENT.getValue(), GetElasticsearch.FLOWFILE_ATTRIBUTE.getValue()].join(", "),
                GetElasticsearch.CLIENT_SERVICE.getDisplayName(),
                GetElasticsearch.CLIENT_SERVICE.getDisplayName()
        )))
    }

    @Test
    void testInvalidAttributeName() {
        final TestRunner runner = createRunner()
        runner.setProperty(GetElasticsearch.DESTINATION, GetElasticsearch.FLOWFILE_ATTRIBUTE)
        runner.setProperty(GetElasticsearch.ATTRIBUTE_NAME, "")

        final AssertionError assertionError = assertThrows(AssertionError.class, runner.&run)
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 1 validation failures:\n" +
                "'%s' validated against '' is invalid because %s cannot be empty\n",
                GetElasticsearch.ATTRIBUTE_NAME.getName(), GetElasticsearch.ATTRIBUTE_NAME.getName()
        )))
    }

    @Test
    void testFetch() throws Exception {
        final TestRunner runner = createRunner()

        runProcessor(runner)

        testCounts(runner, 1, 0, 0, 0)
        final FlowFile doc = runner.getFlowFilesForRelationship(GetElasticsearch.REL_DOC).get(0)
        assertOutputContent(doc.getContent())
        MatcherAssert.assertThat(
                runner.getProvenanceEvents().stream().filter({ pe ->
                    pe.getEventType() == ProvenanceEventType.RECEIVE &&
                            pe.getAttribute("uuid") == doc.getAttribute("uuid")
                }).count(),
                is(1L)
        )
    }

    @Test
    void testInputHandlingDestinationContent() {
        final TestRunner runner = createRunner()
        runner.setProperty(GetElasticsearch.DESTINATION, GetElasticsearch.FLOWFILE_CONTENT)

        runner.setIncomingConnection(true)
        runProcessor(runner)
        testCounts(runner, 1, 0, 0, 0)
        MockFlowFile doc = runner.getFlowFilesForRelationship(GetElasticsearch.REL_DOC).get(0)
        assertOutputContent(doc.getContent())
        assertCommonAttributes(doc)
        assertOutputAttribute(doc)
        reset(runner)

        runner.setIncomingConnection(false)
        runner.run()
        testCounts(runner, 1, 0, 0, 0)
        doc = runner.getFlowFilesForRelationship(GetElasticsearch.REL_DOC).get(0)
        assertOutputContent(doc.getContent())
        assertCommonAttributes(doc)
        assertOutputAttribute(doc)
    }

    @Test
    void testDestinationAttribute() {
        final TestRunner runner = createRunner()
        runner.setProperty(GetElasticsearch.DESTINATION, GetElasticsearch.FLOWFILE_ATTRIBUTE)

        runner.setIncomingConnection(true)
        runProcessor(runner)
        testCounts(runner, 1, 0, 0, 0)
        MockFlowFile doc = runner.getFlowFilesForRelationship(GetElasticsearch.REL_DOC).get(0)
        assertThat(doc.getContent(), is("test"))
        assertCommonAttributes(doc)
        assertOutputAttribute(doc, true)
        reset(runner)

        // non-default attribute name and fetch without type
        runner.setProperty(GetElasticsearch.ATTRIBUTE_NAME, "my_attr")
        runner.removeProperty(GetElasticsearch.TYPE)
        runner.setIncomingConnection(false)
        runner.run()
        testCounts(runner, 1, 0, 0, 0)
        doc = runner.getFlowFilesForRelationship(GetElasticsearch.REL_DOC).get(0)
        assertThat(doc.getContent(), is(""))
        assertCommonAttributes(doc, false)
        assertOutputAttribute(doc, true, "my_attr")
    }

    @Test
    void testErrorDuringFetch() throws Exception {
        final TestRunner runner = createRunner()
        getService(runner).setThrowErrorInGet(true)

        runner.setIncomingConnection(true)
        runProcessor(runner)
        testCounts(runner, 0, 1, 0, 0)
        reset(runner)

        runner.setIncomingConnection(false)
        runner.run()
        testCounts(runner, 0, 0, 0, 0)
    }

    @Test
    void testNotFound() throws Exception {
        final TestRunner runner = createRunner()
        getService(runner).setThrowNotFoundInGet(true)

        runProcessor(runner)
        testCounts(runner, 0, 0, 0, 1)
        reset(runner)
    }

    @Test
    void testRequestParameters() {
        final TestRunner runner = createRunner()
        runner.setProperty(GetElasticsearch.ID, "\${noAttribute}")

        runProcessor(runner)
        testCounts(runner, 0, 1, 0, 0)
        final FlowFile failed = runner.getFlowFilesForRelationship(GetElasticsearch.REL_FAILURE).get(0)
        failed.assertAttributeEquals("elasticsearch.get.error", GetElasticsearch.ID.getDisplayName() + " is blank (after evaluating attribute expressions), cannot GET document")
        reset(runner)
    }

    @Test
    void testEmptyId() {
        final TestRunner runner = createRunner()
        runner.setProperty("refresh", "true")
        runner.setProperty("_source", '${source}')
        runner.setVariable("source", "msg")

        runProcessor(runner)

        final TestElasticsearchClientService service = getService(runner)
        assertEquals(2, service.getRequestParameters().size())
        assertEquals("true", service.getRequestParameters().get("refresh"))
        assertEquals("msg", service.getRequestParameters().get("_source"))
    }

    private static void testCounts(final TestRunner runner, final int doc, final int failure, final int retry, final int notFound) {
        runner.assertTransferCount(GetElasticsearch.REL_DOC, doc)
        runner.assertTransferCount(GetElasticsearch.REL_FAILURE, failure)
        runner.assertTransferCount(GetElasticsearch.REL_RETRY, retry)
        runner.assertTransferCount(GetElasticsearch.REL_NOT_FOUND, notFound)
    }

    private static void assertOutputContent(final String content) {
        assertThat(content, is(toJson(["msg": "one"])))
    }

    private static void assertOutputAttribute(final MockFlowFile doc, final boolean attributeOutput = false, final String attr = "elasticsearch.doc") {
        if (attributeOutput) {
            doc.assertAttributeEquals(attr, toJson(["msg": "one"]))
        } else {
            doc.assertAttributeNotExists(attr)
        }
    }

    private static void assertCommonAttributes(final MockFlowFile doc, final boolean type = true, final boolean error = false) {
        doc.assertAttributeEquals("filename", "doc_1")
        doc.assertAttributeEquals("elasticsearch.index", INDEX_NAME)
        if (type) {
            doc.assertAttributeEquals("elasticsearch.type", "message")
        } else {
            doc.assertAttributeNotExists("elasticsearch.type")
        }

        if (error) {
            doc.assertAttributeEquals("elasticsearch.get.error", "message")
        } else {
            doc.assertAttributeNotExists("elasticsearch.get.error")
        }
    }

    private static TestRunner createRunner() {
        final GetElasticsearch processor = new GetElasticsearch()
        final TestRunner runner = TestRunners.newTestRunner(processor)
        final TestElasticsearchClientService service = new TestElasticsearchClientService(false)
        runner.addControllerService("esService", service)
        runner.enableControllerService(service)
        runner.setProperty(GetElasticsearch.CLIENT_SERVICE, "esService")
        runner.setProperty(GetElasticsearch.INDEX, INDEX_NAME)
        runner.setProperty(GetElasticsearch.TYPE, "message")
        runner.setProperty(GetElasticsearch.ID, "doc_1")
        runner.setProperty(GetElasticsearch.DESTINATION, GetElasticsearch.FLOWFILE_CONTENT)
        runner.setProperty(GetElasticsearch.ATTRIBUTE_NAME, "elasticsearch.doc")
        runner.setValidateExpressionUsage(true)

        return runner
    }

    private static MockFlowFile runProcessor(final TestRunner runner) {
        final MockFlowFile ff = runner.enqueue("test")
        runner.run()
        return ff
    }

    private static TestElasticsearchClientService getService(final TestRunner runner) {
        return runner.getControllerService("esService", TestElasticsearchClientService.class)
    }

    private static void reset(final TestRunner runner) {
        runner.clearProvenanceEvents()
        runner.clearTransferState()
    }
}
