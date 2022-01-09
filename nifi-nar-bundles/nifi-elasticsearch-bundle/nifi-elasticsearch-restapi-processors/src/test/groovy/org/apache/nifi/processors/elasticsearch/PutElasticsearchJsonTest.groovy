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

package org.apache.nifi.processors.elasticsearch

import org.apache.nifi.elasticsearch.IndexOperationRequest
import org.apache.nifi.elasticsearch.IndexOperationResponse
import org.apache.nifi.processors.elasticsearch.mock.MockBulkLoadClientService
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Before
import org.junit.Test


import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

import static org.hamcrest.CoreMatchers.containsString
import static org.hamcrest.MatcherAssert.assertThat

class PutElasticsearchJsonTest {
    MockBulkLoadClientService clientService
    TestRunner runner

    static final String flowFileContents = prettyPrint(toJson(
            [ msg: "Hello, world", from: "john.smith" ]
    ))

    @Before
    void setup() {
        clientService = new MockBulkLoadClientService()
        runner   = TestRunners.newTestRunner(PutElasticsearchJson.class)

        clientService.response = new IndexOperationResponse(1500)

        runner.addControllerService("clientService", clientService)
        runner.setProperty(PutElasticsearchJson.ID_ATTRIBUTE, "doc_id")
        runner.setProperty(PutElasticsearchJson.INDEX_OP, IndexOperationRequest.Operation.Index.getValue())
        runner.setProperty(PutElasticsearchJson.INDEX, "test_index")
        runner.setProperty(PutElasticsearchJson.TYPE, "test_type")
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "1")
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_DOCUMENTS, "false")
        runner.setProperty(PutElasticsearchJson.LOG_ERROR_RESPONSES, "false")
        runner.setProperty(PutElasticsearchJson.CLIENT_SERVICE, "clientService")
        runner.enableControllerService(clientService)

        runner.assertValid()
    }

    void basicTest(int failure, int retry, int success) {
        def evalClosure = { List<IndexOperationRequest> items ->
            int nullIdCount = items.findAll { it.id == null }.size()
            int indexCount = items.findAll { it.index == "test_index" }.size()
            int typeCount = items.findAll { it.type == "test_type" }.size()
            int opCount = items.findAll { it.operation == IndexOperationRequest.Operation.Index }.size()
            Assert.assertEquals(1, nullIdCount)
            Assert.assertEquals(1, indexCount)
            Assert.assertEquals(1, typeCount)
            Assert.assertEquals(1, opCount)
        }

        basicTest(failure, retry, success, evalClosure)
    }

    void basicTest(int failure, int retry, int success, Closure evalClosure) {
        clientService.evalClosure = evalClosure

        basicTest(failure, retry, success, null)
    }

    void basicTest(int failure, int retry, int success, Map<String, String> attr) {
        if (attr != null) {
            runner.enqueue(flowFileContents, attr)
        } else {
            runner.enqueue(flowFileContents)
        }
        runner.run()

        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, failure)
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, retry)
        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, success)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 0)
    }

    @Test
    void simpleTest() {
        def evalParametersClosure = { Map<String, String> params ->
            Assert.assertTrue(params.isEmpty())
        }
        clientService.evalParametersClosure = evalParametersClosure

        basicTest(0, 0, 1)
    }

    @Test
    void simpleTestWithDocIdAndRequestParameters() {
        runner.setProperty("refresh", "true")
        runner.setProperty("slices", '${slices}')
        runner.setVariable("slices", "auto")
        runner.assertValid()

        def evalParametersClosure = { Map<String, String> params ->
            Assert.assertEquals(2, params.size())
            Assert.assertEquals("true", params.get("refresh"))
            Assert.assertEquals("auto", params.get("slices"))
        }

        clientService.evalParametersClosure = evalParametersClosure

        def evalClosure = { List<IndexOperationRequest> items ->
            int idCount = items.findAll { it.id == "123" }.size()
            int indexCount = items.findAll { it.index == "test_index" }.size()
            int typeCount = items.findAll { it.type == "test_type" }.size()
            int opCount = items.findAll { it.operation == IndexOperationRequest.Operation.Index }.size()
            Assert.assertEquals(1, idCount)
            Assert.assertEquals(1, indexCount)
            Assert.assertEquals(1, typeCount)
            Assert.assertEquals(1, opCount)
        }

        clientService.evalClosure = evalClosure

        basicTest(0, 0, 1, [doc_id: "123"])
    }

    @Test
    void simpleTestWithRequestParametersFlowFileEL() {
        runner.setProperty("refresh", "true")
        runner.setProperty("slices", '${slices}')
        runner.assertValid()

        def evalParametersClosure = { Map<String, String> params ->
            Assert.assertEquals(2, params.size())
            Assert.assertEquals("true", params.get("refresh"))
            Assert.assertEquals("auto", params.get("slices"))
        }

        clientService.evalParametersClosure = evalParametersClosure

        basicTest(0, 0, 1, [slices: "auto"])
    }

    @Test
    void testFatalError() {
        clientService.throwFatalError = true
        basicTest(1, 0, 0)
    }

    @Test
    void testRetriable() {
        clientService.throwRetriableError = true
        basicTest(0, 1, 0)
    }

    @Test
    void testInvalidIndexOperation() {
        runner.setProperty(PutElasticsearchJson.INDEX_OP, "not-valid")
        runner.assertNotValid()
        final AssertionError ae = Assert.assertThrows(AssertionError.class, runner.&run)
        Assert.assertEquals(String.format("Processor has 1 validation failures:\n'%s' validated against 'not-valid' is invalid because %s must be Expression Language or one of %s\n",
                PutElasticsearchJson.INDEX_OP.getName(), PutElasticsearchJson.INDEX_OP.getDisplayName(), PutElasticsearchJson.ALLOWED_INDEX_OPERATIONS),
                ae.getMessage()
        )

        runner.setProperty(PutElasticsearchJson.INDEX_OP, "\${operation}")
        runner.assertValid()
        runner.enqueue(flowFileContents, [
                "operation": "not-valid2"
        ])
        runner.run()
        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 1)
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 0)
    }

    @Test
    void testInputRequired() {
        runner.run()
        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 0)
    }

    @Test
    void testBatchingAndErrorRelationship() {
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_DOCUMENTS, "true")
        runner.setProperty(PutElasticsearchJson.LOG_ERROR_RESPONSES, "true")
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "100")

        clientService.response = IndexOperationResponse.fromJsonResponse(MockBulkLoadClientService.SAMPLE_ERROR_RESPONSE)

        def values = [
                [ id: "1", field1: 'value1', field2: '20' ],
                [ id: "2", field1: 'value1', field2: '20' ],
                [ id: "2", field1: 'value1', field2: '20' ],
                [ id: "3", field1: 'value1', field2: '20abcd' ]
        ]

        for (final def val : values) {
            runner.enqueue(prettyPrint(toJson(val)))
        }
        runner.assertValid()
        runner.run()

        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 3)
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 1)
        assertThat(runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILED_DOCUMENTS)[0].getContent(), containsString("20abcd"))
    }

    @Test
    void testBatchingAndNoErrorOutput() {
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_DOCUMENTS, "false")
        runner.setProperty(PutElasticsearchJson.LOG_ERROR_RESPONSES, "false")
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "100")

        clientService.response = IndexOperationResponse.fromJsonResponse(MockBulkLoadClientService.SAMPLE_ERROR_RESPONSE)

        def values = [
                [ id: "1", field1: 'value1', field2: '20' ],
                [ id: "2", field1: 'value1', field2: '20' ],
                [ id: "2", field1: 'value1', field2: '20' ],
                [ id: "3", field1: 'value1', field2: '20abcd' ]
        ]

        for (final def val : values) {
            runner.enqueue(prettyPrint(toJson(val)))
        }
        runner.assertValid()
        runner.run()

        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 4)
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 0)
    }

    @Test
    void testInvalidInput() {
        runner.enqueue("not-json")
        runner.run()

        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 1)
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0)
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 0)

        runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILURE)[0].assertAttributeEquals(
                "elasticsearch.put.error",
                "Unrecognized token 'not': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')\n" +
                " at [Source: (String)\"not-json\"; line: 1, column: 4]"
        )
    }
}
