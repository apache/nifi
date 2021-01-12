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

import org.apache.avro.Schema
import org.apache.nifi.avro.AvroTypeUtil
import org.apache.nifi.elasticsearch.IndexOperationRequest
import org.apache.nifi.elasticsearch.IndexOperationResponse
import org.apache.nifi.json.JsonRecordSetWriter
import org.apache.nifi.json.JsonTreeReader
import org.apache.nifi.processors.elasticsearch.mock.MockBulkLoadClientService
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.serialization.record.MockSchemaRegistry
import org.apache.nifi.util.StringUtils
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class PutElasticsearchRecordTest {
    MockBulkLoadClientService clientService
    MockSchemaRegistry registry
    RecordReaderFactory reader
    TestRunner runner

    static final String SCHEMA = prettyPrint(toJson([
        name: "TestSchema",
        type: "record",
        fields: [
            [ name: "msg", type: "string" ],
            [ name: "from", type: "string" ]
        ]
    ]))

    static final String flowFileContents = prettyPrint(toJson([
            [ msg: "Hello, world", from: "john.smith" ],
            [ msg: "Hi, back at ya!", from: "jane.doe" ]
    ]))

    @Before
    void setup() {
        clientService = new MockBulkLoadClientService()
        registry = new MockSchemaRegistry()
        reader   = new JsonTreeReader()
        runner   = TestRunners.newTestRunner(PutElasticsearchRecord.class)

        registry.addSchema("simple", AvroTypeUtil.createSchema(new Schema.Parser().parse(SCHEMA)))

        clientService.response = new IndexOperationResponse(1500)

        runner.addControllerService("registry", registry)
        runner.addControllerService("reader", reader)
        runner.addControllerService("clientService", clientService)
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_REGISTRY, "registry")
        runner.setProperty(PutElasticsearchRecord.RECORD_READER, "reader")
        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "index")
        runner.setProperty(PutElasticsearchRecord.INDEX, "test_index")
        runner.setProperty(PutElasticsearchRecord.TYPE, "test_type")
        runner.setProperty(PutElasticsearchRecord.CLIENT_SERVICE, "clientService")
        runner.enableControllerService(registry)
        runner.enableControllerService(reader)
        runner.enableControllerService(clientService)

        runner.assertValid()
    }

    void basicTest(int failure, int retry, int success) {
        runner.enqueue(flowFileContents, [ "schema.name": "simple" ])
        runner.run()

        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, failure)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, retry)
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, success)
    }

    @Test
    void simpleTest() {
        basicTest(0, 0, 1)
    }

    @Test
    void simpleTestWithMockReader() {
        reader = new MockRecordParser()
        runner.addControllerService("mockReader", reader)
        runner.setProperty(PutElasticsearchRecord.RECORD_READER, "mockReader")
        runner.enableControllerService(reader)
        basicTest(0, 0, 1)
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
    void testRecordPathFeatures() {
        def newSchema = prettyPrint(toJson([
            type: "record",
            name: "RecordPathTestType",
            fields: [
                [ name: "id", type: "string" ],
                [ name: "index", type: "string" ],
                [ name: "type", type: "string" ],
                [ name: "msg", type: ["null", "string"] ]
            ]
        ]))

        def flowFileContents = prettyPrint(toJson([
            [ id: "rec-1", op: "index", index: "bulk_a", type: "message", msg: "Hello" ],
            [ id: "rec-2", op: "index", index: "bulk_b", type: "message", msg: "Hello" ],
            [ id: "rec-3", op: "index", index: "bulk_a", type: "message", msg: "Hello" ],
            [ id: "rec-4", op: "index", index: "bulk_b", type: "message", msg: "Hello" ],
            [ id: "rec-5", op: "index", index: "bulk_a", type: "message", msg: "" ],
            [ id: "rec-6", op: "create", index: "bulk_b", type: "message", msg: null ]
        ]))

        def evalClosure = { List<IndexOperationRequest> items ->
            def a = items.findAll { it.index == "bulk_a" }.size()
            def b = items.findAll { it.index == "bulk_b" }.size()
            int index = items.findAll { it.operation == IndexOperationRequest.Operation.Index }.size()
            int create = items.findAll { it.operation == IndexOperationRequest.Operation.Create }.size()
            int msg = items.findAll { ("Hello" == it.fields.get("msg")) }.size()
            int empties = items.findAll { ("" == it.fields.get("msg")) }.size()
            int nulls = items.findAll { (null == it.fields.get("msg")) }.size()
            items.each {
                Assert.assertNotNull(it.id)
                Assert.assertTrue(it.id.startsWith("rec-"))
                Assert.assertEquals("message", it.type)
            }
            Assert.assertEquals(3, a)
            Assert.assertEquals(3, b)
            Assert.assertEquals(5, index)
            Assert.assertEquals(1, create)
            Assert.assertEquals(4, msg)
            Assert.assertEquals(1, empties)
            Assert.assertEquals(1, nulls)
        }

        clientService.evalClosure = evalClosure

        registry.addSchema("recordPathTest", AvroTypeUtil.createSchema(new Schema.Parser().parse(newSchema)))

        runner.removeProperty(PutElasticsearchRecord.INDEX_OP)
        runner.setProperty(PutElasticsearchRecord.INDEX_OP_RECORD_PATH, "/op")
        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "/id")
        runner.setProperty(PutElasticsearchRecord.INDEX_RECORD_PATH, "/index")
        runner.setProperty(PutElasticsearchRecord.TYPE_RECORD_PATH, "/type")
        runner.enqueue(flowFileContents, [
            "schema.name": "recordPathTest"
        ])

        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0)

        runner.clearTransferState()

        flowFileContents = prettyPrint(toJson([
            [ id: "rec-1", op: null, index: null, type: null, msg: "Hello" ],
            [ id: "rec-2", op: null, index: null, type: null, msg: "Hello" ],
            [ id: "rec-3", op: null, index: null, type: null, msg: "Hello" ],
            [ id: "rec-4", op: null, index: null, type: null, msg: "Hello" ],
            [ id: "rec-5", op: null, index: null, type: null, msg: "Hello" ],
            [ id: "rec-6", op: null, index: "bulk_b", type: "message", msg: "Hello" ]
        ]))

        evalClosure = { List<IndexOperationRequest> items ->
            def testTypeCount = items.findAll { it.type == "test_type" }.size()
            def messageTypeCount = items.findAll { it.type == "message" }.size()
            def testIndexCount = items.findAll { it.index == "test_index" }.size()
            def bulkIndexCount = items.findAll { it.index.startsWith("bulk_") }.size()
            def indexOperationCount = items.findAll { it.operation == IndexOperationRequest.Operation.Index }.size()
            Assert.assertEquals(5, testTypeCount)
            Assert.assertEquals(1, messageTypeCount)
            Assert.assertEquals(5, testIndexCount)
            Assert.assertEquals(1, bulkIndexCount)
            Assert.assertEquals(6, indexOperationCount)
        }

        clientService.evalClosure = evalClosure

        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "index")
        runner.enqueue(flowFileContents, [
            "schema.name": "recordPathTest"
        ])
        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0)

        runner.clearTransferState()

        flowFileContents = prettyPrint(toJson([
                [ msg: "Hello" ],
                [ id: null, type: null, msg: "Hello" ],
                [ id: "rec-3", msg: "Hello" ],
                [ id: "rec-4", msg: "Hello" ],
                [ id: "rec-5", msg: "Hello" ],
                [ id: "rec-6", type: "message", msg: "Hello" ]
        ]))

        evalClosure = { List<IndexOperationRequest> items ->
            def nullTypeCount = items.findAll { it.type == null }.size()
            def messageTypeCount = items.findAll { it.type == "message" }.size()
            def nullIdCount = items.findAll { it.id == null }.size()
            def recIdCount = items.findAll { StringUtils.startsWith(it.id, "rec-") }.size()
            Assert.assertEquals("null type", 5, nullTypeCount)
            Assert.assertEquals("message type", 1, messageTypeCount)
            Assert.assertEquals("null id", 2, nullIdCount)
            Assert.assertEquals("rec- id", 4, recIdCount)
        }

        clientService.evalClosure = evalClosure

        runner.removeProperty(PutElasticsearchRecord.TYPE)
        runner.enqueue(flowFileContents, [
                "schema.name": "recordPathTest"
        ])
        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0)

        runner.clearTransferState()

        flowFileContents = prettyPrint(toJson([
            [ id: "rec-1", op: "index", index: "bulk_a", type: "message", msg: "Hello" ],
            [ id: "rec-2", op: "create", index: "bulk_b", type: "message", msg: "Hello" ],
            [ id: "rec-3", op: "update", index: "bulk_a", type: "message", msg: "Hello" ],
            [ id: "rec-4", op: "upsert", index: "bulk_b", type: "message", msg: "Hello" ],
            [ id: "rec-5", op: "create", index: "bulk_a", type: "message", msg: "Hello" ],
            [ id: "rec-6", op: "delete", index: "bulk_b", type: "message", msg: "Hello" ]
        ]))

        clientService.evalClosure = { List<IndexOperationRequest> items ->
            int index = items.findAll { it.operation == IndexOperationRequest.Operation.Index }.size()
            int create = items.findAll { it.operation == IndexOperationRequest.Operation.Create }.size()
            int update = items.findAll { it.operation == IndexOperationRequest.Operation.Update }.size()
            int upsert = items.findAll { it.operation == IndexOperationRequest.Operation.Upsert }.size()
            int delete = items.findAll { it.operation == IndexOperationRequest.Operation.Delete }.size()
            Assert.assertEquals(1, index)
            Assert.assertEquals(2, create)
            Assert.assertEquals(1, update)
            Assert.assertEquals(1, upsert)
            Assert.assertEquals(1, delete)
        }

        runner.enqueue(flowFileContents, [
            "schema.name": "recordPathTest"
        ])
        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0)
    }

    @Test
    void testInputRequired() {
        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 0)
    }

    @Test
    void testErrorRelationship() {
        def writer = new JsonRecordSetWriter()
        runner.addControllerService("writer", writer)
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY)
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry")
        runner.enableControllerService(writer)
        runner.setProperty(PutElasticsearchRecord.ERROR_RECORD_WRITER, "writer")

        def newSchema = prettyPrint(toJson([
            type: "record",
            name: "RecordPathTestType",
            fields: [
                [ name: "id", type: "string" ],
                [ name: "field1", type: ["null", "string"]],
                [ name: "field2", type: "string"]
            ]
        ]))

        def values = [
            [ id: "1", field1: 'value1', field2: '20' ],
            [ id: "2", field1: 'value1', field2: '20' ],
            [ id: "2", field1: 'value1', field2: '20' ],
            [ id: "3", field1: 'value1', field2: '20abcd' ]
        ]

        clientService.response = IndexOperationResponse.fromJsonResponse(MockBulkLoadClientService.SAMPLE_ERROR_RESPONSE)

        registry.addSchema("errorTest", AvroTypeUtil.createSchema(new Schema.Parser().parse(newSchema)))
        runner.enqueue(prettyPrint(toJson(values)), [ 'schema.name': 'errorTest' ])
        runner.setProperty(PutElasticsearchRecord.LOG_ERROR_RESPONSES, "true")
        runner.assertValid()
        runner.run()

        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 1)

        def errorFF = runner.getFlowFilesForRelationship(PutElasticsearchRecord.REL_FAILED_RECORDS)[0]
        assert errorFF.getAttribute(PutElasticsearchRecord.ATTR_RECORD_COUNT) == "1"
    }
}