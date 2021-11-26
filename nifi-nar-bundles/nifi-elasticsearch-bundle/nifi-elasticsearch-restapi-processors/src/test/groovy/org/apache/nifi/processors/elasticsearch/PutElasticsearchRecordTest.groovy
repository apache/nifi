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
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.util.StringUtils
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class PutElasticsearchRecordTest {
    private static final int DATE_YEAR = 2020
    private static final int DATE_MONTH = 11
    private static final int DATE_DAY = 27
    private static final int TIME_HOUR = 12
    private static final int TIME_MINUTE = 55
    private static final int TIME_SECOND = 23

    private static final LocalDateTime LOCAL_DATE_TIME = LocalDateTime.of(DATE_YEAR, DATE_MONTH, DATE_DAY, TIME_HOUR, TIME_MINUTE, TIME_SECOND)
    private static final LocalDate LOCAL_DATE = LocalDate.of(DATE_YEAR, DATE_MONTH, DATE_DAY)
    private static final LocalTime LOCAL_TIME = LocalTime.of(TIME_HOUR, TIME_MINUTE, TIME_SECOND)

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
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY)
        runner.setProperty(PutElasticsearchRecord.RECORD_READER, "reader")
        runner.setProperty(PutElasticsearchRecord.INDEX_OP, IndexOperationRequest.Operation.Index.getValue())
        runner.setProperty(PutElasticsearchRecord.INDEX, "test_index")
        runner.setProperty(PutElasticsearchRecord.TYPE, "test_type")
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP, "test_timestamp")
        runner.setProperty(PutElasticsearchRecord.CLIENT_SERVICE, "clientService")
        runner.enableControllerService(registry)
        runner.enableControllerService(reader)
        runner.enableControllerService(clientService)

        runner.assertValid()
    }

    void basicTest(int failure, int retry, int success) {
        def evalClosure = { List<IndexOperationRequest> items ->
            int timestampDefaultCount = items.findAll { it.fields.get("@timestamp") == "test_timestamp" }.size()
            int indexCount = items.findAll { it.index == "test_index" }.size()
            int typeCount = items.findAll { it.type == "test_type" }.size()
            int opCount = items.findAll { it.operation == IndexOperationRequest.Operation.Index }.size()
            Assert.assertEquals(2, timestampDefaultCount)
            Assert.assertEquals(2, indexCount)
            Assert.assertEquals(2, typeCount)
            Assert.assertEquals(2, opCount)
        }

        basicTest(failure, retry, success, evalClosure)
    }

    void basicTest(int failure, int retry, int success, Closure evalClosure) {
        clientService.evalClosure = evalClosure

        basicTest(failure, retry, success, [ "schema.name": "simple" ])
    }

    void basicTest(int failure, int retry, int success, Map<String, String> attr) {
        runner.enqueue(flowFileContents, attr)
        runner.run()

        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, failure)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, retry)
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, success)
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
    void simpleTestCoercedDefaultTimestamp() {
        def evalClosure = { List<IndexOperationRequest> items ->
            int timestampDefault = items.findAll { it.fields.get("@timestamp") == 100L }.size()
            Assert.assertEquals(2, timestampDefault)
        }

        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP, "100")
        basicTest(0, 0, 1, evalClosure)
    }

    @Test
    void simpleTestWithRequestParameters() {
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

        basicTest(0, 0, 1)
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

        basicTest(0, 0, 1, ["schema.name": "simple", slices: "auto"])
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
                [ name: "op", type: "string" ],
                [ name: "index", type: "string" ],
                [ name: "type", type: "string" ],
                [ name: "msg", type: ["null", "string"] ],
                [ name: "ts", type: [ type: "long", logicalType: "timestamp-millis" ] ],
                [ name: "date", type: [ type: "int", logicalType: "date" ] ],
                [ name: "time", type: [ type: "int", logicalType: "time-millis" ] ],
                [ name: "code", type: "long" ]
            ]
        ]))

        def flowFileContents = prettyPrint(toJson([
            [ id: "rec-1", op: "index", index: "bulk_a", type: "message", msg: "Hello", ts: Timestamp.valueOf(LOCAL_DATE_TIME).toInstant().toEpochMilli() ],
            [ id: "rec-2", op: "index", index: "bulk_b", type: "message", msg: "Hello" ],
            [ id: "rec-3", op: "index", index: "bulk_a", type: "message", msg: "Hello" ],
            [ id: "rec-4", op: "index", index: "bulk_b", type: "message", msg: "Hello" ],
            [ id: "rec-5", op: "index", index: "bulk_a", type: "message", msg: "" ],
            [ id: "rec-6", op: "create", index: "bulk_b", type: "message", msg: null ]
        ]))

        def evalClosure = { List<IndexOperationRequest> items ->
            int a = items.findAll { it.index == "bulk_a" }.size()
            int b = items.findAll { it.index == "bulk_b" }.size()
            int index = items.findAll { it.operation == IndexOperationRequest.Operation.Index }.size()
            int create = items.findAll { it.operation == IndexOperationRequest.Operation.Create }.size()
            int msg = items.findAll { ("Hello" == it.fields.get("msg")) }.size()
            int empties = items.findAll { ("" == it.fields.get("msg")) }.size()
            int nulls = items.findAll { (null == it.fields.get("msg")) }.size()
            int timestamp = items.findAll { it.fields.get("@timestamp") ==
                    LOCAL_DATE_TIME.format(DateTimeFormatter.ofPattern(RecordFieldType.TIMESTAMP.getDefaultFormat()))
            }.size()
            int timestampDefault = items.findAll { it.fields.get("@timestamp") == "test_timestamp" }.size()
            int ts = items.findAll { it.fields.get("ts") != null }.size()
            int id = items.findAll { it.fields.get("id") != null }.size()
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
            Assert.assertEquals(1, timestamp)
            Assert.assertEquals(5, timestampDefault)
            Assert.assertEquals(0, ts)
            Assert.assertEquals(0, id)
        }

        clientService.evalClosure = evalClosure

        registry.addSchema("recordPathTest", AvroTypeUtil.createSchema(new Schema.Parser().parse(newSchema)))

        runner.removeProperty(PutElasticsearchRecord.INDEX_OP)
        runner.setProperty(PutElasticsearchRecord.INDEX_OP_RECORD_PATH, "/op")
        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "/id")
        runner.setProperty(PutElasticsearchRecord.INDEX_RECORD_PATH, "/index")
        runner.setProperty(PutElasticsearchRecord.TYPE_RECORD_PATH, "/type")
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/ts")
        runner.enqueue(flowFileContents, [
            "schema.name": "recordPathTest"
        ])

        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0)

        runner.clearTransferState()

        flowFileContents = prettyPrint(toJson([
                [ id: "rec-1", op: null, index: null, type: null, msg: "Hello", date: Date.valueOf(LOCAL_DATE).getTime() ],
                [ id: "rec-2", op: null, index: null, type: null, msg: "Hello" ],
                [ id: "rec-3", op: null, index: null, type: null, msg: "Hello" ],
                [ id: "rec-4", op: null, index: null, type: null, msg: "Hello" ],
                [ id: "rec-5", op: "update", index: null, type: null, msg: "Hello" ],
                [ id: "rec-6", op: null, index: "bulk_b", type: "message", msg: "Hello" ]
        ]))

        evalClosure = { List<IndexOperationRequest> items ->
            def testTypeCount = items.findAll { it.type == "test_type" }.size()
            def messageTypeCount = items.findAll { it.type == "message" }.size()
            def testIndexCount = items.findAll { it.index == "test_index" }.size()
            def bulkIndexCount = items.findAll { it.index.startsWith("bulk_") }.size()
            def indexOperationCount = items.findAll { it.operation == IndexOperationRequest.Operation.Index }.size()
            def updateOperationCount = items.findAll { it.operation == IndexOperationRequest.Operation.Update }.size()
            def timestampCount = items.findAll { it.fields.get("@timestamp") ==
                    LOCAL_DATE.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"))
            }.size()
            int dateCount = items.findAll { it.fields.get("date") != null }.size()
            def idCount = items.findAll { it.fields.get("id") != null }.size()
            def defaultCoercedTimestampCount = items.findAll { it.fields.get("@timestamp") == 100L }.size()
            Assert.assertEquals(5, testTypeCount)
            Assert.assertEquals(1, messageTypeCount)
            Assert.assertEquals(5, testIndexCount)
            Assert.assertEquals(1, bulkIndexCount)
            Assert.assertEquals(5, indexOperationCount)
            Assert.assertEquals(1, updateOperationCount)
            Assert.assertEquals(1, timestampCount)
            Assert.assertEquals(5, defaultCoercedTimestampCount)
            Assert.assertEquals(1, dateCount)
            Assert.assertEquals(6, idCount)
        }

        clientService.evalClosure = evalClosure

        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "\${operation}")
        runner.setProperty(PutElasticsearchRecord.RETAIN_ID_FIELD, "true")
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP, "100")
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/date")
        runner.setProperty(PutElasticsearchRecord.DATE_FORMAT, "dd/MM/yyyy")
        runner.setProperty(PutElasticsearchRecord.RETAIN_AT_TIMESTAMP_FIELD, "true")
        runner.enqueue(flowFileContents, [
            "schema.name": "recordPathTest",
            "operation": "index"
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
                [ id: "rec-6", type: "message", msg: "Hello", time: Time.valueOf(LOCAL_TIME).getTime() ]
        ]))

        evalClosure = { List<IndexOperationRequest> items ->
            def nullTypeCount = items.findAll { it.type == null }.size()
            def messageTypeCount = items.findAll { it.type == "message" }.size()
            def nullIdCount = items.findAll { it.id == null }.size()
            def recIdCount = items.findAll { StringUtils.startsWith(it.id, "rec-") }.size()
            def timestampCount = items.findAll { it.fields.get("@timestamp") ==
                    LOCAL_TIME.format(DateTimeFormatter.ofPattern(RecordFieldType.TIME.getDefaultFormat()))
            }.size()
            Assert.assertEquals("null type", 5, nullTypeCount)
            Assert.assertEquals("message type", 1, messageTypeCount)
            Assert.assertEquals("null id", 2, nullIdCount)
            Assert.assertEquals("rec- id", 4, recIdCount)
            Assert.assertEquals("@timestamp", 1, timestampCount)
        }

        clientService.evalClosure = evalClosure

        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "index")
        runner.removeProperty(PutElasticsearchRecord.AT_TIMESTAMP)
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/time")
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
            [ id: "rec-6", op: "delete", index: "bulk_b", type: "message", msg: "Hello", code: 101L ]
        ]))

        clientService.evalClosure = { List<IndexOperationRequest> items ->
            int index = items.findAll { it.operation == IndexOperationRequest.Operation.Index }.size()
            int create = items.findAll { it.operation == IndexOperationRequest.Operation.Create }.size()
            int update = items.findAll { it.operation == IndexOperationRequest.Operation.Update }.size()
            int upsert = items.findAll { it.operation == IndexOperationRequest.Operation.Upsert }.size()
            int delete = items.findAll { it.operation == IndexOperationRequest.Operation.Delete }.size()
            def timestampCount = items.findAll { it.fields.get("@timestamp") == 101L }.size()
            def noTimestampCount = items.findAll { it.fields.get("@timestamp") == null }.size()
            Assert.assertEquals(1, index)
            Assert.assertEquals(2, create)
            Assert.assertEquals(1, update)
            Assert.assertEquals(1, upsert)
            Assert.assertEquals(1, delete)
            Assert.assertEquals(1, timestampCount)
            Assert.assertEquals(5, noTimestampCount)
        }

        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/code")
        runner.enqueue(flowFileContents, [
            "schema.name": "recordPathTest"
        ])
        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0)

        runner.clearTransferState()

        flowFileContents = prettyPrint(toJson([
                [ id: "rec-1", op: "index", index: "bulk_a", type: "message", msg: "Hello" ]
        ]))

        clientService.evalClosure = { List<IndexOperationRequest> items ->
            def timestampCount = items.findAll { it.fields.get("@timestamp") == "Hello" }.size()
            Assert.assertEquals(1, timestampCount)
        }

        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/msg")
        runner.enqueue(flowFileContents, [
                "schema.name": "recordPathTest"
        ])
        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0)
    }

    @Test
    void testDateTimeFormatting() {
        def newSchema = prettyPrint(toJson([
                type: "record",
                name: "DateTimeFormattingTestType",
                fields: [
                        [ name: "msg", type: ["null", "string"] ],
                        [ name: "ts", type: ["null", [ type: "long", logicalType: "timestamp-millis" ]] ],
                        [ name: "date", type: ["null", [ type: "int", logicalType: "date" ]] ],
                        [ name: "time", type: ["null", [ type: "int", logicalType: "time-millis" ]] ],
                        [ name: "choice_ts", type: ["null", [ type: "long", logicalType: "timestamp-millis" ], "string"] ]
                ]
        ]))

        def flowFileContents = prettyPrint(toJson([
                [ msg: "1", ts: Timestamp.valueOf(LOCAL_DATE_TIME).toInstant().toEpochMilli() ],
                [ msg: "2", date: Date.valueOf(LOCAL_DATE).getTime() ],
                [ msg: "3", time: Time.valueOf(LOCAL_TIME).getTime() ],
                [ msg: "4", choice_ts: Timestamp.valueOf(LOCAL_DATE_TIME).toInstant().toEpochMilli() ],
                [ msg: "5",
                  ts: Timestamp.valueOf(LOCAL_DATE_TIME).toInstant().toEpochMilli(),
                  time: Time.valueOf(LOCAL_TIME).getTime(),
                  date: Date.valueOf(LOCAL_DATE).getTime(),
                  choice_ts: "not-timestamp"
                ]
        ]))

        def evalClosure = { List<IndexOperationRequest> items ->
            int msg = items.findAll { (it.fields.get("msg") != null) }.size()
            int timestamp = items.findAll { it.fields.get("ts") ==
                    LOCAL_DATE_TIME.format(DateTimeFormatter.ofPattern(RecordFieldType.TIMESTAMP.getDefaultFormat())) // "yyyy-MM-dd HH:mm:ss"
            }.size()
            int date = items.findAll { it.fields.get("date") ==
                    LOCAL_DATE.format(DateTimeFormatter.ofPattern(RecordFieldType.DATE.getDefaultFormat())) // "yyyy-MM-dd"
            }.size()
            int time = items.findAll { it.fields.get("time") ==
                    LOCAL_TIME.format(DateTimeFormatter.ofPattern(RecordFieldType.TIME.getDefaultFormat())) // "HH:mm:ss"
            }.size()
            int choiceTs = items.findAll { it.fields.get("choice_ts") ==
                    LOCAL_DATE_TIME.format(DateTimeFormatter.ofPattern(RecordFieldType.TIMESTAMP.getDefaultFormat()))
            }.size()
            int choiceNotTs = items.findAll { it.fields.get("choice_ts") == "not-timestamp" }.size()
            int atTimestampDefault = items.findAll { it.fields.get("@timestamp") == "test_timestamp" }.size()
            int tsNull = items.findAll { it.fields.get("ts") == null }.size()
            int dateNull = items.findAll { it.fields.get("date") == null }.size()
            int timeNull = items.findAll { it.fields.get("time") == null }.size()
            int choiceTsNull = items.findAll { it.fields.get("choice_ts") == null }.size()
            Assert.assertEquals(5, msg)
            Assert.assertEquals(2, timestamp)
            Assert.assertEquals(2, date)
            Assert.assertEquals(2, time)
            Assert.assertEquals(1, choiceTs)
            Assert.assertEquals(1, choiceNotTs)
            Assert.assertEquals(3, tsNull)
            Assert.assertEquals(3, dateNull)
            Assert.assertEquals(3, timeNull)
            Assert.assertEquals(3, choiceTsNull)
            Assert.assertEquals(5, atTimestampDefault)
        }

        clientService.evalClosure = evalClosure

        registry.addSchema("dateTimeFormattingTest", AvroTypeUtil.createSchema(new Schema.Parser().parse(newSchema)))

        runner.enqueue(flowFileContents, [
                "schema.name": "dateTimeFormattingTest"
        ])

        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0)

        runner.clearTransferState()

        evalClosure = { List<IndexOperationRequest> items ->
            String timestampOutput = LOCAL_DATE_TIME.format(DateTimeFormatter.ofPattern("yy MMM D H"))
            int msg = items.findAll { (it.fields.get("msg") != null) }.size()
            int timestamp = items.findAll { it.fields.get("ts") == timestampOutput }.size()
            int date = items.findAll { it.fields.get("date") ==
                    LOCAL_DATE.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"))
            }.size()
            int time = items.findAll { it.fields.get("time") ==
                    // converted to a Long because the output is completely numerical
                    Long.parseLong(LOCAL_TIME.format(DateTimeFormatter.ofPattern("HHmmss")))
            }.size()
            int choiceTs = items.findAll { it.fields.get("choice_ts") == timestampOutput }.size()
            int choiceNotTs = items.findAll { it.fields.get("choice_ts") == "not-timestamp" }.size()
            int atTimestampDefault = items.findAll { it.fields.get("@timestamp") == "test_timestamp" }.size()
            int atTimestamp = items.findAll { it.fields.get("@timestamp") == timestampOutput }.size()
            int tsNull = items.findAll { it.fields.get("ts") == null }.size()
            int dateNull = items.findAll { it.fields.get("date") == null }.size()
            int timeNull = items.findAll { it.fields.get("time") == null }.size()
            int choiceTsNull = items.findAll { it.fields.get("choice_ts") == null }.size()
            Assert.assertEquals(5, msg)
            Assert.assertEquals(2, timestamp)
            Assert.assertEquals(2, date)
            Assert.assertEquals(2, time)
            Assert.assertEquals(1, choiceTs)
            Assert.assertEquals(1, choiceNotTs)
            Assert.assertEquals(3, tsNull)
            Assert.assertEquals(3, dateNull)
            Assert.assertEquals(3, timeNull)
            Assert.assertEquals(3, choiceTsNull)
            Assert.assertEquals(2, atTimestamp)
            Assert.assertEquals(3, atTimestampDefault)
        }

        clientService.evalClosure = evalClosure

        runner.setProperty(PutElasticsearchRecord.TIMESTAMP_FORMAT, "yy MMM D H")
        runner.setProperty(PutElasticsearchRecord.DATE_FORMAT, "dd/MM/yyyy")
        runner.setProperty(PutElasticsearchRecord.TIME_FORMAT, "HHmmss")
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/ts")
        runner.setProperty(PutElasticsearchRecord.RETAIN_AT_TIMESTAMP_FIELD, "true")

        runner.enqueue(flowFileContents, [
                "schema.name": "dateTimeFormattingTest"
        ])

        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0)
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0)
    }

    @Test
    void testInvalidIndexOperation() {
        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "not-valid")
        runner.assertNotValid()
        final AssertionError ae = Assert.assertThrows(AssertionError.class, runner.&run)
        Assert.assertEquals(String.format("Processor has 1 validation failures:\n'%s' validated against 'not-valid' is invalid because %s must be Expression Language or one of %s\n",
                PutElasticsearchRecord.INDEX_OP.getName(), PutElasticsearchRecord.INDEX_OP.getDisplayName(), PutElasticsearchRecord.ALLOWED_INDEX_OPERATIONS),
                ae.getMessage()
        )

        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "\${operation}")
        runner.assertValid()
        runner.enqueue(flowFileContents, [
                "operation": "not-valid2"
        ])
        runner.run()
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 1)
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
