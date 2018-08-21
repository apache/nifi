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

package org.apache.nifi.processors.generation

import org.apache.avro.Schema
import org.apache.nifi.avro.AvroTypeUtil
import org.apache.nifi.json.JsonRecordSetWriter
import org.apache.nifi.json.JsonTreeReader
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.RecordSetWriterFactory
import org.apache.nifi.serialization.record.MockSchemaRegistry
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class TestGenerateRecord {
    TestRunner runner
    MockSchemaRegistry registry
    RecordSetWriterFactory writer
    RecordReaderFactory reader

    @Before
    void setup() {
        runner = TestRunners.newTestRunner(GenerateRecord.class)
        writer = new JsonRecordSetWriter()
        reader = new JsonTreeReader()
        registry = new MockSchemaRegistry()
        runner.addControllerService("writer", writer)
        runner.addControllerService("registry", registry)
        runner.addControllerService("reader", reader)
        [reader, writer].each {
            runner.setProperty(it, SchemaAccessUtils.SCHEMA_REGISTRY, "registry")
            runner.setProperty(it, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY)
        }
        runner.setProperty(GenerateRecord.WRITER, "writer")
        runner.enableControllerService(registry)
        runner.enableControllerService(writer)
        runner.enableControllerService(reader)
    }

    @Test
    void testValidity() {
        runner.assertValid()
    }

    @Test
    void testRun() {
        def simpleSchema = prettyPrint(toJson([
            type: "record",
            name: "SimpleTestRecord",
            fields: [
                [ name: "msg", type: "string" ]
            ]
        ]))

        def attrs = [ "schema.name": "really_simple"]

        def validator = { byte[] input, int expected, boolean fixed ->
            def is = new ByteArrayInputStream(input)
            def jsonReader = reader.createRecordReader(attrs, is, runner.getLogger())
            int count = 0
            def rec = jsonReader.nextRecord()
            while (rec) {
                count++
                rec = jsonReader.nextRecord()
            }
            if (fixed) {
                Assert.assertEquals(expected, count)
            } else {
                Assert.assertTrue(count <= expected && count > 0)
            }
        }

        registry.addSchema("really_simple", AvroTypeUtil.createSchema(new Schema.Parser().parse(simpleSchema)))
        runner.enqueue("", attrs)
        runner.run()

        runner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(GenerateRecord.REL_ORIGINAL, 1)
        runner.assertTransferCount(GenerateRecord.REL_FAILURE, 0)

        validator(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS)[0]), 25, true)

        runner.clearTransferState()

        attrs['generate.limit'] = "100"
        runner.setProperty(GenerateRecord.LIMIT, '${generate.limit}')
        runner.enqueue("", attrs)
        runner.run()

        runner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(GenerateRecord.REL_ORIGINAL, 1)
        runner.assertTransferCount(GenerateRecord.REL_FAILURE, 0)

        validator(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS)[0]), 100, true)

        runner.clearTransferState()
        runner.setProperty(GenerateRecord.FIXED_SIZE, "false")
        runner.setProperty(GenerateRecord.LIMIT, '${generate.limit}')
        runner.enqueue("", attrs)
        runner.run()

        runner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(GenerateRecord.REL_ORIGINAL, 1)
        runner.assertTransferCount(GenerateRecord.REL_FAILURE, 0)
        validator(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS)[0]), 100, false)

        runner.clearTransferState()
        runner.setIncomingConnection(false)
        runner.setProperty(GenerateRecord.SCHEMA, simpleSchema)
        runner.setVariable("generate.limit", "100")
        runner.setProperty(GenerateRecord.FIXED_SIZE, "true")
        runner.run()
        runner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(GenerateRecord.REL_ORIGINAL, 0)
        runner.assertTransferCount(GenerateRecord.REL_FAILURE, 0)
        validator(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS)[0]), 100, false)
    }

    @Test
    void testNumericParameters() {
        def simpleSchema = prettyPrint(toJson([
            type: "record",
            name: "SimpleTestRecord",
            fields: [
                [ name: "msg", type: [
                    type: "long",
                    "arg.properties": [
                        length: [
                            min: 5,
                            max: 10
                        ]
                    ]
                ]]
            ]
        ]))

        def attrs = [ "schema.name": "really_simple" ]

        def validator = { byte[] input, int expected ->
            def is = new ByteArrayInputStream(input)
            def jsonReader = reader.createRecordReader(attrs, is, runner.getLogger())
            int count = 0
            def rec = jsonReader.nextRecord()
            while (rec) {
                count++
                def msg = rec.getAsLong("msg")
                Assert.assertNotNull(msg)
                Assert.assertTrue(msg instanceof Long)
                rec = jsonReader.nextRecord()
            }
            Assert.assertEquals(expected, count)
        }

        registry.addSchema("really_simple", AvroTypeUtil.createSchema(new Schema.Parser().parse(simpleSchema)))
        runner.enqueue("", attrs)
        runner.run()
        runner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(GenerateRecord.REL_ORIGINAL, 1)
        runner.assertTransferCount(GenerateRecord.REL_FAILURE, 0)

        validator(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS)[0]), 25)
    }

    @Test
    void testStringParameters() {
        def options = ["the", "this", "that"]
        def simpleSchema = prettyPrint(toJson([
            type: "record",
            name: "SimpleTestRecord",
            fields: [
                [ name: "msg", type: [
                    type: "string",
                    "arg.properties": [
                        length: [
                            min: 5,
                            max: 10
                        ],
                        regex: "[a-zA-Z]{5,10}"
                    ]
                ]],
                [ name: "msg2", type: [
                    type: "string",
                    "arg.properties": [
                        options: options
                    ]
                ]]
            ]
        ]))

        def attrs = [ "schema.name": "really_simple" ]

        def validator = { byte[] input, int expected ->
            def is = new ByteArrayInputStream(input)
            def jsonReader = reader.createRecordReader(attrs, is, runner.getLogger())
            int count = 0
            def rec = jsonReader.nextRecord()
            while (rec) {
                count++
                def str = rec.getAsString("msg")
                def str2 = rec.getAsString("msg2")
                Assert.assertNotNull(str)
                Assert.assertNotNull(str2)
                Assert.assertTrue(str.length() <= 10 && str.length() >= 5)
                Assert.assertTrue(options.contains(str2))
                rec = jsonReader.nextRecord()
            }
            Assert.assertEquals(expected, count)
        }

        registry.addSchema("really_simple", AvroTypeUtil.createSchema(new Schema.Parser().parse(simpleSchema)))
        runner.enqueue("", attrs)
        runner.run()
        runner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(GenerateRecord.REL_ORIGINAL, 1)
        runner.assertTransferCount(GenerateRecord.REL_FAILURE, 0)

        byte[] input = runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS)[0])
        validator(input, 25)
    }

    @Test
    void testMapField() {
        def schema = this.class.getResourceAsStream("/map_test.avsc")
        def attrs  = [ "schema.name": "map_test" ]

        def validator = { byte[] input, int expected ->
            def is = new ByteArrayInputStream(input)
            def jsonReader = reader.createRecordReader(attrs, is, runner.getLogger())
            int count = 0
            def rec = jsonReader.nextRecord()
            while (rec) {
                count++
                def map = rec.getValue("map_field")
                Assert.assertNotNull(map)
                Assert.assertTrue(map instanceof Map)
                map.each { Assert.assertTrue(it.value instanceof Integer) }
                rec = jsonReader.nextRecord()
            }
            Assert.assertEquals(expected, count)
        }

        registry.addSchema("map_test", AvroTypeUtil.createSchema(new Schema.Parser().parse(schema)))
        runner.enqueue("", attrs)
        runner.run()
        runner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(GenerateRecord.REL_ORIGINAL, 1)
        runner.assertTransferCount(GenerateRecord.REL_FAILURE, 0)

        byte[] input = runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS)[0])
        validator(input, 25)
    }
}
