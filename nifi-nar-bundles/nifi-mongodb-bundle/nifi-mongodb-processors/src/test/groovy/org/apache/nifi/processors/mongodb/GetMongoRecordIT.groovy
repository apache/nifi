/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.mongodb

import groovy.json.JsonSlurper
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.json.JsonRecordSetWriter
import org.apache.nifi.mongodb.MongoDBClientService
import org.apache.nifi.mongodb.MongoDBControllerService
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.DateTimeUtils
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.*
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.bson.Document
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import static groovy.json.JsonOutput.*

class GetMongoRecordIT {
    TestRunner runner
    MongoDBClientService service

    static RecordSchema SCHEMA
    static final String DB_NAME = GetMongoRecord.class.simpleName + Calendar.instance.timeInMillis
    static final String COL_NAME = "test"
    static final String URI = "mongodb://localhost:27017"

    static {
        def fields = [
            new RecordField("name", RecordFieldType.STRING.dataType),
            new RecordField("failedLogins", RecordFieldType.INT.dataType),
            new RecordField("lastLogin", RecordFieldType.DATE.dataType)
        ]
        SCHEMA = new SimpleRecordSchema(fields, new StandardSchemaIdentifier.Builder().name("sample").build())
    }

    static final List<Map> SAMPLES = [
        [ name: "John Smith", failedLogins: 2, lastLogin: Calendar.instance.time ],
        [ name: "Jane Doe", failedLogins: 1, lastLogin: Calendar.instance.time - 360000 ],
        [ name: "John Brown", failedLogins: 4, lastLogin: Calendar.instance.time - 10000 ]
    ].collect { new Document(it) }

    @Before
    void setup() {
        runner = TestRunners.newTestRunner(GetMongoRecord.class)
        service = new MongoDBControllerService()
        runner.addControllerService("client", service)
        runner.setProperty(service, MongoDBControllerService.URI, URI)
        runner.enableControllerService(service)

        def writer = new JsonRecordSetWriter()
        def registry = new MockSchemaRegistry()
        registry.addSchema("sample", SCHEMA)

        runner.addControllerService("writer", writer)
        runner.addControllerService("registry", registry)
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry")
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY)
        runner.setProperty(writer, DateTimeUtils.DATE_FORMAT, "yyyy")
        runner.enableControllerService(registry)
        runner.enableControllerService(writer)

        runner.setProperty(GetMongoRecord.DATABASE_NAME, DB_NAME)
        runner.setProperty(GetMongoRecord.COLLECTION_NAME, COL_NAME)
        runner.setProperty(GetMongoRecord.CLIENT_SERVICE, "client")
        runner.setProperty(GetMongoRecord.WRITER_FACTORY, "writer")

        service.getDatabase(DB_NAME).getCollection(COL_NAME).insertMany(SAMPLES)
    }

    @After
    void after() {
        service.getDatabase(DB_NAME).drop()
    }

    @Test
    void testLookup() {
        def ffValidator = { TestRunner runner ->
            def ffs = runner.getFlowFilesForRelationship(GetMongoRecord.REL_SUCCESS)
            Assert.assertNotNull(ffs)
            Assert.assertTrue(ffs.size() == 1)
            Assert.assertEquals("3", ffs[0].getAttribute("record.count"))
            Assert.assertEquals("application/json", ffs[0].getAttribute(CoreAttributes.MIME_TYPE.key()))
            Assert.assertEquals(COL_NAME, ffs[0].getAttribute(GetMongoRecord.COL_NAME))
            Assert.assertEquals(DB_NAME, ffs[0].getAttribute(GetMongoRecord.DB_NAME))
            Assert.assertEquals(Document.parse("{}"), Document.parse(ffs[0].getAttribute("executed.query")))
        }

        runner.setProperty(GetMongoRecord.QUERY_ATTRIBUTE, "executed.query")
        runner.setProperty(GetMongoRecord.QUERY, "{}")
        runner.enqueue("", [ "schema.name": "sample"])
        runner.run()

        runner.assertTransferCount(GetMongoRecord.REL_FAILURE, 0)
        runner.assertTransferCount(GetMongoRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(GetMongoRecord.REL_ORIGINAL, 1)

        ffValidator(runner)

        runner.clearTransferState()
        runner.removeProperty(GetMongoRecord.QUERY)
        runner.enqueue("{}", [ "schema.name": "sample"])
        runner.run()

        runner.assertTransferCount(GetMongoRecord.REL_FAILURE, 0)
        runner.assertTransferCount(GetMongoRecord.REL_SUCCESS, 1)
        runner.assertTransferCount(GetMongoRecord.REL_ORIGINAL, 1)

        ffValidator(runner)
    }

    @Test
    void testSortAndProjection() {
        runner.setIncomingConnection(false)
        runner.setVariable("schema.name", "sample")
        runner.setProperty(GetMongoRecord.SORT, toJson([failedLogins: 1]))
        runner.setProperty(GetMongoRecord.PROJECTION, toJson([failedLogins: 1]))
        runner.setProperty(GetMongoRecord.QUERY, "{}")
        runner.run()

        def parsed = sharedTest()
        Assert.assertEquals(3, parsed.size())
        def values = [1, 2, 4]
        int index = 0
        parsed.each {
            Assert.assertEquals(values[index++], it["failedLogins"])
            Assert.assertNull(it["name"])
            Assert.assertNull(it["lastLogin"])
        }
    }

    List<Map<String, Object>> sharedTest() {
        runner.assertTransferCount(GetMongoRecord.REL_FAILURE, 0)
        runner.assertTransferCount(GetMongoRecord.REL_SUCCESS, 1)

        def ff = runner.getFlowFilesForRelationship(GetMongoRecord.REL_SUCCESS)[0]
        def raw = runner.getContentAsByteArray(ff)
        String content = new String(raw)
        def parsed = new JsonSlurper().parseText(content)
        Assert.assertNotNull(parsed)

        parsed
    }

    @Test
    void testLimit() {
        runner.setIncomingConnection(false)
        runner.setProperty(GetMongoRecord.LIMIT, "1")
        runner.setProperty(GetMongoRecord.QUERY, "{}")
        runner.setVariable("schema.name", "sample")
        runner.run()

        def parsed = sharedTest()
        Assert.assertEquals(1, parsed.size())

    }
}
