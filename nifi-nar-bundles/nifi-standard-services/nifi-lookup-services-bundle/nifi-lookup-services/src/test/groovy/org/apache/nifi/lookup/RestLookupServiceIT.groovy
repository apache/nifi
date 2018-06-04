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

package org.apache.nifi.lookup

import org.apache.avro.Schema
import org.apache.nifi.avro.AvroTypeUtil
import org.apache.nifi.json.JsonTreeReader
import org.apache.nifi.lookup.rest.SchemaUtil
import org.apache.nifi.lookup.rest.handlers.BasicAuth
import org.apache.nifi.lookup.rest.handlers.ComplexJson
import org.apache.nifi.lookup.rest.handlers.NoRecord
import org.apache.nifi.lookup.rest.handlers.SimpleJson
import org.apache.nifi.lookup.rest.handlers.SimpleJsonArray
import org.apache.nifi.lookup.rest.handlers.VerbTest
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.record.MockSchemaRegistry
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.apache.nifi.web.util.TestServer
import org.eclipse.jetty.servlet.ServletHandler
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class RestLookupServiceIT {
    static final JsonTreeReader reader
    static final MockSchemaRegistry registry = new MockSchemaRegistry()
    static final RecordSchema simpleSchema
    static final RecordSchema nestedSchema

    TestRunner runner
    RestLookupService lookupService

    static {
        simpleSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(SchemaUtil.SIMPLE))
        nestedSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(SchemaUtil.COMPLEX))
        registry.addSchema("simple", simpleSchema)
        registry.addSchema("complex", nestedSchema)

        reader = new JsonTreeReader()
    }

    @Before
    void setup() {
        lookupService = new RestLookupService()

        runner = TestRunners.newTestRunner(TestRestLookupServiceProcessor.class)
        runner.addControllerService("jsonReader", reader)
        runner.addControllerService("registry", registry)
        runner.addControllerService("lookupService", lookupService)
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_REGISTRY, "registry")
        runner.setProperty(lookupService, SchemaAccessUtils.SCHEMA_REGISTRY, "registry")
        runner.setProperty(lookupService, RestLookupService.RECORD_READER, "jsonReader")
        runner.setProperty(TestRestLookupServiceProcessor.CLIENT_SERVICE, "lookupService")
        runner.enableControllerService(registry)
        runner.enableControllerService(reader)
        runner.enableControllerService(lookupService)

        runner.assertValid()
    }

    @Test
    void basicAuth() {
        runner.disableControllerService(lookupService)
        runner.setProperty(lookupService, RestLookupService.PROP_BASIC_AUTH_USERNAME, "john.smith")
        runner.setProperty(lookupService, RestLookupService.PROP_BASIC_AUTH_PASSWORD, "testing1234")
        runner.enableControllerService(lookupService)

        TestServer server = new TestServer()
        server.addHandler(new BasicAuth())
        try {
            server.startServer()
            def coordinates = [
                "schema.name": "simple",
                "endpoint": server.url + "/simple",
                "mime.type": "application/json",
                "request.method": "get"
            ]

            Optional<Record> response = lookupService.lookup(coordinates)
            Assert.assertTrue(response.isPresent())
            def record = response.get()
            Assert.assertEquals("john.smith", record.getAsString("username"))
            Assert.assertEquals("testing1234", record.getAsString("password"))

            Throwable t
            try {
                runner.disableControllerService(lookupService)
                runner.setProperty(lookupService, RestLookupService.PROP_BASIC_AUTH_USERNAME, "john.smith2")
                runner.setProperty(lookupService, RestLookupService.PROP_BASIC_AUTH_PASSWORD, ":wetadfasdfadf")
                runner.enableControllerService(lookupService)

                lookupService.lookup(coordinates)
            } catch (Throwable lfe) {
                t = lfe
            }

            Assert.assertNotNull(t)
            Assert.assertTrue(t.getClass().getCanonicalName(), t instanceof LookupFailureException)
        } finally {
            server.shutdownServer()
        }
    }

    @Test
    void simpleJson() {
        TestServer server = new TestServer()
        ServletHandler handler = new ServletHandler()
        handler.addServletWithMapping(SimpleJson.class, "/simple")
        server.addHandler(handler)
        try {
            server.startServer()
            def coordinates = [
                "schema.name": "simple",
                "endpoint": server.url + "/simple",
                "mime.type": "application/json",
                "request.method": "get"
            ]

            Optional<Record> response = lookupService.lookup(coordinates)
            Assert.assertTrue(response.isPresent())
            def record = response.get()
            Assert.assertEquals("john.smith", record.getAsString("username"))
            Assert.assertEquals("testing1234", record.getAsString("password"))
        } finally {
            server.shutdownServer()
        }
    }

    @Test
    void noRecord() {
        TestServer server = new TestServer()
        ServletHandler handler = new ServletHandler()
        handler.addServletWithMapping(NoRecord.class, "/simple")
        server.addHandler(handler)
        try {
            server.startServer()
            def coordinates = [
                "schema.name": "simple",
                "endpoint": server.url + "/simple",
                "mime.type": "application/json",
                "request.method": "get"
            ]

            Optional<Record> response = lookupService.lookup(coordinates)
            Assert.assertTrue(response.isPresent())
            def record = response.get()
            Assert.assertNull(record.getAsString("username"))
            Assert.assertNull(record.getAsString("password"))
        } finally {
            server.shutdownServer()
        }
    }

    @Test
    void simpleJsonArray() {
        TestServer server = new TestServer()
        ServletHandler handler = new ServletHandler()
        handler.addServletWithMapping(SimpleJsonArray.class, "/simple_array")
        server.addHandler(handler)
        try {
            server.startServer()
            def coordinates = [
                "schema.name": "simple",
                "endpoint": server.url + "/simple_array",
                "mime.type": "application/json",
                "request.method": "get"
            ]

            Optional<Record> response = lookupService.lookup(coordinates)
            Assert.assertTrue(response.isPresent())
            def record = response.get()
            Assert.assertEquals("john.smith", record.getAsString("username"))
            Assert.assertEquals("testing1234", record.getAsString("password"))
        } finally {
            server.shutdownServer()
        }
    }

    @Test
    void testHeaders() {
        runner.disableControllerService(lookupService)
        runner.setProperty(lookupService, "X-USER", "jane.doe")
        runner.setProperty(lookupService, "X-PASS", "testing7890")
        runner.enableControllerService(lookupService)

        TestServer server = new TestServer()
        ServletHandler handler = new ServletHandler()
        handler.addServletWithMapping(SimpleJson.class, "/simple")
        server.addHandler(handler)
        try {
            server.startServer()

            def coordinates = [
                "schema.name": "simple",
                "endpoint": server.url + "/simple",
                "mime.type": "application/json",
                "request.method": "get"
            ]

            Optional<Record> response = lookupService.lookup(coordinates)
            Assert.assertTrue(response.isPresent())
            def record = response.get()
            Assert.assertEquals("jane.doe", record.getAsString("username"))
            Assert.assertEquals("testing7890", record.getAsString("password"))
        } finally {
            server.shutdownServer()
        }
    }

    @Test
    void complexJson() {
        runner.disableControllerService(lookupService)
        runner.setProperty(lookupService, RestLookupService.RECORD_PATH, "/top/middle/inner")
        runner.enableControllerService(lookupService)

        TestServer server = new TestServer()
        ServletHandler handler = new ServletHandler()
        handler.addServletWithMapping(ComplexJson.class, "/complex")
        server.addHandler(handler)
        try {
            server.startServer()
            def coordinates = [
                "schema.name": "complex",
                "endpoint": server.url + "/complex",
                "mime.type": "application/json",
                "request.method": "get"
            ]

            Optional<Record> response = lookupService.lookup(coordinates)
            Assert.assertTrue(response.isPresent())
            def record = response.get()
            Assert.assertEquals("jane.doe", record.getAsString("username"))
            Assert.assertEquals("testing7890", record.getAsString("password"))
            Assert.assertEquals("jane.doe@test-example.com", record.getAsString("email"))
        } finally {
            server.shutdownServer()
        }
    }

    @Test
    void testOtherVerbs() {
        TestServer server = new TestServer()
        ServletHandler handler = new ServletHandler()
        handler.addServletWithMapping(VerbTest.class, "/simple")
        server.addHandler(handler)
        try {
            runner.disableControllerService(lookupService)
            runner.setProperty(lookupService, "needs-body", "true")
            runner.enableControllerService(lookupService)
            server.startServer()

            def validation = { String verb, boolean addBody ->
                def coordinates = [
                    "schema.name"   : "simple",
                    "endpoint"      : server.url + "/simple",
                    "mime.type"     : "application/json",
                    "request.method": verb
                ]

                if (addBody) {
                    coordinates["request.body"] = prettyPrint(toJson([ msg: "Hello, world" ]))
                }

                Optional<Record> response = lookupService.lookup(coordinates)
                Assert.assertTrue(response.isPresent())
                def record = response.get()
                Assert.assertEquals("john.smith", record.getAsString("username"))
                Assert.assertEquals("testing1234", record.getAsString("password"))
            }

            ["delete", "post", "put"].each { verb ->
                validation(verb, true)
            }

            runner.disableControllerService(lookupService)
            runner.setProperty(lookupService, "needs-body", "")
            runner.enableControllerService(lookupService)

            validation("delete", false)
        } finally {
            server.shutdownServer()
        }
    }

    @Test
    void testTemplateMode() {
        TestServer server = new TestServer()
        ServletHandler handler = new ServletHandler()
        handler.addServletWithMapping(SimpleJson.class, "/simple/john.smith/friends/12345")
        server.addHandler(handler)
        try {
            server.startServer()
            def coordinates = [
                "schema.name": "simple",
                "endpoint": server.url + '/simple/${user.name}/friends/${friend.id}',
                "mime.type": "application/json",
                "request.method": "get",
                "user.name": "john.smith",
                "friend.id": 12345,
                "endpoint.template": true
            ]

            Optional<Record> response = lookupService.lookup(coordinates)
            Assert.assertTrue(response.isPresent())
            def record = response.get()
            Assert.assertEquals("john.smith", record.getAsString("username"))
            Assert.assertEquals("testing1234", record.getAsString("password"))
        } finally {
            server.shutdownServer()
        }
    }
}
