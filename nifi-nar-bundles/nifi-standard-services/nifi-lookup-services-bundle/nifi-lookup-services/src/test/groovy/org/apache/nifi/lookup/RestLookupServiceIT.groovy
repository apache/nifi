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

    TestServer server
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
    }

    @Test
    void basicAuth() {
        runner.setProperty(lookupService, RestLookupService.PROP_BASIC_AUTH_USERNAME, "john.smith")
        runner.setProperty(lookupService, RestLookupService.PROP_BASIC_AUTH_PASSWORD, "testing1234")

        TestServer server = new TestServer()
        server.addHandler(new BasicAuth())
        try {
            server.startServer()

            setEndpoint(server.port, "/simple")

            def coordinates = [
                "mime.type": "application/json",
                "request.method": "get"
            ]

            def context = [ "schema.name": "simple" ]

            Optional<Record> response = lookupService.lookup(coordinates, context)
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

            setEndpoint(server.port, "/simple")

            def coordinates = [
                "mime.type": "application/json",
                "request.method": "get"
            ]

            def context = [ "schema.name": "simple" ]

            Optional<Record> response = lookupService.lookup(coordinates, context)
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

            setEndpoint(server.port, "/simple")

            def coordinates = [
                "mime.type": "application/json",
                "request.method": "get"
            ]

            def context = [ "schema.name": "simple" ]

            Optional<Record> response = lookupService.lookup(coordinates, context)
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

            setEndpoint(server.port, "/simple_array")

            def coordinates = [
                "mime.type": "application/json",
                "request.method": "get"
            ]

            def context = [ "schema.name": "simple" ]

            Optional<Record> response = lookupService.lookup(coordinates, context)
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
        runner.setProperty(lookupService, "X-USER", "jane.doe")
        runner.setProperty(lookupService, "X-PASS", "testing7890")

        TestServer server = new TestServer()
        ServletHandler handler = new ServletHandler()
        handler.addServletWithMapping(SimpleJson.class, "/simple")
        server.addHandler(handler)
        try {
            server.startServer()

            setEndpoint(server.port, "/simple")

            def coordinates = [
                "mime.type": "application/json",
                "request.method": "get"
            ]

            def context = [ "schema.name": "simple" ]

            Optional<Record> response = lookupService.lookup(coordinates, context)
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
        runner.setProperty(lookupService, RestLookupService.RECORD_PATH, "/top/middle/inner")

        TestServer server = new TestServer()
        ServletHandler handler = new ServletHandler()
        handler.addServletWithMapping(ComplexJson.class, "/complex")
        server.addHandler(handler)
        try {
            server.startServer()

            setEndpoint(server.port, "/complex")

            def coordinates = [
                "mime.type": "application/json",
                "request.method": "get"
            ]

            def context = [ "schema.name": "complex" ]

            Optional<Record> response = lookupService.lookup(coordinates, context)
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
            server.startServer()

            setEndpoint(server.port, "/simple")

            def validation = { String verb, boolean addBody, boolean addMimeType, boolean valid ->
                def coordinates = [
                    "mime.type"     : addMimeType ? "application/json" : null,
                    "request.method": verb
                ]

                def context = [ "schema.name": "simple" ]

                if (addBody) {
                    coordinates["request.body"] = prettyPrint(toJson([ msg: "Hello, world" ]))
                }

                try {
                    Optional<Record> response = lookupService.lookup(coordinates, context)
                    if (!valid) {
                        Assert.fail("Validation should fail.")
                    }

                    Assert.assertTrue(response.isPresent())
                    def record = response.get()
                    Assert.assertEquals("john.smith", record.getAsString("username"))
                    Assert.assertEquals("testing1234", record.getAsString("password"))

                } catch (LookupFailureException e) {
                    if (valid) {
                        Assert.fail("Validation should be successful.")
                    }
                }
            }

            // Delete does not require body nor mimeType.
            validation("delete", false, false, true)

            // Post and Put require body and mimeType.
            ["post", "put"].each { verb ->
                validation(verb, false, false, false)
                validation(verb, true, false, false)
                validation(verb, true, true, true)
            }

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

            setEndpoint(server.port, '/simple/${user.name}/friends/${friend.id}')

            def coordinates = [
                "mime.type": "application/json",
                "request.method": "get",
                "user.name": "john.smith",
                "friend.id": 12345,
                "endpoint.template": true
            ]

            def context = [ "schema.name": "simple" ]

            Optional<Record> response = lookupService.lookup(coordinates, context)
            Assert.assertTrue(response.isPresent())
            def record = response.get()
            Assert.assertEquals("john.smith", record.getAsString("username"))
            Assert.assertEquals("testing1234", record.getAsString("password"))
        } finally {
            server.shutdownServer()
        }
    }

    void setEndpoint(Integer serverPort, String endpoint) {
        // Resolve environmental part of the URL via variable registry.
        runner.setVariable("serverPort", String.valueOf(serverPort))
        runner.setProperty(lookupService, RestLookupService.URL, "http://localhost:${serverPort}" + endpoint)
        runner.enableControllerService(lookupService)

        runner.assertValid()
    }
}
