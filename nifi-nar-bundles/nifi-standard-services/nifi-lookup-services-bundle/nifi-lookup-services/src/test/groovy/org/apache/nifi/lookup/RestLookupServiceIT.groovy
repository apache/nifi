package org.apache.nifi.lookup

import org.apache.avro.Schema
import org.apache.nifi.avro.AvroTypeUtil
import org.apache.nifi.json.JsonTreeReader
import org.apache.nifi.lookup.rest.SchemaUtil
import org.apache.nifi.lookup.rest.handlers.ComplexJson
import org.apache.nifi.lookup.rest.handlers.SimpleJson
import org.apache.nifi.lookup.rest.handlers.SimpleJsonArray
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
        runner.setProperty(lookupService, "header.X-USER", "jane.doe")
        runner.setProperty(lookupService, "header.X-PASS", "testing7890")
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
            Assert.assertEquals("jane.doe@company.com", record.getAsString("email"))
        } finally {
            server.shutdownServer()
        }
    }
}
