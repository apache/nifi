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

import okhttp3.*
import org.apache.nifi.lookup.rest.MockRestLookupService
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import static groovy.json.JsonOutput.toJson

class TestRestLookupService {
    TestRunner runner
    MockRecordParser recordReader
    MockRestLookupService lookupService

    static final String JSON_TYPE = "application/json"

    @Before
    void setup() {
        recordReader = new MockRecordParser()
        lookupService = new MockRestLookupService()
        runner = TestRunners.newTestRunner(TestRestLookupServiceProcessor.class)
        runner.addControllerService("lookupService", lookupService)
        runner.addControllerService("recordReader", recordReader)
        runner.setProperty(lookupService, RestLookupService.RECORD_READER, "recordReader")
        runner.setProperty("Lookup Service", "lookupService")
        runner.setProperty(lookupService, RestLookupService.URL, "http://localhost:8080")
        runner.enableControllerService(lookupService)
        runner.enableControllerService(recordReader)
        runner.assertValid()
    }

    @Test
    void testSimpleLookup() {
        recordReader.addSchemaField("name", RecordFieldType.STRING)
        recordReader.addSchemaField("age", RecordFieldType.INT)
        recordReader.addSchemaField("sport", RecordFieldType.STRING)

        recordReader.addRecord("John Doe", 48, "Soccer")
        recordReader.addRecord("Jane Doe", 47, "Tennis")
        recordReader.addRecord("Sally Doe", 47, "Curling")

        lookupService.response = buildResponse(toJson([ simpleTest: true]), JSON_TYPE)
        def result = lookupService.lookup(getCoordinates(JSON_TYPE, "get"))
        Assert.assertTrue(result.isPresent())
        def record = result.get()
        Assert.assertEquals("John Doe", record.getAsString("name"))
        Assert.assertEquals(48, record.getAsInt("age"))
        Assert.assertEquals("Soccer", record.getAsString("sport"))
    }
    
    @Test
    void testNestedLookup() {
        runner.disableControllerService(lookupService)
        runner.setProperty(lookupService, RestLookupService.RECORD_PATH, "/person")
        runner.enableControllerService(lookupService)
        runner.assertValid()

        recordReader.addSchemaField("id", RecordFieldType.INT)
        final List<RecordField> personFields = new ArrayList<>()
        final RecordField nameField = new RecordField("name", RecordFieldType.STRING.getDataType())
        final RecordField ageField = new RecordField("age", RecordFieldType.INT.getDataType())
        final RecordField sportField = new RecordField("sport", RecordFieldType.STRING.getDataType())
        personFields.add(nameField)
        personFields.add(ageField)
        personFields.add(sportField)
        final RecordSchema personSchema = new SimpleRecordSchema(personFields)
        recordReader.addSchemaField("person", RecordFieldType.RECORD)
        recordReader.addRecord(1, new MapRecord(personSchema, new HashMap<String,Object>() {{
            put("name", "John Doe")
            put("age", 48)
            put("sport", "Soccer")
        }}))

        lookupService.response = buildResponse(toJson([ simpleTest: true]), JSON_TYPE)
        def result = lookupService.lookup(getCoordinates(JSON_TYPE, "get"))
        Assert.assertTrue(result.isPresent())
        def record = result.get()

        Assert.assertEquals("John Doe", record.getAsString("name"))
        Assert.assertEquals(48, record.getAsInt("age"))
        Assert.assertEquals("Soccer", record.getAsString("sport"))

        /*
         * Test deep lookup
         */

        runner.disableControllerService(lookupService)
        runner.setProperty(lookupService, RestLookupService.RECORD_PATH, "/person/sport")
        runner.enableControllerService(lookupService)
        runner.assertValid()

        result = lookupService.lookup(getCoordinates(JSON_TYPE, "get"))
        Assert.assertTrue(result.isPresent())
        record = result.get()
        Assert.assertNotNull(record.getAsString("sport"))
        Assert.assertEquals("Soccer", record.getAsString("sport"))
    }

    private Map<String, Object> getCoordinates(String mimeType, String method) {
        def retVal = [:]
        retVal[RestLookupService.MIME_TYPE_KEY] = mimeType
        retVal[RestLookupService.METHOD_KEY] = method

        retVal
    }

    private Response buildResponse(String resp, String mimeType) {
        return new Response.Builder()
            .code(200)
            .body(
                ResponseBody.create(MediaType.parse(mimeType), resp)
            )
            .message("Test")
            .protocol(Protocol.HTTP_1_1)
            .request(new Request.Builder().url("http://localhost:8080").get().build())
            .build()
    }
}
