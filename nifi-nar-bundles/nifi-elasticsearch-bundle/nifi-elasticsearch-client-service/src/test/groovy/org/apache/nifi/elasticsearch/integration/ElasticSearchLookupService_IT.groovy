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

package org.apache.nifi.elasticsearch.integration

import org.apache.nifi.elasticsearch.ElasticSearchClientService
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl
import org.apache.nifi.elasticsearch.ElasticSearchLookupService
import org.apache.nifi.lookup.LookupFailureException
import org.apache.nifi.record.path.RecordPath
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.schemaregistry.services.SchemaRegistry
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.serialization.record.type.RecordDataType
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Before
import org.junit.Test

class ElasticSearchLookupService_IT {
    private TestRunner runner
    private ElasticSearchClientService service
    private ElasticSearchLookupService lookupService

    @Before
    void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class)
        service = new ElasticSearchClientServiceImpl()
        lookupService = new ElasticSearchLookupService()
        runner.addControllerService("Client Service", service)
        runner.addControllerService("Lookup Service", lookupService)
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, "http://localhost:9400")
        runner.setProperty(service, ElasticSearchClientService.CONNECT_TIMEOUT, "10000")
        runner.setProperty(service, ElasticSearchClientService.SOCKET_TIMEOUT, "60000")
        runner.setProperty(service, ElasticSearchClientService.RETRY_TIMEOUT, "60000")
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, "Client Service")
        runner.setProperty(TestControllerServiceProcessor.LOOKUP_SERVICE, "Lookup Service")
        runner.setProperty(lookupService, ElasticSearchLookupService.CLIENT_SERVICE, "Client Service")
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "user_details")
        runner.setProperty(lookupService, ElasticSearchLookupService.TYPE, "details")

        try {
            runner.enableControllerService(service)
            runner.enableControllerService(lookupService)
        } catch (Exception ex) {
            ex.printStackTrace()
            throw ex
        }
    }

    @Test
    void testValidity() throws Exception {
        setDefaultSchema()
        runner.assertValid()
    }

    private void setDefaultSchema() throws Exception {
        runner.disableControllerService(lookupService)
        SchemaRegistry registry = new TestSchemaRegistry()
        runner.addControllerService("registry", registry)
        runner.setProperty(lookupService, SchemaAccessUtils.SCHEMA_REGISTRY, "registry")
        runner.enableControllerService(registry)
        runner.enableControllerService(lookupService)
    }

    @Test
    void lookupById() {
        def coordinates = [ _id: "2" ]
        Optional<Record> result = lookupService.lookup(coordinates)

        Assert.assertNotNull(result)
        Assert.assertTrue(result.isPresent())
        def record = result.get()
        Assert.assertEquals("jane.doe@company.com", record.getAsString("email"))
        Assert.assertEquals("098-765-4321", record.getAsString("phone"))
        Assert.assertEquals("GHIJK", record.getAsString("accessKey"))
    }

    @Test
    void testInvalidIdScenarios() {
        def coordinates = [
            [
                _id: 1
            ],
            [
                _id: "1", "email": "john.smith@company.com"
            ]
        ]

        coordinates.each { coordinate ->
            def exception

            try {
                lookupService.lookup(coordinate)
            } catch (Exception ex) {
                exception = ex
            }

            Assert.assertNotNull(exception)
            Assert.assertTrue(exception instanceof LookupFailureException)
        }
    }

    @Test
    void lookupByQuery() {
        def coordinates = [ "phone": "098-765-4321", "email": "jane.doe@company.com" ]
        Optional<Record> result = lookupService.lookup(coordinates)

        Assert.assertNotNull(result)
        Assert.assertTrue(result.isPresent())
        def record = result.get()
        Assert.assertEquals("jane.doe@company.com", record.getAsString("email"))
        Assert.assertEquals("098-765-4321", record.getAsString("phone"))
        Assert.assertEquals("GHIJK", record.getAsString("accessKey"))
    }

    @Test
    void testNestedSchema() {
        def coordinates = [
            "subField.deeper.deepest.super_secret": "The sky is blue",
            "subField.deeper.secretz": "Buongiorno, mondo!!",
            "msg": "Hello, world"
        ]

        runner.disableControllerService(lookupService)
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "nested")
        runner.setProperty(lookupService, ElasticSearchLookupService.TYPE, "nested_complex")
        runner.enableControllerService(lookupService)

        Optional<Record> response = lookupService.lookup(coordinates)
        Assert.assertNotNull(response)
        Assert.assertTrue(response.isPresent())
        def rec = response.get()
        Assert.assertEquals("Hello, world", rec.getValue("msg"))
        def subRec = getSubRecord(rec, "subField")
        Assert.assertNotNull(subRec)
        def deeper = getSubRecord(subRec, "deeper")
        Assert.assertNotNull(deeper)
        def deepest = getSubRecord(deeper, "deepest")
        Assert.assertNotNull(deepest)
        Assert.assertEquals("The sky is blue", deepest.getAsString("super_secret"))
    }

    @Test
    void testDetectedSchema() throws LookupFailureException {
        runner.disableControllerService(lookupService)
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "complex")
        runner.setProperty(lookupService, ElasticSearchLookupService.TYPE, "complex")
        runner.enableControllerService(lookupService)
        def coordinates = ["_id": "1" ]

        Optional<Record> response = lookupService.lookup(coordinates)
        Assert.assertNotNull(response)
        Record rec = response.get()
        Record subRec = getSubRecord(rec, "subField")

        def r2 = new MapRecord(rec.schema, [:])
        def path = RecordPath.compile("/subField/longField")
        def result = path.evaluate(r2)
        result.selectedFields.findFirst().get().updateValue(1234567890L)

        Assert.assertNotNull(rec)
        Assert.assertNotNull(subRec)
        Assert.assertEquals("Hello, world", rec.getValue("msg"))
        Assert.assertNotNull(rec.getValue("subField"))
        Assert.assertEquals(new Long(100000), subRec.getValue("longField"))
        Assert.assertEquals("2018-04-10T12:18:05Z", subRec.getValue("dateField"))
    }

    Record getSubRecord(Record rec, String fieldName) {
        RecordSchema schema = rec.schema
        RecordSchema subSchema = ((RecordDataType)schema.getField(fieldName).get().dataType).childSchema
        rec.getAsRecord(fieldName, subSchema)
    }

    @Test
    void testMappings() {
        runner.disableControllerService(lookupService)
        runner.setProperty(lookupService, "\$.subField.longField", "/longField2")
        runner.setProperty(lookupService, '$.subField.dateField', '/dateField2')
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "nested")
        runner.setProperty(lookupService, ElasticSearchLookupService.TYPE, "nested_complex")
        runner.enableControllerService(lookupService)

        def coordinates = ["msg": "Hello, world"]
        def result = lookupService.lookup(coordinates)
        Assert.assertTrue(result.isPresent())
        def rec = result.get()
        ["dateField": "2018-08-14T10:08:00Z", "longField": 150000L].each { field ->
            def value = rec.getValue(field.key)
            Assert.assertEquals(field.value, value)
        }
    }
}
