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
import org.apache.nifi.elasticsearch.ElasticSearchLookupService
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Before
import org.junit.Test

class ElasticSearchLookupServiceTest {
    ElasticSearchClientService mockClientService
    ElasticSearchLookupService lookupService
    TestRunner runner

    @Before
    void setup() throws Exception {
        mockClientService = new TestElasticSearchClientService()
        lookupService = new ElasticSearchLookupService()
        def registry = new TestSchemaRegistry()
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class)
        runner.addControllerService("clientService", mockClientService)
        runner.addControllerService("lookupService", lookupService)
        runner.addControllerService("registry", registry)
        runner.enableControllerService(mockClientService)
        runner.enableControllerService(registry)
        runner.setProperty(lookupService, ElasticSearchLookupService.CLIENT_SERVICE, "clientService")
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "users")
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, "clientService")
        runner.setProperty(TestControllerServiceProcessor.LOOKUP_SERVICE, "lookupService")
        runner.setProperty(lookupService, SchemaAccessUtils.SCHEMA_REGISTRY, "registry")
        runner.setProperty(lookupService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INFER_SCHEMA)
        runner.enableControllerService(lookupService)
    }

    @Test
    void simpleLookupTest() throws Exception {
        def coordinates = ["_id": "12345" ]

        Optional<MapRecord> result = lookupService.lookup(coordinates)

        Assert.assertNotNull(result)
        Assert.assertTrue(result.isPresent())
        MapRecord record = result.get()
        Assert.assertEquals("john.smith", record.getAsString("username"))
        Assert.assertEquals("testing1234", record.getAsString("password"))
        Assert.assertEquals("john.smith@test.com", record.getAsString("email"))
        Assert.assertEquals("Software Engineer", record.getAsString("position"))
    }
}
