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

package org.apache.nifi.serialization

import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.serialization.record.type.RecordDataType
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Test

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY

class TestJsonInferenceSchemaRegistryService {
    @Test
    void testInfer() {
        def runner = TestRunners.newTestRunner(FakeProcessor.class)
        def service = new JsonInferenceSchemaRegistryService()
        runner.addControllerService("schemaService", service)
        runner.setProperty(FakeProcessor.CLIENT_SERVICE, "schemaService")
        runner.setProperty(service, service.getPropertyDescriptor(SCHEMA_ACCESS_STRATEGY.getName()), SchemaAccessUtils.INFER_SCHEMA)
        runner.enableControllerService(service)
        runner.assertValid()

        def json = [
            name: "John Smith",
            age: 35,
            contact: [
                email: "john.smith@example.com",
                phone: "123-456-7890"
            ]
        ]

        def schema = service.getSchema([:], json, null)

        Assert.assertNotNull(schema)
        def name = schema.getField("name")
        def age  = schema.getField("age")
        def contact = schema.getField("contact")
        Assert.assertTrue(name.isPresent())
        Assert.assertTrue(age.isPresent())
        Assert.assertTrue(contact.isPresent())
        Assert.assertTrue(contact.get().dataType instanceof RecordDataType)
    }
}
