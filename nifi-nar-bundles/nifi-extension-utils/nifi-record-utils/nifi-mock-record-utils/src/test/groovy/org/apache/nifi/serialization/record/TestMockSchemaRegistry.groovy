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

package org.apache.nifi.serialization.record

import org.apache.avro.Schema
import org.apache.nifi.avro.AvroTypeUtil
import org.junit.Assert
import org.junit.Test
import static groovy.json.JsonOutput.*

class TestMockSchemaRegistry {
    @Test
    void testGetSchemaByName() {
        def registry = new MockSchemaRegistry()
        def schema = prettyPrint(toJson([
            name: "TestSchema",
            type: "record",
            fields: [
                [ name: "msg", type: "string" ]
            ]
        ]))
        def recordSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schema))
        registry.addSchema("simple", recordSchema)

        def identifier = SchemaIdentifier.builder().name("simple").build()
        def result = registry.retrieveSchemaByName(identifier)

        Assert.assertNotNull("Failed to load schema.", result)
        Assert.assertEquals(result.fieldNames, recordSchema.fieldNames)
    }
}
