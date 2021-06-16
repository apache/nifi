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

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.schema.access.SchemaField
import org.apache.nifi.schemaregistry.services.SchemaRegistry
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.serialization.record.SchemaIdentifier

class TestSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
    @Override
    RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) {
        new SimpleRecordSchema([
            new RecordField("msg", RecordFieldType.STRING.dataType)
        ])
    }

    @Override
    Set<SchemaField> getSuppliedSchemaFields() {
        [ SchemaField.SCHEMA_NAME ]
    }
}
