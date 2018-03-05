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
package org.apache.nifi.schema.access;

import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class AbstractSchemaAccessStrategyTest {

    protected SchemaRegistry schemaRegistry;
    protected RecordSchema recordSchema;

    @Before
    public void setup() {
        this.schemaRegistry = Mockito.mock(SchemaRegistry.class);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("firstName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));

        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder()
                .name("person").branch("master").version(1).id(1L).build();

        this.recordSchema = new SimpleRecordSchema(fields, schemaIdentifier);
    }


}
