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
package org.apache.nifi.services.protobuf.schema;

import com.google.common.collect.Sets;
import com.squareup.wire.schema.CoreLoaderKt;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.SchemaLoader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.jupiter.api.Test;

import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.services.protobuf.ProtoTestUtil.BASE_TEST_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestProtoSchemaParser {

    @Test
    public void testSchemaParserForProto3() {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Collections.singletonList(Location.get(BASE_TEST_PATH + "test_proto3.proto")), Collections.emptyList());

        final ProtoSchemaParser schemaParser = new ProtoSchemaParser(schemaLoader.loadSchema());
        final RecordSchema recordSchema = schemaParser.createSchema("Proto3Message");

        assertRecordField(recordSchema.getField("field1"), RecordFieldType.BOOLEAN.getDataType(), true);
        assertRecordField(recordSchema.getField("field2"), RecordFieldType.STRING.getDataType(), true);
        assertRecordField(recordSchema.getField("field3"), RecordFieldType.INT.getDataType(), true);
        assertRecordField(recordSchema.getField("field4"), RecordFieldType.LONG.getDataType(), true);
        assertRecordField(recordSchema.getField("field5"), RecordFieldType.LONG.getDataType(), true);
        assertRecordField(recordSchema.getField("field6"), RecordFieldType.LONG.getDataType(), true);
        assertRecordField(recordSchema.getField("field7"), RecordFieldType.INT.getDataType(), true);
        assertRecordField(recordSchema.getField("field8"), RecordFieldType.DOUBLE.getDataType(), true);
        assertRecordField(recordSchema.getField("field9"), RecordFieldType.FLOAT.getDataType(), true);
        assertRecordField(recordSchema.getField("field10"), RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()), true);
        assertRecordField(recordSchema.getField("field11"), RecordFieldType.LONG.getDataType(), true);
        assertRecordField(recordSchema.getField("field12"), RecordFieldType.BIGINT.getDataType(), true);
        assertRecordField(recordSchema.getField("field13"), RecordFieldType.LONG.getDataType(), true);
        assertRecordField(recordSchema.getField("field14"), RecordFieldType.BIGINT.getDataType(), true);
        assertRecordField(recordSchema.getField("field15"), RecordFieldType.LONG.getDataType(), true);

        final RecordSchema nestedSchema = ((RecordDataType) recordSchema.getField("nestedMessage").get().getDataType()).getChildSchema();
        assertRecordField(nestedSchema.getField("testEnum"), new EnumDataType(Arrays.asList("ENUM_VALUE_1", "ENUM_VALUE_2", "ENUM_VALUE_3")), true);
        assertRecordField(nestedSchema.getField("repeatedField"), RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()), true);

        final List<DataType> choiceDataTypes = Arrays.asList(RecordFieldType.STRING.getDataType(), RecordFieldType.BOOLEAN.getDataType(), RecordFieldType.INT.getDataType());
        final Set<String> choiceAliases = Sets.newHashSet("option1", "option2", "option3");
        assertRecordField(nestedSchema.getField("oneOfField"), RecordFieldType.CHOICE.getChoiceDataType(choiceDataTypes), true, null, choiceAliases);
        assertRecordField(nestedSchema.getField("testMap"), RecordFieldType.MAP.getMapDataType(RecordFieldType.INT.getDataType()), true);

        assertRecordField(recordSchema.getField("nestedMessage"), RecordFieldType.RECORD.getRecordDataType(nestedSchema), true);
    }

    @Test
    public void testSchemaParserForProto2() {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Arrays.asList(
                Location.get(BASE_TEST_PATH, "test_proto2.proto"),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/any.proto")), Collections.emptyList());

        final ProtoSchemaParser schemaParser = new ProtoSchemaParser(schemaLoader.loadSchema());
        final RecordSchema recordSchema = schemaParser.createSchema("Proto2Message");

        final RecordSchema anySchema = ((RecordDataType) recordSchema.getField("anyField").get().getDataType()).getChildSchema();
        assertRecordField(anySchema.getField("type_url"), RecordFieldType.STRING.getDataType(), true);
        assertRecordField(anySchema.getField("value"), RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()), true);

        assertRecordField(recordSchema.getField("field1"), RecordFieldType.BOOLEAN.getDataType(), false);
        assertRecordField(recordSchema.getField("field2"), RecordFieldType.STRING.getDataType(), true, "Missing field");
        assertRecordField(recordSchema.getField("anyField"), RecordFieldType.RECORD.getRecordDataType(anySchema), true);
        assertRecordField(recordSchema.getField("extensionField"), RecordFieldType.INT.getDataType(), true);
    }

    private void assertRecordField(Optional<RecordField> recordField, DataType dataType, boolean isNullable) {
        assertRecordField(recordField, dataType, isNullable, null, Collections.emptySet());
    }

    private void assertRecordField(Optional<RecordField> recordField, DataType dataType, boolean isNullable, Object defaultValue) {
        assertRecordField(recordField, dataType, isNullable, defaultValue, Collections.emptySet());
    }

    private void assertRecordField(Optional<RecordField> recordField, DataType dataType, boolean isNullable, Object defaultValue, Set<String> aliases) {
        final RecordField field = recordField.get();

        assertEquals(dataType, field.getDataType());
        assertEquals(isNullable, field.isNullable());
        assertEquals(defaultValue, field.getDefaultValue());
        assertEquals(aliases, field.getAliases());
    }
}
