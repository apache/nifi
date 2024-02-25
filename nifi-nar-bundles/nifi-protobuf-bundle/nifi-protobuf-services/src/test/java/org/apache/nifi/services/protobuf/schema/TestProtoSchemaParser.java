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

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadProto2TestSchema;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadProto3TestSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestProtoSchemaParser {

    @Test
    public void testSchemaParserForProto3() {
        final ProtoSchemaParser schemaParser = new ProtoSchemaParser(loadProto3TestSchema());

        final SimpleRecordSchema expected = new SimpleRecordSchema(Arrays.asList(
                new RecordField("booleanField", RecordFieldType.BOOLEAN.getDataType()),
                new RecordField("stringField", RecordFieldType.STRING.getDataType()),
                new RecordField("int32Field", RecordFieldType.INT.getDataType()),
                new RecordField("uint32Field", RecordFieldType.LONG.getDataType()),
                new RecordField("sint32Field", RecordFieldType.LONG.getDataType()),
                new RecordField("fixed32Field", RecordFieldType.LONG.getDataType()),
                new RecordField("sfixed32Field", RecordFieldType.INT.getDataType()),
                new RecordField("doubleField", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("floatField", RecordFieldType.FLOAT.getDataType()),
                new RecordField("bytesField", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())),
                new RecordField("int64Field", RecordFieldType.LONG.getDataType()),
                new RecordField("uint64Field", RecordFieldType.BIGINT.getDataType()),
                new RecordField("sint64Field", RecordFieldType.LONG.getDataType()),
                new RecordField("fixed64Field", RecordFieldType.BIGINT.getDataType()),
                new RecordField("sfixed64Field", RecordFieldType.LONG.getDataType()),
                new RecordField("nestedMessage", RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(Arrays.asList(
                        new RecordField("testEnum", RecordFieldType.ENUM.getEnumDataType(Arrays.asList("ENUM_VALUE_1", "ENUM_VALUE_2", "ENUM_VALUE_3"))),
                        new RecordField("repeatedField", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())),
                        new RecordField("testMap", RecordFieldType.MAP.getMapDataType(RecordFieldType.INT.getDataType())),
                        new RecordField("stringOption", RecordFieldType.STRING.getDataType()),
                        new RecordField("booleanOption", RecordFieldType.BOOLEAN.getDataType()),
                        new RecordField("int32Option", RecordFieldType.INT.getDataType())
                ))))
        ));

        final RecordSchema actual = schemaParser.createSchema("Proto3Message");
        assertEquals(expected, actual);
    }

    @Test
    public void testSchemaParserForProto2() {
        final ProtoSchemaParser schemaParser = new ProtoSchemaParser(loadProto2TestSchema());

        final SimpleRecordSchema expected = new SimpleRecordSchema(Arrays.asList(
                new RecordField("booleanField", RecordFieldType.BOOLEAN.getDataType(), false),
                new RecordField("stringField", RecordFieldType.STRING.getDataType(), "Missing field", true),
                new RecordField("anyField", RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(Arrays.asList(
                        new RecordField("type_url", RecordFieldType.STRING.getDataType()),
                        new RecordField("value", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()))
                )))),
                new RecordField("extensionField", RecordFieldType.INT.getDataType())
        ));

        final RecordSchema actual = schemaParser.createSchema("Proto2Message");
        assertEquals(expected, actual);
    }
}
