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
package org.apache.nifi.common.zendesk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.common.zendesk.util.ZendeskRecordPathUtils.addField;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ZendeskRecordPathUtilsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testFieldValueEvaluation() {
        ObjectNode testNode;
        Record record = initRecord();

        testNode = mapper.createObjectNode();
        addField("/a/b", "@{/field1}", testNode, record);
        Assertions.assertEquals("{\"a\":{\"b\":\"value1\"}}", testNode.toString());

        testNode = mapper.createObjectNode();
        addField("/a/b/c", "constant", testNode, record);
        Assertions.assertEquals("{\"a\":{\"b\":{\"c\":\"constant\"}}}", testNode.toString());

        testNode = mapper.createObjectNode();
        addField("/a/0", "array_element", testNode, record);
        Assertions.assertEquals("{\"a\":[\"array_element\"]}", testNode.toString());

        ProcessException e1 = assertThrows(ProcessException.class, () -> addField("/a", "@{/field2}", mapper.createObjectNode(), record));
        Assertions.assertEquals("The provided RecordPath [/field2] points to a [ARRAY] type value", e1.getMessage());

        ProcessException e2 = assertThrows(ProcessException.class, () -> addField("/a", "@{/field3}", mapper.createObjectNode(), record));
        Assertions.assertEquals("The provided RecordPath [/field3] points to a [RECORD] type value", e2.getMessage());

        ProcessException e3 = assertThrows(ProcessException.class, () -> addField("/a", "@{/field4}", mapper.createObjectNode(), record));
        Assertions.assertEquals("The provided RecordPath [/field4] points to a [CHOICE] type value with Record subtype", e3.getMessage());
    }

    private Record initRecord() {
        List<RecordField> recordFields = new ArrayList<>();
        recordFields.add(new RecordField("nestedField1", RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField("nestedField2", RecordFieldType.STRING.getDataType()));

        RecordSchema nestedRecordSchema = new SimpleRecordSchema(recordFields);

        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("field1", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("field2", new ArrayDataType(RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField("field3", new RecordDataType(nestedRecordSchema)));
        fields.add(new RecordField("field4", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.STRING.getDataType(), RecordFieldType.INT.getDataType(), RecordFieldType.RECORD.getDataType())));
        RecordSchema schema = new SimpleRecordSchema(fields);

        List<String> valueList = new ArrayList<>();
        valueList.add("listElement");

        Map<String, Object> nestedValueMap = new HashMap<>();
        nestedValueMap.put("nestedField1", "nestedValue1");
        nestedValueMap.put("nestedField2", "nestedValue2");

        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("field1", "value1");
        valueMap.put("field2", valueList);
        valueMap.put("field3", nestedValueMap);
        valueMap.put("field4", "field4");

        return new MapRecord(schema, valueMap);
    }
}
