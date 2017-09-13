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

package org.apache.nifi.record.path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.nifi.record.path.exception.RecordPathException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Test;

public class TestRecordPath {

    @Test
    public void testCompile() {
        System.out.println(RecordPath.compile("/person/name/last"));
        System.out.println(RecordPath.compile("/person[2]"));
        System.out.println(RecordPath.compile("//person[2]"));
        System.out.println(RecordPath.compile("/person/child[1]//sibling/name"));

        // contains is a 'filter function' so can be used as the predicate
        RecordPath.compile("/name[contains(., 'hello')]");

        // substring is not a filter function so cannot be used as a predicate
        try {
            RecordPath.compile("/name[substring(., 1, 2)]");
        } catch (final RecordPathException e) {
            // expected
        }

        // substring is not a filter function so can be used as *part* of a predicate but not as the entire predicate
        RecordPath.compile("/name[substring(., 1, 2) = 'e']");
    }

    @Test
    public void testChildField() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        final Record record = new MapRecord(schema, values);

        assertEquals(48, RecordPath.compile("/id").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(record, RecordPath.compile("/id").evaluate(record).getSelectedFields().findFirst().get().getParentRecord().get());

        assertEquals("John Doe", RecordPath.compile("/name").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(record, RecordPath.compile("/name").evaluate(record).getSelectedFields().findFirst().get().getParentRecord().get());

        assertEquals(accountRecord, RecordPath.compile("/mainAccount").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(record, RecordPath.compile("/mainAccount").evaluate(record).getSelectedFields().findFirst().get().getParentRecord().get());

        assertEquals(1, RecordPath.compile("/mainAccount/id").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(accountRecord, RecordPath.compile("/mainAccount/id").evaluate(record).getSelectedFields().findFirst().get().getParentRecord().get());

        assertEquals(123.45D, RecordPath.compile("/mainAccount/balance").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(accountRecord, RecordPath.compile("/mainAccount/id").evaluate(record).getSelectedFields().findFirst().get().getParentRecord().get());
    }

    @Test
    public void testRootRecord() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = RecordPath.compile("/").evaluate(record).getSelectedFields().findFirst().get();
        assertEquals(Optional.empty(), fieldValue.getParent());
        assertEquals(record, fieldValue.getValue());
    }

    @Test
    public void testWildcardChild() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/mainAccount/*").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(2, fieldValues.size());

        for (final FieldValue fieldValue : fieldValues) {
            assertEquals(accountRecord, fieldValue.getParentRecord().get());
        }

        assertEquals("id", fieldValues.get(0).getField().getFieldName());
        assertEquals(1, fieldValues.get(0).getValue());

        assertEquals("balance", fieldValues.get(1).getField().getFieldName());
        assertEquals(123.45D, fieldValues.get(1).getValue());
    }

    @Test
    public void testWildcardWithArray() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("accounts", new Object[] {accountRecord});
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/*[0]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertTrue(fieldValue.getField().getFieldName().startsWith("accounts["));
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals(accountRecord, fieldValue.getValue());
    }

    @Test
    public void testDescendantField() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("//id").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(2, fieldValues.size());

        final FieldValue first = fieldValues.get(0);
        final FieldValue second = fieldValues.get(1);

        assertEquals(RecordFieldType.INT, first.getField().getDataType().getFieldType());
        assertEquals(RecordFieldType.INT, second.getField().getDataType().getFieldType());

        assertEquals(48, first.getValue());
        assertEquals(1, second.getValue());
    }

    @Test
    public void testParent() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("//id/..").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(2, fieldValues.size());

        final FieldValue first = fieldValues.get(0);
        final FieldValue second = fieldValues.get(1);

        assertEquals(RecordFieldType.RECORD, first.getField().getDataType().getFieldType());
        assertEquals(RecordFieldType.RECORD, second.getField().getDataType().getFieldType());

        assertEquals(record, first.getValue());
        assertEquals(accountRecord, second.getValue());
    }

    @Test
    public void testMapKey() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("city", "New York");
        attributes.put("state", "NY");

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("attributes", attributes);
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = RecordPath.compile("/attributes['city']").evaluate(record).getSelectedFields().findFirst().get();
        assertTrue(fieldValue.getField().getFieldName().startsWith("attributes['"));
        assertEquals("New York", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testMapWildcard() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("city", "New York");
        attributes.put("state", "NY");

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("attributes", attributes);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/attributes[*]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(2, fieldValues.size());

        assertEquals("New York", fieldValues.get(0).getValue());
        assertEquals("NY", fieldValues.get(1).getValue());

        for (final FieldValue fieldValue : fieldValues) {
            assertTrue(fieldValue.getField().getFieldName().startsWith("attributes['"));
            assertEquals(record, fieldValue.getParentRecord().get());
        }
    }

    @Test
    public void testMapMultiKey() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("city", "New York");
        attributes.put("state", "NY");

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("attributes", attributes);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/attributes['city', 'state']").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(2, fieldValues.size());

        assertEquals("New York", fieldValues.get(0).getValue());
        assertEquals("NY", fieldValues.get(1).getValue());

        for (final FieldValue fieldValue : fieldValues) {
            assertTrue(fieldValue.getField().getFieldName().startsWith("attributes['"));
            assertEquals(record, fieldValue.getParentRecord().get());
        }
    }

    @Test
    public void testEscapedFieldName() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name,date", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name,date", "John Doe");
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = RecordPath.compile("/'name,date'").evaluate(record).getSelectedFields().findFirst().get();
        assertEquals("name,date", fieldValue.getField().getFieldName());
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testSingleArrayIndex() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = RecordPath.compile("/numbers[3]").evaluate(record).getSelectedFields().findFirst().get();
        assertTrue(fieldValue.getField().getFieldName().startsWith("numbers["));
        assertEquals(3, fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testSingleArrayRange() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/numbers[0..1]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        for (final FieldValue fieldValue : fieldValues) {
            assertTrue(fieldValue.getField().getFieldName().startsWith("numbers["));
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        assertEquals(2, fieldValues.size());
        for (int i = 0; i < 1; i++) {
            assertEquals(i, fieldValues.get(0).getValue());
        }
    }


    @Test
    public void testMultiArrayIndex() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/numbers[3,6, -1, -2]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        int i = 0;
        final int[] expectedValues = new int[] {3, 6, 9, 8};
        for (final FieldValue fieldValue : fieldValues) {
            assertTrue(fieldValue.getField().getFieldName().startsWith("numbers["));
            assertEquals(expectedValues[i++], fieldValue.getValue());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

    }

    @Test
    public void testMultiArrayIndexWithRanges() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        List<FieldValue> fieldValues = RecordPath.compile("/numbers[0, 2, 4..7, 9]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        for (final FieldValue fieldValue : fieldValues) {
            assertTrue(fieldValue.getField().getFieldName().startsWith("numbers["));
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        int[] expectedValues = new int[] {0, 2, 4, 5, 6, 7, 9};
        assertEquals(expectedValues.length, fieldValues.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], fieldValues.get(i).getValue());
        }

        fieldValues = RecordPath.compile("/numbers[0..-1]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        for (final FieldValue fieldValue : fieldValues) {
            assertTrue(fieldValue.getField().getFieldName().startsWith("numbers["));
            assertEquals(record, fieldValue.getParentRecord().get());
        }
        expectedValues = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        assertEquals(expectedValues.length, fieldValues.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], fieldValues.get(i).getValue());
        }


        fieldValues = RecordPath.compile("/numbers[-1..-1]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        for (final FieldValue fieldValue : fieldValues) {
            assertTrue(fieldValue.getField().getFieldName().startsWith("numbers["));
            assertEquals(record, fieldValue.getParentRecord().get());
        }
        expectedValues = new int[] {9};
        assertEquals(expectedValues.length, fieldValues.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], fieldValues.get(i).getValue());
        }

        fieldValues = RecordPath.compile("/numbers[*]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        for (final FieldValue fieldValue : fieldValues) {
            assertTrue(fieldValue.getField().getFieldName().startsWith("numbers["));
            assertEquals(record, fieldValue.getParentRecord().get());
        }
        expectedValues = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        assertEquals(expectedValues.length, fieldValues.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], fieldValues.get(i).getValue());
        }

        fieldValues = RecordPath.compile("/xx[1,2,3]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(0, fieldValues.size());
    }

    @Test
    public void testEqualsPredicate() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        values.put("numbers", new Object[] {1, 2, 3, 4, 4, 4, 5});
        final Record record = new MapRecord(schema, values);


        List<FieldValue> fieldValues = RecordPath.compile("/numbers[0..-1][. = 4]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(3, fieldValues.size());

        for (final FieldValue fieldValue : fieldValues) {
            final String fieldName = fieldValue.getField().getFieldName();
            assertTrue(Pattern.compile("numbers\\[\\d\\]").matcher(fieldName).matches());
            assertEquals(RecordFieldType.INT, fieldValue.getField().getDataType().getFieldType());
            assertEquals(4, fieldValue.getValue());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        fieldValues = RecordPath.compile("//id[. = 48]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());
        final FieldValue fieldValue = fieldValues.get(0);

        assertEquals("id", fieldValue.getField().getFieldName());
        assertEquals(RecordFieldType.INT.getDataType(), fieldValue.getField().getDataType());
        assertEquals(48, fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testRelativePath() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/mainAccount/././balance/.").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals(accountRecord, fieldValue.getParentRecord().get());
        assertEquals(123.45D, fieldValue.getValue());
        assertEquals("balance", fieldValue.getField().getFieldName());
    }

    @Test
    public void testCompareToLiteral() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("numbers", new Object[] {0, 1, 2});
        final Record record = new MapRecord(schema, values);

        List<FieldValue> fieldValues = RecordPath.compile("/id[. > 42]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        fieldValues = RecordPath.compile("/id[. < 42]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(0, fieldValues.size());
    }

    @Test
    public void testCompareToAbsolute() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("numbers", new Object[] {0, 1, 2});
        final Record record = new MapRecord(schema, values);

        List<FieldValue> fieldValues = RecordPath.compile("/numbers[0..-1][. < /id]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(3, fieldValues.size());

        fieldValues = RecordPath.compile("/id[. > /numbers[-1]]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());
    }

    @Test
    public void testCompareWithEmbeddedPaths() {
        final Map<String, Object> accountValues1 = new HashMap<>();
        accountValues1.put("id", 1);
        accountValues1.put("balance", 10_000.00D);
        final Record accountRecord1 = new MapRecord(getAccountSchema(), accountValues1);

        final Map<String, Object> accountValues2 = new HashMap<>();
        accountValues2.put("id", 2);
        accountValues2.put("balance", 48.02D);
        final Record accountRecord2 = new MapRecord(getAccountSchema(), accountValues2);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("accounts", new Object[] {accountRecord1, accountRecord2});
        final Record record = new MapRecord(schema, values);

        final RecordPath recordPath = RecordPath.compile("/accounts[0..-1][./balance > 100]");
        List<FieldValue> fieldValues = recordPath.evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("accounts[0]", fieldValue.getField().getFieldName());
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals(accountRecord1, fieldValue.getValue());
    }

    @Test
    public void testPredicateInMiddleOfPath() {
        final Map<String, Object> accountValues1 = new HashMap<>();
        accountValues1.put("id", 1);
        accountValues1.put("balance", 10_000.00D);
        final Record accountRecord1 = new MapRecord(getAccountSchema(), accountValues1);

        final Map<String, Object> accountValues2 = new HashMap<>();
        accountValues2.put("id", 2);
        accountValues2.put("balance", 48.02D);
        final Record accountRecord2 = new MapRecord(getAccountSchema(), accountValues2);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("accounts", new Object[] {accountRecord1, accountRecord2});
        final Record record = new MapRecord(schema, values);

        final RecordPath recordPath = RecordPath.compile("/accounts[0..-1][./balance > 100]/id");
        List<FieldValue> fieldValues = recordPath.evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("id", fieldValue.getField().getFieldName());
        assertEquals(accountRecord1, fieldValue.getParentRecord().get());
        assertEquals(1, fieldValue.getValue());
    }

    @Test
    public void testUpdateValueOnMatchingFields() {
        final Map<String, Object> accountValues1 = new HashMap<>();
        accountValues1.put("id", 1);
        accountValues1.put("balance", 10_000.00D);
        final Record accountRecord1 = new MapRecord(getAccountSchema(), accountValues1);

        final Map<String, Object> accountValues2 = new HashMap<>();
        accountValues2.put("id", 2);
        accountValues2.put("balance", 48.02D);
        final Record accountRecord2 = new MapRecord(getAccountSchema(), accountValues2);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("accounts", new Object[] {accountRecord1, accountRecord2});
        final Record record = new MapRecord(schema, values);

        final RecordPath recordPath = RecordPath.compile("/accounts[0..-1][./balance > 100]/id");
        recordPath.evaluate(record).getSelectedFields().findFirst().get().updateValue(100);

        assertEquals(48, record.getValue("id"));
        assertEquals(100, accountRecord1.getValue("id"));
        assertEquals(2, accountRecord2.getValue("id"));
    }

    @Test
    public void testPredicateDoesNotIncludeFieldsThatDontHaveRelativePath() {
        final List<RecordField> addressFields = new ArrayList<>();
        addressFields.add(new RecordField("city", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("state", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("zip", RecordFieldType.STRING.getDataType()));
        final RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

        final List<RecordField> detailsFields = new ArrayList<>();
        detailsFields.add(new RecordField("position", RecordFieldType.STRING.getDataType()));
        detailsFields.add(new RecordField("managerName", RecordFieldType.STRING.getDataType()));
        final RecordSchema detailsSchema = new SimpleRecordSchema(detailsFields);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("address", RecordFieldType.RECORD.getRecordDataType(addressSchema)));
        fields.add(new RecordField("details", RecordFieldType.RECORD.getRecordDataType(detailsSchema)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Record record = new MapRecord(recordSchema, new HashMap<>());
        record.setValue("name", "John Doe");

        final Record addressRecord = new MapRecord(addressSchema, new HashMap<>());
        addressRecord.setValue("city", "San Francisco");
        addressRecord.setValue("state", "CA");
        addressRecord.setValue("zip", "12345");
        record.setValue("address", addressRecord);

        final Record detailsRecord = new MapRecord(detailsSchema, new HashMap<>());
        detailsRecord.setValue("position", "Developer");
        detailsRecord.setValue("managerName", "Jane Doe");
        record.setValue("details", detailsRecord);

        final RecordPath recordPath = RecordPath.compile("/*[./state != 'NY']");
        final RecordPathResult result = recordPath.evaluate(record);
        final List<FieldValue> fieldValues = result.getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("address", fieldValue.getField().getFieldName());

        assertEquals("12345", RecordPath.compile("/*[./state != 'NY']/zip").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testPredicateWithAbsolutePath() {
        final List<RecordField> addressFields = new ArrayList<>();
        addressFields.add(new RecordField("city", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("state", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("zip", RecordFieldType.STRING.getDataType()));
        final RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

        final List<RecordField> detailsFields = new ArrayList<>();
        detailsFields.add(new RecordField("position", RecordFieldType.STRING.getDataType()));
        detailsFields.add(new RecordField("preferredState", RecordFieldType.STRING.getDataType()));
        final RecordSchema detailsSchema = new SimpleRecordSchema(detailsFields);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("address1", RecordFieldType.RECORD.getRecordDataType(addressSchema)));
        fields.add(new RecordField("address2", RecordFieldType.RECORD.getRecordDataType(addressSchema)));
        fields.add(new RecordField("details", RecordFieldType.RECORD.getRecordDataType(detailsSchema)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Record record = new MapRecord(recordSchema, new HashMap<>());
        record.setValue("name", "John Doe");

        final Record address1Record = new MapRecord(addressSchema, new HashMap<>());
        address1Record.setValue("city", "San Francisco");
        address1Record.setValue("state", "CA");
        address1Record.setValue("zip", "12345");
        record.setValue("address1", address1Record);

        final Record address2Record = new MapRecord(addressSchema, new HashMap<>());
        address2Record.setValue("city", "New York");
        address2Record.setValue("state", "NY");
        address2Record.setValue("zip", "01234");
        record.setValue("address2", address2Record);

        final Record detailsRecord = new MapRecord(detailsSchema, new HashMap<>());
        detailsRecord.setValue("position", "Developer");
        detailsRecord.setValue("preferredState", "NY");
        record.setValue("details", detailsRecord);

        final RecordPath recordPath = RecordPath.compile("/*[./state = /details/preferredState]");
        final RecordPathResult result = recordPath.evaluate(record);
        final List<FieldValue> fieldValues = result.getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("address2", fieldValue.getField().getFieldName());
    }

    @Test
    public void testRelativePathOnly() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        final FieldValue recordFieldValue = new StandardFieldValue(record, new RecordField("record", RecordFieldType.RECORD.getDataType()), null);

        final List<FieldValue> fieldValues = RecordPath.compile("./name").evaluate(record, recordFieldValue).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals("name", fieldValue.getField().getFieldName());
    }

    @Test
    public void testRelativePathAgainstNonRecordField() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        final FieldValue recordFieldValue = new StandardFieldValue(record, new RecordField("root", RecordFieldType.RECORD.getRecordDataType(record.getSchema())), null);
        final FieldValue nameFieldValue = new StandardFieldValue("John Doe", new RecordField("name", RecordFieldType.STRING.getDataType()), recordFieldValue);

        final List<FieldValue> fieldValues = RecordPath.compile(".").evaluate(record, nameFieldValue).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals("name", fieldValue.getField().getFieldName());

        fieldValue.updateValue("Jane Doe");
        assertEquals("Jane Doe", record.getValue("name"));
    }

    @Test
    public void testSubstringFunction() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = RecordPath.compile("substring(/name, 0, 4)").evaluate(record).getSelectedFields().findFirst().get();
        assertEquals("John", fieldValue.getValue());

        assertEquals("John", RecordPath.compile("substring(/name, 0, -5)").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("", RecordPath.compile("substring(/name, 1000, 1005)").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("", RecordPath.compile("substring(/name, 4, 3)").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("substring(/name, 0, 10000)").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("", RecordPath.compile("substring(/name, -50, -1)").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testSubstringBeforeFunction() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John", RecordPath.compile("substringBefore(/name, ' ')").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("substringBefore(/name, 'XYZ')").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("substringBefore(/name, '')").evaluate(record).getSelectedFields().findFirst().get().getValue());

        assertEquals("John D", RecordPath.compile("substringBeforeLast(/name, 'o')").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("substringBeforeLast(/name, 'XYZ')").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("substringBeforeLast(/name, '')").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testSubstringAfterFunction() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("hn Doe", RecordPath.compile("substringAfter(/name, 'o')").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("substringAfter(/name, 'XYZ')").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("substringAfter(/name, '')").evaluate(record).getSelectedFields().findFirst().get().getValue());

        assertEquals("e", RecordPath.compile("substringAfterLast(/name, 'o')").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("substringAfterLast(/name, 'XYZ')").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("substringAfterLast(/name, '')").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testContains() {
        final Record record = createSimpleRecord();
        assertEquals("John Doe", RecordPath.compile("/name[contains(., 'o')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(0L, RecordPath.compile("/name[contains(., 'x')]").evaluate(record).getSelectedFields().count());

        record.setValue("name", "John Doe 48");
        assertEquals("John Doe 48", RecordPath.compile("/name[contains(., /id)]").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testStartsWith() {
        final Record record = createSimpleRecord();
        assertEquals("John Doe", RecordPath.compile("/name[startsWith(., 'J')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(0L, RecordPath.compile("/name[startsWith(., 'x')]").evaluate(record).getSelectedFields().count());
        assertEquals("John Doe", RecordPath.compile("/name[startsWith(., '')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testEndsWith() {
        final Record record = createSimpleRecord();
        assertEquals("John Doe", RecordPath.compile("/name[endsWith(., 'e')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(0L, RecordPath.compile("/name[endsWith(., 'x')]").evaluate(record).getSelectedFields().count());
        assertEquals("John Doe", RecordPath.compile("/name[endsWith(., '')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testIsEmpty() {
        final Record record = createSimpleRecord();
        assertEquals("John Doe", RecordPath.compile("/name[isEmpty(../missing)]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("/name[isEmpty(/missing)]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(0L, RecordPath.compile("/name[isEmpty(../id)]").evaluate(record).getSelectedFields().count());

        record.setValue("missing", "   ");
        assertEquals(0L, RecordPath.compile("/name[isEmpty(/missing)]").evaluate(record).getSelectedFields().count());
    }


    @Test
    public void testIsBlank() {
        final Record record = createSimpleRecord();
        assertEquals("John Doe", RecordPath.compile("/name[isBlank(../missing)]").evaluate(record).getSelectedFields().findFirst().get().getValue());

        record.setValue("missing", "   ");
        assertEquals("John Doe", RecordPath.compile("/name[isBlank(../missing)]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("/name[isBlank(/missing)]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(0L, RecordPath.compile("/name[isBlank(../id)]").evaluate(record).getSelectedFields().count());
    }

    @Test
    public void testContainsRegex() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe", RecordPath.compile("/name[containsRegex(., 'o')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("/name[containsRegex(., '[xo]')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(0L, RecordPath.compile("/name[containsRegex(., 'x')]").evaluate(record).getSelectedFields().count());
    }

    @Test
    public void testNot() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe", RecordPath.compile("/name[not(contains(., 'x'))]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(0L, RecordPath.compile("/name[not(. = 'John Doe')]").evaluate(record).getSelectedFields().count());
        assertEquals("John Doe", RecordPath.compile("/name[not(. = 'Jane Doe')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testChainingFunctions() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe", RecordPath.compile("/name[contains(substringAfter(., 'o'), 'h')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }



    @Test
    public void testMatchesRegex() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals(0L, RecordPath.compile("/name[matchesRegex(., 'John D')]").evaluate(record).getSelectedFields().count());
        assertEquals("John Doe", RecordPath.compile("/name[matchesRegex(., '[John Doe]{8}')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testReplace() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe", RecordPath.compile("/name[replace(../id, 48, 18) = 18]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(0L, RecordPath.compile("/name[replace(../id, 48, 18) = 48]").evaluate(record).getSelectedFields().count());

        assertEquals("Jane Doe", RecordPath.compile("replace(/name, 'ohn', 'ane')").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("replace(/name, 'ohnny', 'ane')").evaluate(record).getSelectedFields().findFirst().get().getValue());

        assertEquals("John 48", RecordPath.compile("replace(/name, 'Doe', /id)").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("23", RecordPath.compile("replace(/id, 48, 23)").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testReplaceRegex() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("ohn oe", RecordPath.compile("replaceRegex(/name, '[JD]', '')").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("replaceRegex(/name, 'ohnny', 'ane')").evaluate(record).getSelectedFields().findFirst().get().getValue());

        assertEquals("11", RecordPath.compile("replaceRegex(/id, '[0-9]', 1)").evaluate(record).getSelectedFields().findFirst().get().getValue());

        assertEquals("Jxohn Dxoe", RecordPath.compile("replaceRegex(/name, '([JD])', '$1x')").evaluate(record).getSelectedFields().findFirst().get().getValue());

        assertEquals("Jxohn Dxoe", RecordPath.compile("replaceRegex(/name, '(?<hello>[JD])', '${hello}x')").evaluate(record).getSelectedFields().findFirst().get().getValue());

        assertEquals("48ohn 48oe", RecordPath.compile("replaceRegex(/name, '(?<hello>[JD])', /id)").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testReplaceNull() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("missing", RecordFieldType.LONG.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals(48, RecordPath.compile("replaceNull(/missing, /id)").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(14, RecordPath.compile("replaceNull(/missing, 14)").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(48, RecordPath.compile("replaceNull(/id, 14)").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testConcat() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("fullName", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("firstName", RecordFieldType.LONG.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("lastName", "Doe");
        values.put("firstName", "John");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe: 48", RecordPath.compile("concat(/firstName, ' ', /lastName, ': ', 48)").evaluate(record).getSelectedFields().findFirst().get().getValue());
    }

    @Test
    public void testFieldName() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("name", RecordPath.compile("fieldName(/name)").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("name", RecordPath.compile("fieldName(/*)").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("//*[startsWith(fieldName(.), 'na')]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("name", RecordPath.compile("fieldName(//*[startsWith(fieldName(.), 'na')])").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals("John Doe", RecordPath.compile("//name[not(startsWith(fieldName(.), 'xyz'))]").evaluate(record).getSelectedFields().findFirst().get().getValue());
        assertEquals(0L, RecordPath.compile("//name[not(startsWith(fieldName(.), 'n'))]").evaluate(record).getSelectedFields().count());
    }

    private List<RecordField> getDefaultFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("attributes", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField("mainAccount", RecordFieldType.RECORD.getRecordDataType(getAccountSchema())));
        fields.add(new RecordField("numbers", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));

        final DataType accountDataType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountDataType);
        final RecordField accountsField = new RecordField("accounts", accountsType);
        fields.add(accountsField);
        return fields;
    }

    private RecordSchema getAccountSchema() {
        final List<RecordField> accountFields = new ArrayList<>();
        accountFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        accountFields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));

        final RecordSchema accountSchema = new SimpleRecordSchema(accountFields);
        return accountSchema;
    }

    private Record createSimpleRecord() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("missing", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);
        return record;
    }

}
