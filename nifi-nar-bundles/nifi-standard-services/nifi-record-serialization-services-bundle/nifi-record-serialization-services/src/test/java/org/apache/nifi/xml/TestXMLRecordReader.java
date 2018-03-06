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

package org.apache.nifi.xml;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestXMLRecordReader {
    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();

    @Test
    public void testSimpleRecord() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, reader.nextRecord().getValues());
    }

    @Test
    public void testSimpleRecord2() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema2(), "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        assertNull(reader.nextRecord(true, true).getValue("AGE"));
        assertNull(reader.nextRecord(false, true).getValue("AGE"));
        assertNotNull(reader.nextRecord(true, false).getValue("AGE"));
        assertNotNull(reader.nextRecord(false, false).getValue("AGE"));
    }

    @Test
    public void testSimpleRecord3() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        assertEquals(Integer.class, reader.nextRecord(true, true).getValue("AGE").getClass());
        assertEquals(String.class, reader.nextRecord(false, true).getValue("AGE").getClass());
    }

    @Test
    public void testSimpleRecord4() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        List<RecordField> fields = getSimpleRecordFields();
        fields.remove(2);
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(fields), "PEOPLE", "PERSON",
                null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        assertEquals(Integer.class, reader.nextRecord(true, false).getValue("AGE").getClass());
        assertEquals(String.class, reader.nextRecord(false, false).getValue("AGE").getClass());
    }

    @Test
    public void testSimpleRecordIgnoreSchema() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_no_attributes.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Assert.assertArrayEquals(new Object[] {"Cleve Butler", "42", "USA"}, reader.nextRecord(false, false).getValues());
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", "33", "UK"}, reader.nextRecord(false, false).getValues());
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", "74", "FR"}, reader.nextRecord(false, false).getValues());
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", "16", "USA"}, reader.nextRecord(false, false).getValues());
    }

    @Test
    public void testSimpleRecordWithAttribute() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField("ID", RecordFieldType.STRING.getDataType()));
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(fields), "PEOPLE", "PERSON",
                null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record first = reader.nextRecord();
        assertTrue(Arrays.asList(first.getValues()).containsAll(Arrays.asList("Cleve Butler", 42, "USA", "P1")));
        assertEquals("P1", first.getAsString("ID"));

        Record second = reader.nextRecord();
        assertTrue(Arrays.asList(second.getValues()).containsAll(Arrays.asList("Ainslie Fletcher", 33, "UK", "P2")));
        assertEquals("P2", second.getAsString("ID"));

        Record third = reader.nextRecord();
        assertTrue(Arrays.asList(third.getValues()).containsAll(Arrays.asList("Amélie Bonfils", 74, "FR", "P3")));
        assertEquals("P3", third.getAsString("ID"));

        Record fourth = reader.nextRecord();
        assertTrue(Arrays.asList(fourth.getValues()).containsAll(Arrays.asList("Elenora Scrivens", 16, "USA", "P4")));
        assertEquals("P4", fourth.getAsString("ID"));
    }

    @Test
    public void testSimpleRecordWithAttribute2() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField("ID", RecordFieldType.STRING.getDataType()));
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(fields), "PEOPLE", "PERSON",
                "ATTR_", "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record first = reader.nextRecord();
        assertTrue(Arrays.asList(first.getValues()).containsAll(Arrays.asList("Cleve Butler", 42, "USA")));
        assertEquals("P1", first.getAsString("ATTR_ID"));

        Record second = reader.nextRecord();
        assertTrue(Arrays.asList(second.getValues()).containsAll(Arrays.asList("Ainslie Fletcher", 33, "UK")));
        assertEquals("P2", second.getAsString("ATTR_ID"));

        Record third = reader.nextRecord();
        assertTrue(Arrays.asList(third.getValues()).containsAll(Arrays.asList("Amélie Bonfils", 74, "FR")));
        assertEquals("P3", third.getAsString("ATTR_ID"));

        Record fourth = reader.nextRecord();
        assertTrue(Arrays.asList(fourth.getValues()).containsAll(Arrays.asList("Elenora Scrivens", 16, "USA")));
        assertEquals("P4", fourth.getAsString("ATTR_ID"));
    }

    @Test
    public void testSimpleRecordWithAttribute3() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(Collections.emptyList()),
                "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record first = reader.nextRecord(true, true);
        assertEquals(null, first.getAsString("ID"));

        Record second = reader.nextRecord(false, false);
        assertEquals("P2", second.getAsString("ID"));

        Record third = reader.nextRecord(true, false);
        assertEquals("P3", third.getAsString("ID"));

        Record fourth = reader.nextRecord(false, true);
        assertEquals(null, fourth.getAsString("ID"));
    }

    @Test
    public void testSimpleRecordWithAttribute4() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people2.xml");
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField("ID", RecordFieldType.INT.getDataType()));

        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(fields), "PEOPLE", "PERSON",
                null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        assertEquals(Integer.class, reader.nextRecord(true, true).getValue("ID").getClass());
        assertEquals(String.class, reader.nextRecord(false, true).getValue("ID").getClass());
    }

    @Test
    public void testSimpleRecordWithAttribute5() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people2.xml");
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField("ID", RecordFieldType.INT.getDataType()));

        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(fields), "PEOPLE", "PERSON",
                null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        assertEquals(Integer.class, reader.nextRecord(true, false).getValue("ID").getClass());
        assertEquals(String.class, reader.nextRecord(false, false).getValue("ID").getClass());
    }

    @Test
    public void testSimpleRecordWithAttributeIgnoreSchema() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField("ID", RecordFieldType.STRING.getDataType()));
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(fields), "PEOPLE", "PERSON",
                null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record first = reader.nextRecord(false, false);
        assertTrue(Arrays.asList(new Object[] {"Cleve Butler", "42", "USA", "P1"}).containsAll(Arrays.asList(first.getValues())));
        assertEquals("P1", first.getAsString("ID"));

        Record second = reader.nextRecord(false, false);
        assertTrue(Arrays.asList(new Object[] {"Ainslie Fletcher", "33", "UK", "P2"}).containsAll(Arrays.asList(second.getValues())));
        assertEquals("P2", second.getAsString("ID"));

        Record third = reader.nextRecord(false, false);
        assertTrue(Arrays.asList(new Object[] {"Amélie Bonfils", "74", "FR", "P3"}).containsAll(Arrays.asList(third.getValues())));
        assertEquals("P3", third.getAsString("ID"));

        Record fourth = reader.nextRecord(false, false);
        assertTrue(Arrays.asList(new Object[] {"Elenora Scrivens", "16", "USA", "P4"}).containsAll(Arrays.asList(fourth.getValues())));
        assertEquals("P4", fourth.getAsString("ID"));
    }


    @Test
    public void testNestedRecord() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_nested.xml");
        RecordSchema schema = getSchemaWithNestedRecord();
        XMLRecordReader reader = new XMLRecordReader(is, schema, "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));
        Object[] valuesFirstRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));
        Assert.assertArrayEquals(new Object[] {"292 West Street", "Jersey City"},((Record) valuesFirstRecord[valuesFirstRecord.length - 1]).getValues());

        Object[] valuesSecondRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));
        Assert.assertArrayEquals(new Object[] {"123 6th St.", "Seattle"},((Record) valuesSecondRecord[valuesSecondRecord.length - 1]).getValues());

        Object[] valuesThirdRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));
        Assert.assertArrayEquals(new Object[] {"44 Shirley Ave.", "Los Angeles"},((Record) valuesThirdRecord[valuesThirdRecord.length - 1]).getValues());

        Object[] valuesFourthRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, Arrays.copyOfRange(valuesFourthRecord, 0, valuesFourthRecord.length - 1));
        Assert.assertArrayEquals(new Object[] {"70 Bowman St." , "Columbus"},((Record) valuesFourthRecord[valuesFourthRecord.length - 1]).getValues());
    }

    @Test
    public void testNestedRecordIgnoreSchema() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_nested.xml");
        RecordSchema schema = getSchemaWithNestedRecord();
        XMLRecordReader reader = new XMLRecordReader(is, schema, "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record first = reader.nextRecord(false, false);
        Object[] valuesFirstRecord = first.getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", "42", "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));
        assertEquals("P1", first.getAsString("ID"));

        Record nestedFirstRecord = (Record) first.getValue("ADDRESS");
        Assert.assertEquals("Jersey City", nestedFirstRecord.getAsString("CITY"));
        Assert.assertEquals("292 West Street", nestedFirstRecord.getAsString("STREET"));

        Record second = reader.nextRecord(false, false);
        Object[] valuesSecondRecord = second.getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", "33", "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));
        assertEquals("P2", second.getAsString("ID"));

        Record nestedSecondRecord = (Record) second.getValue("ADDRESS");
        Assert.assertEquals("Seattle", nestedSecondRecord.getAsString("CITY"));
        Assert.assertEquals("123 6th St.", nestedSecondRecord.getAsString("STREET"));

        Record third = reader.nextRecord(false, false);
        Object[] valuesThirdRecord = third.getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", "74", "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));
        assertEquals("P3", third.getAsString("ID"));

        Record nestedThirdRecord = (Record) third.getValue("ADDRESS");
        Assert.assertEquals("Los Angeles", nestedThirdRecord.getAsString("CITY"));
        Assert.assertEquals("44 Shirley Ave.", nestedThirdRecord.getAsString("STREET"));

        Record fourth = reader.nextRecord(false, false);
        Object[] valuesFourthRecord = fourth.getValues();
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", "16", "USA"}, Arrays.copyOfRange(valuesFourthRecord, 0, valuesFourthRecord.length - 1));
        assertEquals("P4", fourth.getAsString("ID"));

        Record nestedFourthRecord = (Record) fourth.getValue("ADDRESS");
        Assert.assertEquals("Columbus", nestedFourthRecord.getAsString("CITY"));
        Assert.assertEquals("70 Bowman St.", nestedFourthRecord.getAsString("STREET"));
    }

    @Test
    public void testSimpleArray() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_array_simple.xml");
        RecordSchema schema = getSchemaWithSimpleArray();
        XMLRecordReader reader = new XMLRecordReader(is, schema, "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record firstRecord = reader.nextRecord();
        Object[] valuesFirstRecord = firstRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));
        Object[] nestedArrayFirstRecord = (Object[]) valuesFirstRecord[valuesFirstRecord.length - 1];
        assertEquals(2, nestedArrayFirstRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2"}, nestedArrayFirstRecord);
        assertNotEquals(null, firstRecord.getValue("CHILD"));

        Record secondRecord = reader.nextRecord();
        Object[] valuesSecondRecord = secondRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));
        Object[] nestedArraySecondRecord = (Object[]) valuesSecondRecord[valuesSecondRecord.length - 1];
        assertEquals(1, nestedArraySecondRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1"}, nestedArraySecondRecord);
        assertNotEquals(null, secondRecord.getValue("CHILD"));

        Record thirdRecord = reader.nextRecord();
        Object[] valuesThirdRecord = thirdRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));
        Object[] nestedArrayThirdRecord = (Object[]) valuesThirdRecord[valuesThirdRecord.length - 1];
        assertEquals(3, nestedArrayThirdRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2", "child3"}, nestedArrayThirdRecord);
        assertNotEquals(null, thirdRecord.getValue("CHILD"));

        Record valuesFourthRecord = reader.nextRecord();
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, Arrays.copyOfRange(valuesFourthRecord.getValues(), 0, valuesFourthRecord.getValues().length - 1));
        assertEquals(null, valuesFourthRecord.getValue("CHILD"));
    }

    @Test
    public void testSimpleArrayIgnoreSchema() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_array_simple.xml");
        RecordSchema schema = getSchemaWithSimpleArray();
        XMLRecordReader reader = new XMLRecordReader(is, schema, "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record first = reader.nextRecord(false, false);
        Object[] valuesFirstRecord = first.getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", "42", "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));
        Object[] nestedArrayFirstRecord = (Object[]) valuesFirstRecord[valuesFirstRecord.length - 1];
        assertEquals(2, nestedArrayFirstRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2"}, nestedArrayFirstRecord);
        assertNotEquals(null, first.getValue("CHILD"));

        Record second = reader.nextRecord(false, false);
        Object[] valuesSecondRecord = second.getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", "33", "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));
        String nestedArraySecondRecord = (String) valuesSecondRecord[valuesSecondRecord.length - 1];
        Assert.assertEquals("child1", nestedArraySecondRecord);
        assertNotEquals(null, second.getValue("CHILD"));

        Record third = reader.nextRecord(false, false);
        Object[] valuesThirdRecord = third.getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", "74", "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));
        Object[] nestedArrayThirdRecord = (Object[]) valuesThirdRecord[valuesThirdRecord.length - 1];
        assertEquals(3, nestedArrayThirdRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2", "child3"}, nestedArrayThirdRecord);
        assertNotEquals(null, third.getValue("CHILD"));

        Record fourth = reader.nextRecord(false, false);
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", "16", "USA"}, Arrays.copyOfRange(fourth.getValues(), 0, fourth.getValues().length - 1));
        assertEquals(null, fourth.getValue("CHILD"));
    }

    @Test
    public void testNestedArrayInNestedRecord() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_array.xml");
        RecordSchema schema = getSchemaWithNestedArray();
        XMLRecordReader reader = new XMLRecordReader(is, schema, "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record firstRecord = reader.nextRecord();
        Object[] valuesFirstRecord = firstRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));

        Record nestedArrayFirstRecord = (Record) firstRecord.getValue("CHILDREN");
        assertEquals(2, ((Object[]) nestedArrayFirstRecord.getValue("CHILD")).length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2"}, ((Object[]) nestedArrayFirstRecord.getValue("CHILD")));

        Record secondRecord = reader.nextRecord();
        Object[] valuesSecondRecord = secondRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));

        Record nestedArraySecondRecord = (Record) secondRecord.getValue("CHILDREN");
        assertEquals(1, ((Object[]) nestedArraySecondRecord.getValue("CHILD")).length);
        Assert.assertArrayEquals(new Object[] {"child1"}, ((Object[]) nestedArraySecondRecord.getValue("CHILD")));

        Record thirdRecord = reader.nextRecord();
        Object[] valuesThirdRecord = thirdRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));

        Record nestedArrayThirdRecord = (Record) thirdRecord.getValue("CHILDREN");
        assertEquals(3, ((Object[]) nestedArrayThirdRecord.getValue("CHILD")).length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2", "child3"}, ((Object[]) nestedArrayThirdRecord.getValue("CHILD")));

        Record fourthRecord = reader.nextRecord();
        Object[] valuesFourthRecord = fourthRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, Arrays.copyOfRange(valuesFourthRecord, 0, valuesFourthRecord.length - 1));

        Assert.assertEquals(null, fourthRecord.getValue("CHILDREN"));
    }

    @Test
    public void testDeeplyNestedArraysAndRecords() throws IOException, MalformedRecordException {
        // test records in nested arrays
        InputStream is = new FileInputStream("src/test/resources/xml/people_complex1.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSchemaForComplexData(), "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record first = reader.nextRecord(true, true);
        Object[] grandchildren_arr = (Object[]) first.getValue("CHILDREN");

        Record first_1_1_1 = (Record)(((Object[])((Record) grandchildren_arr[0]).getValue("CHILD"))[0]);
        assertEquals("daughter", first_1_1_1.getValue("ROLE"));
        assertEquals("1-1-1", first_1_1_1.getValue("ID"));
        assertEquals("Selina", first_1_1_1.getValue("NAME"));

        Record first_1_1_2 = (Record)(((Object[])((Record) grandchildren_arr[0]).getValue("CHILD"))[1]);
        assertEquals("son", first_1_1_2.getValue("ROLE"));
        assertEquals("1-1-2", first_1_1_2.getValue("ID"));
        assertEquals("Hans", first_1_1_2.getValue("NAME"));

        Record first_1_1_3 = (Record)(((Object[])((Record) grandchildren_arr[1]).getValue("CHILD"))[0]);
        assertEquals("daughter", first_1_1_3.getValue("ROLE"));
        assertEquals("1-2-1", first_1_1_3.getValue("ID"));
        assertEquals("Selina2", first_1_1_3.getValue("NAME"));

        Record first_1_1_4 = (Record)(((Object[])((Record) grandchildren_arr[1]).getValue("CHILD"))[1]);
        assertEquals("son", first_1_1_4.getValue("ROLE"));
        assertEquals("1-2-2", first_1_1_4.getValue("ID"));
        assertEquals("Hans2", first_1_1_4.getValue("NAME"));

        Record second = reader.nextRecord(true, true);
        Object[] grandchildren_arr2 = (Object[]) second.getValue("CHILDREN");

        Record second_2_1_1 = (Record)(((Object[])((Record) grandchildren_arr2[0]).getValue("CHILD"))[0]);
        assertEquals("daughter", second_2_1_1.getValue("ROLE"));
        assertEquals("2-1-1", second_2_1_1.getValue("ID"));
        assertEquals("Selina3", second_2_1_1.getValue("NAME"));
    }

    @Test
    public void testDeeplyNestedArraysAndRecords2() throws IOException, MalformedRecordException {
        // test multiply nested arrays and records (recursion)
        InputStream is = new FileInputStream("src/test/resources/xml/people_complex2.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSchemaForComplexData2(), "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record first = reader.nextRecord();
        assertEquals("grandmother", first.getValue("ROLE"));
        assertEquals("1", first.getValue("ID"));
        assertEquals("Lisa", first.getValue("NAME"));

        Object[] gm_spouses = (Object[]) first.getValue("CHILDREN");
        assertEquals(2, gm_spouses.length);

        Object[] gm_spouse1_parents = (Object[]) ((Record) gm_spouses[0]).getValue("CHILD");
        assertEquals(2, gm_spouse1_parents.length);

        Record first_1_1 = (Record) gm_spouse1_parents[0];
        assertEquals("mother", first_1_1.getValue("ROLE"));
        assertEquals("1-1", first_1_1.getValue("ID"));
        assertEquals("Anna", first_1_1.getValue("NAME"));

        Object[] gm_spouse1_parent1_first_husband = (Object[]) first_1_1.getValue("CHILDREN");
        assertEquals(1, gm_spouse1_parent1_first_husband.length);
        Object[] gm_spouse1_parent1_children = (Object[])((Record) gm_spouse1_parent1_first_husband[0]).getValue("CHILD");

        Record first_1_1_1 = (Record) gm_spouse1_parent1_children[0];
        assertEquals("daughter", first_1_1_1.getValue("ROLE"));
        assertEquals("1-1-1", first_1_1_1.getValue("ID"));
        assertEquals("Selina", first_1_1_1.getValue("NAME"));

        Record first_1_1_2 = (Record) gm_spouse1_parent1_children[1];
        assertEquals("son", first_1_1_2.getValue("ROLE"));
        assertEquals("1-1-2", first_1_1_2.getValue("ID"));
        assertEquals("Hans", first_1_1_2.getValue("NAME"));

        Record first_1_2 = (Record) gm_spouse1_parents[1];
        assertEquals("mother", first_1_2.getValue("ROLE"));
        assertEquals("1-2", first_1_2.getValue("ID"));
        assertEquals("Catrina", first_1_2.getValue("NAME"));

        Object[] gm_spouse2_parents = (Object[]) ((Record) gm_spouses[1]).getValue("CHILD");
        assertEquals(1, gm_spouse2_parents.length);

        Record second = reader.nextRecord();
        Record second_2_1_1 = (Record)((Object[])((Record)((Object[])((Record)((Object[])((Record)((Object[]) second
                .getValue("CHILDREN"))[0])
                .getValue("CHILD"))[0])
                .getValue("CHILDREN"))[0])
                .getValue("CHILD"))[0];
        assertEquals("daughter", second_2_1_1.getValue("ROLE"));
        assertEquals("2-1-1", second_2_1_1.getValue("ID"));
        assertEquals("Selina3", second_2_1_1.getValue("NAME"));
    }

    @Test
    public void testDeeplyNestedArraysAndRecordsIgnoreSchema() throws IOException, MalformedRecordException {
        // test multiply nested arrays and records (recursion)
        InputStream is = new FileInputStream("src/test/resources/xml/people_complex2.xml");
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(Collections.emptyList()),
                "PEOPLE", "PERSON", null, "CONTENT", dateFormat, timeFormat, timestampFormat, Mockito.mock(ComponentLog.class));

        Record first = reader.nextRecord(false, false);
        assertEquals("1", first.getValue("ID"));
        assertEquals("Lisa", first.getValue("NAME"));
        assertEquals("grandmother", first.getValue("ROLE"));
        Object[] gm_arr = (Object[]) first.getValue("CHILDREN");
        assertEquals(2, gm_arr.length);

        Record gm_hus1_arr_rec = (Record) gm_arr[0];
        assertEquals("husband1", gm_hus1_arr_rec.getValue("SPOUSE"));
        Object[] gm_hus1_arr_rec_arr = (Object[]) gm_hus1_arr_rec.getValue("CHILD");
        assertEquals(2, gm_hus1_arr_rec_arr.length);

        Record child1_1 = (Record) gm_hus1_arr_rec_arr[0];
        assertEquals("1-1", child1_1.getValue("ID"));
        assertEquals("Anna", child1_1.getValue("NAME"));
        assertEquals("mother", child1_1.getValue("ROLE"));

        Record child1_1_rec = (Record) child1_1.getValue("CHILDREN");
        assertEquals("first husband", child1_1_rec.getValue("ID"));
        Object[] child1_1_rec_arr = (Object[]) child1_1_rec.getValue("CHILD");
        assertEquals(2, child1_1_rec_arr.length);

        Record child1_1_1 = (Record) child1_1_rec_arr[0];
        assertEquals("1-1-1", child1_1_1.getValue("ID"));
        assertEquals("Selina", child1_1_1.getValue("NAME"));
        assertEquals("daughter", child1_1_1.getValue("ROLE"));

        Record child1_1_2 = (Record) child1_1_rec_arr[1];
        assertEquals("1-1-2", child1_1_2.getValue("ID"));
        assertEquals("Hans", child1_1_2.getValue("NAME"));
        assertEquals("son", child1_1_2.getValue("ROLE"));

        Record child1_2 = (Record) gm_hus1_arr_rec_arr[1];
        assertEquals("1-2", child1_2.getValue("ID"));
        assertEquals("Catrina", child1_2.getValue("NAME"));
        assertEquals("mother", child1_2.getValue("ROLE"));

        Record gm_hus2_arr_rec = (Record) gm_arr[1];
        assertEquals("husband2", gm_hus2_arr_rec.getValue("SPOUSE"));

        Record child1_3 = (Record) gm_hus2_arr_rec.getValue("CHILD");
        assertEquals("1-3", child1_3.getValue("ID"));
        assertEquals("Anna2", child1_3.getValue("NAME"));
        assertEquals("mother", child1_3.getValue("ROLE"));
        assertEquals(2, ((Object[])((Record) child1_3.getValue("CHILDREN")).getValue("CHILD")).length);

        Record second = reader.nextRecord(false, false);
        assertEquals("2-1-1", ((Record)((Record)((Record)((Record) second.getValue("CHILDREN"))
                .getValue("CHILD"))
                .getValue("CHILDREN"))
                .getValue("CHILD"))
                .getValue("ID"));
    }

    private List<RecordField> getSimpleRecordFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("NAME", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("AGE", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("COUNTRY", RecordFieldType.STRING.getDataType()));
        return fields;
    }

    private List<RecordField> getSimpleRecordFields2() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("NAME", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("COUNTRY", RecordFieldType.STRING.getDataType()));
        return fields;
    }

    private List<RecordField> getNestedRecordFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("STREET", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("CITY", RecordFieldType.STRING.getDataType()));
        return fields;
    }

    private RecordSchema getSimpleSchema() {
        return new SimpleRecordSchema(getSimpleRecordFields());
    }

    private RecordSchema getSimpleSchema2() {
        return new SimpleRecordSchema(getSimpleRecordFields2());
    }

    private RecordSchema getNestedSchema() {
        return new SimpleRecordSchema(getNestedRecordFields());
    }

    private RecordSchema getSchemaWithNestedRecord() {
        final List<RecordField> fields = getSimpleRecordFields();
        final DataType recordType = RecordFieldType.RECORD.getRecordDataType(getNestedSchema());
        fields.add(new RecordField("ADDRESS", recordType));
        return new SimpleRecordSchema(fields);
    }

    private RecordSchema getSchemaWithSimpleArray() {
        final List<RecordField> fields = getSimpleRecordFields();
        final DataType arrayType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
        fields.add(new RecordField("CHILD", arrayType));
        return new SimpleRecordSchema(fields);
    }

    private RecordSchema getSchemaWithNestedArray() {
        final List<RecordField> fields = getSimpleRecordFields();
        final DataType arrayType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
        final List<RecordField> nestedArrayField = new ArrayList<RecordField>() {{ add(new RecordField("CHILD", arrayType)); }};

        final DataType recordType = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(nestedArrayField));
        fields.add(new RecordField("CHILDREN", recordType));
        return new SimpleRecordSchema(fields);
    }

    private List<RecordField> getSimpleFieldsForComplexData() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("ID", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("NAME", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("ROLE", RecordFieldType.STRING.getDataType()));
        return fields;
    }

    private RecordSchema getSchemaForComplexData() {
        final DataType grandchild = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(getSimpleFieldsForComplexData()));
        final DataType grandchild_arr1 = RecordFieldType.ARRAY.getArrayDataType(grandchild);
        final DataType grandchildren = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(
                new ArrayList<RecordField>() {{ add(new RecordField("CHILD", grandchild_arr1)); }}));
        final DataType grandchild_arr = RecordFieldType.ARRAY.getArrayDataType(grandchildren);
        return new SimpleRecordSchema(
                new ArrayList<RecordField>() {{ add(new RecordField("CHILDREN", grandchild_arr)); }});
    }

    private RecordSchema getSchemaForComplexData2() {
        final DataType grandchild = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(getSimpleFieldsForComplexData()));
        final DataType grandchild_arr = RecordFieldType.ARRAY.getArrayDataType(grandchild);

        final DataType grandchildren = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(
                new ArrayList<RecordField>() {{ add(new RecordField("CHILD", grandchild_arr)); }}));
        final DataType grandchildren_arr = RecordFieldType.ARRAY.getArrayDataType(grandchildren);

        final DataType parent = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(
                new ArrayList<RecordField>() {{
                    add(new RecordField("CHILDREN", grandchildren_arr));
                    addAll(getSimpleFieldsForComplexData());
                }}));
        final DataType parent_arr = RecordFieldType.ARRAY.getArrayDataType(parent);

        final DataType parents = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(
                new ArrayList<RecordField>() {{
                    add(new RecordField("CHILD", parent_arr));
                }}));
        final DataType parents_arr = RecordFieldType.ARRAY.getArrayDataType(parents);

        final List<RecordField> fields = new ArrayList<RecordField>() {{
            add(new RecordField("CHILDREN", parents_arr));
            addAll(getSimpleFieldsForComplexData());
        }};
        return new SimpleRecordSchema(fields);
    }
}
