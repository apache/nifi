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

package org.apache.nifi.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCSVRecordReader {
    private final DataType stringDataType = RecordFieldType.STRING.getDataType();
    private final DataType doubleDataType = RecordFieldType.DOUBLE.getDataType();
    private final DataType timeDataType = RecordFieldType.TIME.getDataType();

    @Test
    public void testSimpleParse() throws IOException, MalformedRecordException {
        final Map<String, DataType> overrides = new HashMap<>();
        overrides.put("balance", doubleDataType);
        overrides.put("other", timeDataType);

        try (final InputStream fis = new FileInputStream(new File("src/test/resources/csv/single-bank-account.csv"))) {
            final CSVRecordReader reader = new CSVRecordReader(fis, null, overrides);

            final RecordSchema schema = reader.getSchema();
            verifyFields(schema);

            final Object[] record = reader.nextRecord().getValues();
            final Object[] expectedValues = new Object[] {"1", "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            Assert.assertArrayEquals(expectedValues, record);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testMultipleRecords() throws IOException, MalformedRecordException {
        final Map<String, DataType> overrides = new HashMap<>();
        overrides.put("balance", doubleDataType);

        try (final InputStream fis = new FileInputStream(new File("src/test/resources/csv/multi-bank-account.csv"))) {
            final CSVRecordReader reader = new CSVRecordReader(fis, null, overrides);

            final RecordSchema schema = reader.getSchema();
            verifyFields(schema);

            final Object[] firstRecord = reader.nextRecord().getValues();
            final Object[] firstExpectedValues = new Object[] {"1", "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            Assert.assertArrayEquals(firstExpectedValues, firstRecord);

            final Object[] secondRecord = reader.nextRecord().getValues();
            final Object[] secondExpectedValues = new Object[] {"2", "Jane Doe", 4820.09D, "321 Your Street", "Your City", "NY", "33333", "USA"};
            Assert.assertArrayEquals(secondExpectedValues, secondRecord);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testExtraWhiteSpace() throws IOException, MalformedRecordException {
        final Map<String, DataType> overrides = new HashMap<>();
        overrides.put("balance", doubleDataType);

        try (final InputStream fis = new FileInputStream(new File("src/test/resources/csv/extra-white-space.csv"))) {
            final CSVRecordReader reader = new CSVRecordReader(fis, Mockito.mock(ComponentLog.class), overrides);

            final RecordSchema schema = reader.getSchema();
            verifyFields(schema);

            final Object[] firstRecord = reader.nextRecord().getValues();
            final Object[] firstExpectedValues = new Object[] {"1", "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            Assert.assertArrayEquals(firstExpectedValues, firstRecord);

            final Object[] secondRecord = reader.nextRecord().getValues();
            final Object[] secondExpectedValues = new Object[] {"2", "Jane Doe", 4820.09D, "321 Your Street", "Your City", "NY", "33333", "USA"};
            Assert.assertArrayEquals(secondExpectedValues, secondRecord);

            assertNull(reader.nextRecord());
        }
    }

    private void verifyFields(final RecordSchema schema) {
        final List<String> fieldNames = schema.getFieldNames();
        final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country");
        assertEquals(expectedFieldNames, fieldNames);

        final List<DataType> dataTypes = schema.getDataTypes();
        final List<DataType> expectedDataTypes = Arrays.asList(stringDataType, stringDataType, doubleDataType,
            stringDataType, stringDataType, stringDataType, stringDataType, stringDataType);
        assertEquals(expectedDataTypes, dataTypes);
    }
}
