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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ITApacheCSVRecordReader {

    private final CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setTrim(true).setQuote('"').build();

    private List<RecordField> getDefaultFields() {
        return createStringFields(new String[]{"id", "name", "balance", "address", "city", "state", "zipCode", "country"});
    }

    private List<RecordField> createStringFields(String[] fieldNames) {
        final List<RecordField> fields = new ArrayList<>();
        for (final String fieldName : fieldNames) {
            fields.add(new RecordField(fieldName, RecordFieldType.STRING.getDataType()));
        }
        return fields;
    }

    @Test
    public void testParserPerformance() throws IOException, MalformedRecordException {
        // Generates about 130MB of data
        final int NUM_LINES = 2500000;
        String sb = "id,name,balance,address,city,state,zipCode,country\n" + "1,John Doe,4750.89D,123 My Street,My City,MS,11111,USA\n".repeat(NUM_LINES);
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream bais = new ByteArrayInputStream(sb.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            Record record;
            int numRecords = 0;
            while ((record = reader.nextRecord()) != null) {
                assertNotNull(record);
                numRecords++;
            }
            assertEquals(NUM_LINES, numRecords);
        }
    }

    @Test
    public void testExceptionThrownOnParseProblem() {
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setQuoteMode(QuoteMode.ALL).setTrim(true).setDelimiter(',').build();
        final int NUM_LINES = 25;
        StringBuilder sb = new StringBuilder("\"id\",\"name\",\"balance\"");
        for (int i = 0; i < NUM_LINES; i++) {
            sb.append(String.format("\"%s\",\"John Doe\",\"4750.89D\"\n", i));
        }
        // cause a parse problem
        sb.append(String.format("\"%s\"dieParser,\"John Doe\",\"4750.89D\"\n", NUM_LINES ));
        sb.append(String.format("\"%s\",\"John Doe\",\"4750.89D\"\n", NUM_LINES + 1));
        final RecordSchema schema = new SimpleRecordSchema(createStringFields(new String[] {"id", "name", "balance"}));

        try (final InputStream bais = new ByteArrayInputStream(sb.toString().getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, csvFormat, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            while (reader.nextRecord() != null) {}
        } catch (Exception e) {
            assertInstanceOf(MalformedRecordException.class, e);
        }
    }
}
