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
package org.apache.nifi.text;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestTextLineRecordReader {

    @Before
    public void setUp() {

    }

    @Test
    public void testReadAllLines() throws Exception {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/text/test_lines.txt"));
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 0, 1, false, "\n", "UTF-8")) {

            Record record = reader.nextRecord();
            assertNotNull(record);
            Object[] values = record.getValues();
            assertEquals(1, values.length);
            assertEquals("Header line 1", values[0]);
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Header line 2", record.getValue("myField"));
            assertNull(record.getValue("not a field"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("First line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Second line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Third line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Next non-empty line", record.getValue("myField"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadNonEmptyLines() throws Exception {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/text/test_lines.txt"));
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 0, 1, true, "\n", "UTF-8")) {

            Record record = reader.nextRecord();
            assertNotNull(record);
            Object[] values = record.getValues();
            assertEquals(1, values.length);
            assertEquals("Header line 1", values[0]);
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Header line 2", record.getValue("myField"));
            assertNull(record.getValue("not a field"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("First line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Second line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Third line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Next non-empty line", record.getValue("myField"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadGroupedLinesIgnoreEmpty() throws Exception {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/text/test_lines.txt"));
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 0, 2, true, "\n", "UTF-8")) {

            Record record = reader.nextRecord();
            assertNotNull(record);
            Object[] values = record.getValues();
            assertEquals(1, values.length);
            assertEquals("Header line 1\nHeader line 2", values[0]);
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("First line\nSecond line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Third line\nNext non-empty line", record.getValue("myField"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadGroupedLines() throws Exception {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/text/test_lines.txt"));
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 0, 2, false, "\n", "UTF-8")) {

            Record record = reader.nextRecord();
            assertNotNull(record);
            Object[] values = record.getValues();
            assertEquals(1, values.length);
            assertEquals("Header line 1\nHeader line 2", values[0]);
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("First line\nSecond line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Third line\n", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Next non-empty line", record.getValue("myField"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadGroupedLinesLargerGroupThanLinesPresent() throws Exception {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/text/test_lines.txt"));
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 0, 100, false, "\n", "UTF-8")) {

            Record record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Header line 1\nHeader line 2\nFirst line\nSecond line\nThird line\n\nNext non-empty line", record.getValue("myField"));
            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testSkipHeader() throws Exception {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/text/test_lines.txt"));
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 2, 1, false, "\n", "UTF-8")) {

            Record record = reader.nextRecord();
            assertNotNull(record);
            Object[] values = record.getValues();
            assertEquals(1, values.length);
            assertEquals("First line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Second line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Third line", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("Next non-empty line", record.getValue("myField"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadNoLines() throws Exception {
        try (final InputStream fis = new ByteArrayInputStream(new byte[0]);
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 0, 1, false, "\n", "UTF-8")) {
            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadSingleEmptyLine() throws Exception {
        try (final InputStream fis = new ByteArrayInputStream("\n".getBytes());
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 0, 1, false, "\n", "UTF-8")) {
            Record record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("", record.getValue("myField"));
            assertNull(reader.nextRecord());
        }

        // Try one empty line while ignoring empty lines, should return null for the first record
        try (final InputStream fis = new ByteArrayInputStream("\n".getBytes());
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 0, 1, true, "\n", "UTF-8")) {
            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadEmptyLines() throws Exception {
        try (final InputStream fis = new ByteArrayInputStream("\n\n".getBytes());
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 0, 1, false, "\n", "UTF-8")) {
            Record record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("", record.getValue("myField"));
            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadEmptyLinesSkipHeader() throws Exception {
        // Try with a record following the skip
        try (final InputStream fis = new ByteArrayInputStream("1\n\n".getBytes());
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 1, 1, false, "\n", "UTF-8")) {
            Record record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("", record.getValue("myField"));
            assertNull(reader.nextRecord());
        }

        // Try with no records following the skip
        try (final InputStream fis = new ByteArrayInputStream("1\n2\n".getBytes());
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 2, 1, false, "\n", "UTF-8")) {
            assertNull(reader.nextRecord());
        }

        // Try with skip count > number of lines
        try (final InputStream fis = new ByteArrayInputStream("1\n2\n".getBytes());
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 3, 1, false, "\n", "UTF-8")) {
            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadDifferentDelimiter() throws Exception {
        try (final InputStream fis = new ByteArrayInputStream("a\nb,c\n\nd,,e".getBytes());
             final TextLineRecordReader reader = new TextLineRecordReader(fis, Mockito.mock(ComponentLog.class), "myField", 0, 1, false, ",", "UTF-8")) {
            Record record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("a\nb", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("c\n\nd", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("", record.getValue("myField"));
            record = reader.nextRecord();
            assertNotNull(record);
            assertEquals("e", record.getValue("myField"));
            assertNull(reader.nextRecord());
        }
    }
}