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
package org.apache.nifi.windowsevent;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestWindowsEventLogRecordReader {

    private static final String DATE_FORMAT = RecordFieldType.DATE.getDefaultFormat();
    private static final String TIME_FORMAT = RecordFieldType.TIME.getDefaultFormat();
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    @Test
    public void testSingleEvent() throws IOException, MalformedRecordException {
        InputStream is = new BufferedInputStream(new FileInputStream("src/test/resources/windowseventlog/single_event.xml"));
        WindowsEventLogRecordReader reader = new WindowsEventLogRecordReader(is, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, Mockito.mock(ComponentLog.class));
        Record r = reader.nextRecord();
        assertNotNull(r);
        assertEquals(2, r.getValues().length);

        // Verify some System fields
        Object childObj = r.getValue("System");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        Record childRecord = (Record) childObj;
        assertEquals(14, childRecord.getValues().length);
        childObj = childRecord.getValue("Provider");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(2, childRecord.getValues().length);
        assertEquals("Microsoft-Windows-Security-Auditing", childRecord.getAsString("Name"));

        // Verify some EventData fields, including Data fields
        childObj = r.getValue("EventData");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(4, childRecord.getValues().length);
        assertEquals("DOMAIN", childRecord.getAsString("TargetDomainName"));

        assertNull(reader.nextRecord());
    }

    @Test
    public void testSingleEventNoParent() throws IOException, MalformedRecordException {
        InputStream is = new BufferedInputStream(new FileInputStream("src/test/resources/windowseventlog/single_event_no_parent.xml"));
        WindowsEventLogRecordReader reader = new WindowsEventLogRecordReader(is, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, Mockito.mock(ComponentLog.class));
        Record r = reader.nextRecord();
        assertNotNull(r);
        assertEquals(2, r.getValues().length);

        // Verify some System fields
        Object childObj = r.getValue("System");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        Record childRecord = (Record) childObj;
        assertEquals(14, childRecord.getValues().length);
        childObj = childRecord.getValue("Provider");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(2, childRecord.getValues().length);
        assertEquals("Microsoft-Windows-Security-Auditing", childRecord.getAsString("Name"));

        // Verify some EventData fields, including Data fields
        childObj = r.getValue("EventData");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(4, childRecord.getValues().length);
        assertEquals("DOMAIN", childRecord.getAsString("TargetDomainName"));

        assertNull(reader.nextRecord());
    }

    @Test
    public void testMultipleEvents() throws IOException, MalformedRecordException {
        InputStream is = new BufferedInputStream(new FileInputStream("src/test/resources/windowseventlog/multiple_events.xml"));
        WindowsEventLogRecordReader reader = new WindowsEventLogRecordReader(is, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, Mockito.mock(ComponentLog.class));
        Record r = reader.nextRecord(true, true);
        assertNotNull(r);
        assertEquals(2, r.getValues().length);

        // Verify some System fields
        Object childObj = r.getValue("System");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        Record childRecord = (Record) childObj;
        assertEquals(14, childRecord.getValues().length);
        childObj = childRecord.getValue("Provider");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(2, childRecord.getValues().length);
        assertEquals("Microsoft-Windows-Security-Auditing", childRecord.getAsString("Name"));

        // Verify some EventData fields, including Data fields
        childObj = r.getValue("EventData");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(16, childRecord.getValues().length);
        assertEquals("DOMAIN", childRecord.getAsString("TargetDomainName"));
        assertNull(childRecord.getValue("SubjectUserName"));

        // Verify next record
        r = reader.nextRecord(true, true);
        assertNotNull(r);
        assertEquals(2, r.getValues().length);
        childObj = r.getValue("System");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(14, childRecord.getValues().length);
        childObj = childRecord.getValue("Provider");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(2, childRecord.getValues().length);
        assertEquals("Microsoft-Windows-Security-Auditing", childRecord.getAsString("Name"));

        // Verify some EventData fields, including Data fields
        childObj = r.getValue("EventData");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(16, childRecord.getValues().length);
        assertEquals("DOMAIN", childRecord.getAsString("TargetDomainName"));
        assertEquals("-", childRecord.getValue("SubjectUserName"));

        // Verify next record
        r = reader.nextRecord(true, false);
        assertNotNull(r);
        assertEquals(2, r.getValues().length);
        childObj = r.getValue("System");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(14, childRecord.getValues().length);
        childObj = childRecord.getValue("Provider");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(2, childRecord.getValues().length);
        assertEquals("Microsoft-Windows-Security-Auditing", childRecord.getAsString("Name"));

        // Verify some EventData fields, including Data fields
        childObj = r.getValue("EventData");
        assertNotNull(childObj);
        assertInstanceOf(Record.class, childObj);
        childRecord = (Record) childObj;
        assertEquals(16, childRecord.getValues().length);
        assertNull(childRecord.getAsString("TargetDomainName"));
        assertEquals("User", childRecord.getValue("SubjectUserName"));
        assertEquals("D9060000", childRecord.getValue("SessionEnv"));

        assertNull(reader.nextRecord());
    }

    @Test
    public void testNotXmlInput() {
        InputStream is = new ByteArrayInputStream("I am not XML" .getBytes(StandardCharsets.UTF_8));
        assertThrows(MalformedRecordException.class,
                () -> new WindowsEventLogRecordReader(is, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, Mockito.mock(ComponentLog.class)));
    }
}