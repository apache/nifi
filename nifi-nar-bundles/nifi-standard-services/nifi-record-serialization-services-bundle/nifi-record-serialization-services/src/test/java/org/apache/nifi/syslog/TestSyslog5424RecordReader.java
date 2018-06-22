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

package org.apache.nifi.syslog;

import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.syslog.attributes.Syslog5424Attributes;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.keyproviders.SimpleKeyProvider;
import org.apache.nifi.syslog.parsers.StrictSyslog5424Parser;
import org.apache.nifi.syslog.utils.NifiStructuredDataPolicy;
import org.apache.nifi.syslog.utils.NilHandlingPolicy;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestSyslog5424RecordReader {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final String expectedVersion = "1";
    private static final String expectedMessage = "Removing instance";
    private static final String expectedAppName = "d0602076-b14a-4c55-852a-981e7afeed38";
    private static final String expectedHostName = "loggregator";
    private static final String expectedPri = "14";
    private static final String expectedProcId = "DEA";
    private static final Timestamp expectedTimestamp = Timestamp.from(Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse("2014-06-20T09:14:07+00:00")));
    private static final String expectedMessageId = "MSG-01";
    private static final String expectedFacility = "1";
    private static final String expectedSeverity = "6";

    private static final String expectedIUT1 = "3";
    private static final String expectedIUT2 = "4";
    private static final String expectedEventSource1 = "Application";
    private static final String expectedEventSource2 = "Other Application";
    private static final String expectedEventID1 = "1011";
    private static final String expectedEventID2 = "2022";

    @Test
    @SuppressWarnings("unchecked")
    public void testParseSingleLine() throws IOException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/syslog/syslog5424/log_all.txt"))) {
            StrictSyslog5424Parser parser = new StrictSyslog5424Parser(CHARSET,
                    NilHandlingPolicy.NULL,
                    NifiStructuredDataPolicy.MAP_OF_MAPS,
                    new SimpleKeyProvider());
            final Syslog5424RecordReader deserializer = new Syslog5424RecordReader(parser, fis, Syslog5424Reader.createRecordSchema());

            final Record record = deserializer.nextRecord();
            assertNotNull(record.getValues());

            Assert.assertEquals(expectedVersion, record.getAsString(SyslogAttributes.VERSION.key()));
            Assert.assertEquals(expectedMessage, record.getAsString(SyslogAttributes.BODY.key()));
            Assert.assertEquals(expectedAppName, record.getAsString(Syslog5424Attributes.APP_NAME.key()));
            Assert.assertEquals(expectedHostName, record.getAsString(SyslogAttributes.HOSTNAME.key()));
            Assert.assertEquals(expectedPri, record.getAsString(SyslogAttributes.PRIORITY.key()));
            Assert.assertEquals(expectedSeverity, record.getAsString(SyslogAttributes.SEVERITY.key()));
            Assert.assertEquals(expectedFacility, record.getAsString(SyslogAttributes.FACILITY.key()));
            Assert.assertEquals(expectedProcId, record.getAsString(Syslog5424Attributes.PROCID.key()));
            Assert.assertEquals(expectedTimestamp, (Timestamp)record.getValue(SyslogAttributes.TIMESTAMP.key()));
            Assert.assertEquals(expectedMessageId, record.getAsString(Syslog5424Attributes.MESSAGEID.key()));

            Assert.assertNotNull(record.getValue(Syslog5424Attributes.STRUCTURED_BASE.key()));
            Map<String,Object> structured = (Map<String,Object>)record.getValue(Syslog5424Attributes.STRUCTURED_BASE.key());

            Assert.assertTrue(structured.containsKey("exampleSDID@32473"));
            Map<String, Object> example1 = (Map<String, Object>) structured.get("exampleSDID@32473");

            Assert.assertTrue(example1.containsKey("iut"));
            Assert.assertTrue(example1.containsKey("eventSource"));
            Assert.assertTrue(example1.containsKey("eventID"));
            Assert.assertEquals(expectedIUT1, example1.get("iut").toString());
            Assert.assertEquals(expectedEventSource1, example1.get("eventSource").toString());
            Assert.assertEquals(expectedEventID1, example1.get("eventID").toString());

            Assert.assertTrue(structured.containsKey("exampleSDID@32480"));
            Map<String, Object> example2 = (Map<String, Object>) structured.get("exampleSDID@32480");

            Assert.assertTrue(example2.containsKey("iut"));
            Assert.assertTrue(example2.containsKey("eventSource"));
            Assert.assertTrue(example2.containsKey("eventID"));
            Assert.assertEquals(expectedIUT2, example2.get("iut").toString());
            Assert.assertEquals(expectedEventSource2, example2.get("eventSource").toString());
            Assert.assertEquals(expectedEventID2, example2.get("eventID").toString());
            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParseSingleLineSomeNulls() throws IOException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/syslog/syslog5424/log.txt"))) {
            StrictSyslog5424Parser parser = new StrictSyslog5424Parser(CHARSET,
                    NilHandlingPolicy.NULL,
                    NifiStructuredDataPolicy.MAP_OF_MAPS,
                    new SimpleKeyProvider());
            final Syslog5424RecordReader deserializer = new Syslog5424RecordReader(parser, fis, Syslog5424Reader.createRecordSchema());

            final Record record = deserializer.nextRecord();
            assertNotNull(record.getValues());

            Assert.assertEquals(expectedVersion, record.getAsString(SyslogAttributes.VERSION.key()));
            Assert.assertEquals(expectedMessage, record.getAsString(SyslogAttributes.BODY.key()));
            Assert.assertEquals(expectedAppName, record.getAsString(Syslog5424Attributes.APP_NAME.key()));
            Assert.assertEquals(expectedHostName, record.getAsString(SyslogAttributes.HOSTNAME.key()));
            Assert.assertEquals(expectedPri, record.getAsString(SyslogAttributes.PRIORITY.key()));
            Assert.assertEquals(expectedSeverity, record.getAsString(SyslogAttributes.SEVERITY.key()));
            Assert.assertEquals(expectedFacility, record.getAsString(SyslogAttributes.FACILITY.key()));
            Assert.assertEquals(expectedProcId, record.getAsString(Syslog5424Attributes.PROCID.key()));
            Assert.assertEquals(expectedTimestamp, (Timestamp)record.getValue(SyslogAttributes.TIMESTAMP.key()));
            Assert.assertNull(record.getAsString(Syslog5424Attributes.MESSAGEID.key()));

            Assert.assertNotNull(record.getValue(Syslog5424Attributes.STRUCTURED_BASE.key()));
            Map<String,Object> structured = (Map<String,Object>)record.getValue(Syslog5424Attributes.STRUCTURED_BASE.key());

            Assert.assertTrue(structured.containsKey("exampleSDID@32473"));
            Map<String, Object> example1 = (Map<String, Object>) structured.get("exampleSDID@32473");

            Assert.assertTrue(example1.containsKey("iut"));
            Assert.assertTrue(example1.containsKey("eventSource"));
            Assert.assertTrue(example1.containsKey("eventID"));
            Assert.assertEquals(expectedIUT1, example1.get("iut").toString());
            Assert.assertEquals(expectedEventSource1, example1.get("eventSource").toString());
            Assert.assertEquals(expectedEventID1, example1.get("eventID").toString());

            Assert.assertTrue(structured.containsKey("exampleSDID@32480"));
            Map<String, Object> example2 = (Map<String, Object>) structured.get("exampleSDID@32480");

            Assert.assertTrue(example2.containsKey("iut"));
            Assert.assertTrue(example2.containsKey("eventSource"));
            Assert.assertTrue(example2.containsKey("eventID"));
            Assert.assertEquals(expectedIUT2, example2.get("iut").toString());
            Assert.assertEquals(expectedEventSource2, example2.get("eventSource").toString());
            Assert.assertEquals(expectedEventID2, example2.get("eventID").toString());
            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParseMultipleLine() throws IOException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/syslog/syslog5424/log_mix.txt"))) {
            StrictSyslog5424Parser parser = new StrictSyslog5424Parser(CHARSET,
                    NilHandlingPolicy.NULL,
                    NifiStructuredDataPolicy.MAP_OF_MAPS,
                    new SimpleKeyProvider());
            final Syslog5424RecordReader deserializer = new Syslog5424RecordReader(parser, fis, Syslog5424Reader.createRecordSchema());

            Record record = deserializer.nextRecord();
            int count = 0;
            while (record != null){
                assertNotNull(record.getValues());
                count++;
                record = deserializer.nextRecord();
            }
            Assert.assertEquals(count, 3);
            deserializer.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParseMultipleLineWithError() throws IOException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/syslog/syslog5424/log_mix_in_error.txt"))) {
            StrictSyslog5424Parser parser = new StrictSyslog5424Parser(CHARSET,
                    NilHandlingPolicy.NULL,
                    NifiStructuredDataPolicy.MAP_OF_MAPS,
                    new SimpleKeyProvider());
            final Syslog5424RecordReader deserializer = new Syslog5424RecordReader(parser, fis, Syslog5424Reader.createRecordSchema());

            Record record = deserializer.nextRecord();
            int count = 0;
            int exceptionCount = 0;
            while (record != null){
                assertNotNull(record.getValues());
                try {
                    record = deserializer.nextRecord();
                    count++;
                } catch (Exception e) {
                    exceptionCount++;
                }
            }
            Assert.assertEquals(count, 3);
            Assert.assertEquals(exceptionCount,1);
            deserializer.close();
        }
    }

    public void writeSchema() {
        String s = Syslog5424Reader.createRecordSchema().toString();
        System.out.println(s);
        System.out.println(AvroTypeUtil.extractAvroSchema( Syslog5424Reader.createRecordSchema() ).toString(true));
    }

}
