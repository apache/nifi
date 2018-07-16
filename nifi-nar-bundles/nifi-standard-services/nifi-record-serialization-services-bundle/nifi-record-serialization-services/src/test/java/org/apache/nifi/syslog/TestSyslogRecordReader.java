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
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.parsers.SyslogParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestSyslogRecordReader {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final String PRI = "34";
    private static final String SEV = "2";
    private static final String FAC = "4";
    private static final String TIME = "Oct 13 15:43:23";
    private static final String HOST = "localhost.home";
    private static final String IPV6SRC = "fe80::216:3300:eeaa:eeaa";
    private static final String IPV4SRC = "8.8.4.4";
    private static final String BODY = "some message";

    static final String VALID_MESSAGE_RFC3164_0 = "<" + PRI + ">" + TIME + " " + HOST + " " + BODY + "\n";
    static final String VALID_MESSAGE_RFC3164_1 = "<" + PRI + ">" + TIME + " " + IPV6SRC + " " + BODY + "\n";
    static final String VALID_MESSAGE_RFC3164_2 = "<" + PRI + ">" + TIME + " " + IPV4SRC + " " + BODY + "\n";


    private static final String expectedVersion = "1";
    private static final String expectedAppName = "d0602076-b14a-4c55-852a-981e7afeed38";
    private static final String expectedHostName = "loggregator";
    private static final String expectedProcId = "DEA";
    private static final String expectedMessageId = "MSG-01";
    private static final String expectedMessage = expectedAppName +
            " " + expectedProcId +
            " " + expectedMessageId +
            " " + "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] [exampleSDID@32480 iut=\"4\" eventSource=\"Other Application\" eventID=\"2022\"]" +
            " " + "Removing instance";
    private static final String expectedPri = "14";
    private static final String expectedTimestamp = "2014-06-20T09:14:07+00:00";
    private static final String expectedFacility = "1";
    private static final String expectedSeverity = "6";

    @Test
    @SuppressWarnings("unchecked")
    public void testParseSingleLine() throws IOException, MalformedRecordException {
        try (final InputStream fis = new ByteArrayInputStream(VALID_MESSAGE_RFC3164_0.getBytes(CHARSET))){
            SyslogParser parser = new SyslogParser(CHARSET);
            final SyslogRecordReader deserializer = new SyslogRecordReader(parser, fis, SyslogReader.createRecordSchema());

            final Record record = deserializer.nextRecord();
            assertNotNull(record.getValues());

            Assert.assertEquals(BODY, record.getAsString(SyslogAttributes.BODY.key()));
            Assert.assertEquals(HOST, record.getAsString(SyslogAttributes.HOSTNAME.key()));
            Assert.assertEquals(PRI, record.getAsString(SyslogAttributes.PRIORITY.key()));
            Assert.assertEquals(SEV, record.getAsString(SyslogAttributes.SEVERITY.key()));
            Assert.assertEquals(FAC, record.getAsString(SyslogAttributes.FACILITY.key()));
            Assert.assertEquals(TIME, record.getAsString(SyslogAttributes.TIMESTAMP.key()));
            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParseSingleLineIPV6() throws IOException, MalformedRecordException {
        try (final InputStream fis = new ByteArrayInputStream(VALID_MESSAGE_RFC3164_1.getBytes(CHARSET))){
            SyslogParser parser = new SyslogParser(CHARSET);
            final SyslogRecordReader deserializer = new SyslogRecordReader(parser, fis, SyslogReader.createRecordSchema());

            final Record record = deserializer.nextRecord();
            assertNotNull(record.getValues());

            Assert.assertEquals(BODY, record.getAsString(SyslogAttributes.BODY.key()));
            Assert.assertEquals(IPV6SRC, record.getAsString(SyslogAttributes.HOSTNAME.key()));
            Assert.assertEquals(PRI, record.getAsString(SyslogAttributes.PRIORITY.key()));
            Assert.assertEquals(SEV, record.getAsString(SyslogAttributes.SEVERITY.key()));
            Assert.assertEquals(FAC, record.getAsString(SyslogAttributes.FACILITY.key()));
            Assert.assertEquals(TIME, record.getAsString(SyslogAttributes.TIMESTAMP.key()));
            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParseSingleLineIPV4() throws IOException, MalformedRecordException {
        try (final InputStream fis = new ByteArrayInputStream(VALID_MESSAGE_RFC3164_2.getBytes(CHARSET))){
            SyslogParser parser = new SyslogParser(CHARSET);
            final SyslogRecordReader deserializer = new SyslogRecordReader(parser, fis, SyslogReader.createRecordSchema());

            final Record record = deserializer.nextRecord();
            assertNotNull(record.getValues());

            Assert.assertEquals(BODY, record.getAsString(SyslogAttributes.BODY.key()));
            Assert.assertEquals(IPV4SRC, record.getAsString(SyslogAttributes.HOSTNAME.key()));
            Assert.assertEquals(PRI, record.getAsString(SyslogAttributes.PRIORITY.key()));
            Assert.assertEquals(SEV, record.getAsString(SyslogAttributes.SEVERITY.key()));
            Assert.assertEquals(FAC, record.getAsString(SyslogAttributes.FACILITY.key()));
            Assert.assertEquals(TIME, record.getAsString(SyslogAttributes.TIMESTAMP.key()));
            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParseMultipleLine() throws IOException, MalformedRecordException {
        try (final InputStream fis = new ByteArrayInputStream((VALID_MESSAGE_RFC3164_0 + VALID_MESSAGE_RFC3164_1 + VALID_MESSAGE_RFC3164_2).getBytes(CHARSET))) {
            SyslogParser parser = new SyslogParser(CHARSET);
            final SyslogRecordReader deserializer = new SyslogRecordReader(parser, fis, SyslogReader.createRecordSchema());

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
        try (final InputStream fis = new ByteArrayInputStream((VALID_MESSAGE_RFC3164_0 + "\n" + VALID_MESSAGE_RFC3164_1 + VALID_MESSAGE_RFC3164_2).getBytes(CHARSET))) {
            SyslogParser parser = new SyslogParser(CHARSET);
            final SyslogRecordReader deserializer = new SyslogRecordReader(parser, fis, SyslogReader.createRecordSchema());

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

    @Test
    @SuppressWarnings("unchecked")
    public void testParseSingleLine5424() throws IOException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/syslog/syslog5424/log_all.txt"))) {
            SyslogParser parser = new SyslogParser(CHARSET);
            final SyslogRecordReader deserializer = new SyslogRecordReader(parser, fis, SyslogReader.createRecordSchema());

            final Record record = deserializer.nextRecord();
            assertNotNull(record.getValues());

            Assert.assertEquals(expectedVersion, record.getAsString(SyslogAttributes.VERSION.key()));
            Assert.assertEquals(expectedMessage, record.getAsString(SyslogAttributes.BODY.key()));
            Assert.assertEquals(expectedHostName, record.getAsString(SyslogAttributes.HOSTNAME.key()));
            Assert.assertEquals(expectedPri, record.getAsString(SyslogAttributes.PRIORITY.key()));
            Assert.assertEquals(expectedSeverity, record.getAsString(SyslogAttributes.SEVERITY.key()));
            Assert.assertEquals(expectedFacility, record.getAsString(SyslogAttributes.FACILITY.key()));
            Assert.assertEquals(expectedTimestamp, record.getAsString(SyslogAttributes.TIMESTAMP.key()));

            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }

    public void writeSchema() {
        String s = SyslogReader.createRecordSchema().toString();
        System.out.println(s);
        System.out.println(AvroTypeUtil.extractAvroSchema( SyslogReader.createRecordSchema() ).toString(true));
    }
}
