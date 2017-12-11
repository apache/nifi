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

package org.apache.nifi.grok;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Test;

import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.exception.GrokException;

public class TestGrokRecordReader {

    @Test
    public void testParseSingleLineLogMessages() throws GrokException, IOException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/grok/single-line-log-messages.txt"))) {
            final Grok grok = new Grok();
            grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
            grok.compile("%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}");

            final GrokRecordReader deserializer = new GrokRecordReader(fis, grok, GrokReader.createRecordSchema(grok), GrokReader.createRecordSchema(grok), true);

            final String[] logLevels = new String[] {"INFO", "WARN", "ERROR", "FATAL", "FINE"};
            final String[] messages = new String[] {"Test Message 1", "Red", "Green", "Blue", "Yellow"};
            final String[] rawMessages = new String[] {"2016-11-08 21:24:23,029 INFO Test Message 1",
                    "2016-11-08 21:24:23,029 WARN Red", "2016-11-08 21:24:23,029 ERROR Green",
                    "2016-11-08 21:24:23,029 FATAL Blue", "2016-11-08 21:24:23,029 FINE Yellow"};

            for (int i = 0; i < logLevels.length; i++) {
                final Object[] values = deserializer.nextRecord().getValues();

                assertNotNull(values);
                assertEquals(5, values.length); // values[] contains 4 elements: timestamp, level, message, STACK_TRACE, RAW_MESSAGE
                assertEquals("2016-11-08 21:24:23,029", values[0]);
                assertEquals(logLevels[i], values[1]);
                assertEquals(messages[i], values[2]);
                assertNull(values[3]);
                assertEquals(rawMessages[i], values[4]);
            }

            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }


    @Test
    public void testParseEmptyMessageWithStackTrace() throws GrokException, IOException, MalformedRecordException {
        final Grok grok = new Grok();
        grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
        grok.compile("%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} \\[%{DATA:thread}\\] %{DATA:class} %{GREEDYDATA:message}");

        final String msg = "2016-08-04 13:26:32,473 INFO [Leader Election Notification Thread-1] o.a.n.LoggerClass \n"
            + "org.apache.nifi.exception.UnitTestException: Testing to ensure we are able to capture stack traces";
        final InputStream bais = new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8));
        final GrokRecordReader deserializer = new GrokRecordReader(bais, grok, GrokReader.createRecordSchema(grok), GrokReader.createRecordSchema(grok), true);

        final Object[] values = deserializer.nextRecord().getValues();

        assertNotNull(values);
        assertEquals(7, values.length); // values[] contains 4 elements: timestamp, level, message, STACK_TRACE, RAW_MESSAGE
        assertEquals("2016-08-04 13:26:32,473", values[0]);
        assertEquals("INFO", values[1]);
        assertEquals("Leader Election Notification Thread-1", values[2]);
        assertEquals("o.a.n.LoggerClass", values[3]);
        assertEquals("", values[4]);
        assertEquals("org.apache.nifi.exception.UnitTestException: Testing to ensure we are able to capture stack traces", values[5]);
        assertEquals(msg, values[6]);

        deserializer.close();
    }



    @Test
    public void testParseNiFiSampleLog() throws IOException, GrokException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/grok/nifi-log-sample.log"))) {
            final Grok grok = new Grok();
            grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
            grok.compile("%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} \\[%{DATA:thread}\\] %{DATA:class} %{GREEDYDATA:message}");

            final GrokRecordReader deserializer = new GrokRecordReader(fis, grok, GrokReader.createRecordSchema(grok), GrokReader.createRecordSchema(grok), true);

            final String[] logLevels = new String[] {"INFO", "INFO", "INFO", "WARN", "WARN"};

            for (int i = 0; i < logLevels.length; i++) {
                final Object[] values = deserializer.nextRecord().getValues();

                assertNotNull(values);
                assertEquals(7, values.length); // values[] contains 6 elements: timestamp, level, thread, class, message, STACK_TRACE, RAW_MESSAGE
                assertEquals(logLevels[i], values[1]);
                assertNull(values[5]);
                assertNotNull(values[6]);
            }

            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }

    @Test
    public void testParseNiFiSampleMultilineWithStackTrace() throws IOException, GrokException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/grok/nifi-log-sample-multiline-with-stacktrace.log"))) {
            final Grok grok = new Grok();
            grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
            grok.compile("%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} \\[%{DATA:thread}\\] %{DATA:class} %{GREEDYDATA:message}?");

            final GrokRecordReader deserializer = new GrokRecordReader(fis, grok, GrokReader.createRecordSchema(grok), GrokReader.createRecordSchema(grok), true);

            final String[] logLevels = new String[] {"INFO", "INFO", "ERROR", "WARN", "WARN"};

            for (int i = 0; i < logLevels.length; i++) {
                final Record record = deserializer.nextRecord();
                final Object[] values = record.getValues();

                assertNotNull(values);
                assertEquals(7, values.length); // values[] contains 6 elements: timestamp, level, thread, class, message, STACK_TRACE, RAW_MESSAGE
                assertEquals(logLevels[i], values[1]);
                if ("ERROR".equals(values[1])) {
                    final String msg = (String) values[4];
                    assertEquals("One\nTwo\nThree", msg);
                    assertNotNull(values[5]);
                    assertTrue(values[6].toString().startsWith("2016-08-04 13:26:32,474 ERROR [Leader Election Notification Thread-2] o.apache.nifi.controller.FlowController One"));
                    assertTrue(values[6].toString().endsWith("    at org.apache.nifi.cluster."
                            + "coordination.node.NodeClusterCoordinator.getElectedActiveCoordinatorAddress(NodeClusterCoordinator.java:185) "
                            + "[nifi-framework-cluster-1.0.0-SNAPSHOT.jar:1.0.0-SNAPSHOT]\n    ... 12 common frames omitted"));
                } else {
                    assertNull(values[5]);
                }
            }

            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }


    @Test
    public void testParseStackTrace() throws GrokException, IOException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/grok/error-with-stack-trace.log"))) {
            final Grok grok = new Grok();
            grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
            grok.compile("%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}");

            final GrokRecordReader deserializer = new GrokRecordReader(fis, grok, GrokReader.createRecordSchema(grok), GrokReader.createRecordSchema(grok), true);

            final String[] logLevels = new String[] {"INFO", "ERROR", "INFO"};
            final String[] messages = new String[] {"message without stack trace",
                "Log message with stack trace",
                "message without stack trace"};

            for (int i = 0; i < logLevels.length; i++) {
                final Object[] values = deserializer.nextRecord().getValues();

                assertNotNull(values);
                assertEquals(5, values.length); // values[] contains 4 elements: timestamp, level, message, STACK_TRACE, RAW_MESSAGE
                assertEquals(logLevels[i], values[1]);
                assertEquals(messages[i], values[2]);
                assertNotNull(values[4]);

                if (values[1].equals("ERROR")) {
                    final String stackTrace = (String) values[3];
                    assertNotNull(stackTrace);
                    assertTrue(stackTrace.startsWith("org.apache.nifi.exception.UnitTestException: Testing to ensure we are able to capture stack traces"));
                    assertTrue(stackTrace.contains("        at org.apache.nifi.cluster.coordination.node.NodeClusterCoordinator.getElectedActiveCoordinatorAddress("
                        + "NodeClusterCoordinator.java:185) [nifi-framework-cluster-1.0.0-SNAPSHOT.jar:1.0.0-SNAPSHOT]"));
                    assertTrue(stackTrace.contains("Caused by: org.apache.nifi.exception.UnitTestException: Testing to ensure we are able to capture stack traces"));
                    assertTrue(stackTrace.contains("at org.apache.nifi.cluster.coordination.node.NodeClusterCoordinator.getElectedActiveCoordinatorAddress("
                        + "NodeClusterCoordinator.java:185) [nifi-framework-cluster-1.0.0-SNAPSHOT.jar:1.0.0-SNAPSHOT]"));
                    assertTrue(stackTrace.endsWith("    ... 12 common frames omitted"));

                    final String raw = (String) values[4];
                    assertTrue(raw.startsWith("2016-11-23 16:00:02,689 ERROR Log message with stack trace"));
                    assertTrue(raw.contains("org.apache.nifi.exception.UnitTestException: Testing to ensure we are able to capture stack traces"));
                    assertTrue(raw.contains("        at org.apache.nifi.cluster.coordination.node.NodeClusterCoordinator.getElectedActiveCoordinatorAddress("
                        + "NodeClusterCoordinator.java:185) [nifi-framework-cluster-1.0.0-SNAPSHOT.jar:1.0.0-SNAPSHOT]"));
                    assertTrue(raw.contains("Caused by: org.apache.nifi.exception.UnitTestException: Testing to ensure we are able to capture stack traces"));
                    assertTrue(raw.contains("at org.apache.nifi.cluster.coordination.node.NodeClusterCoordinator.getElectedActiveCoordinatorAddress("
                        + "NodeClusterCoordinator.java:185) [nifi-framework-cluster-1.0.0-SNAPSHOT.jar:1.0.0-SNAPSHOT]"));
                    assertTrue(raw.endsWith("    ... 12 common frames omitted"));
                }
            }

            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }

    @Test
    public void testInheritNamedParameters() throws FileNotFoundException, IOException, GrokException, MalformedRecordException {
        final String syslogMsg = "May 22 15:58:23 my-host nifi[12345]:My Message";
        final byte[] msgBytes = syslogMsg.getBytes();

        try (final InputStream in = new ByteArrayInputStream(msgBytes)) {
            final Grok grok = new Grok();
            grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
            grok.compile("%{SYSLOGBASE}%{GREEDYDATA:message}");

            final RecordSchema schema = GrokReader.createRecordSchema(grok);
            final List<String> fieldNames = schema.getFieldNames();
            assertEquals(9, fieldNames.size());
            assertTrue(fieldNames.contains("timestamp"));
            assertTrue(fieldNames.contains("logsource"));
            assertTrue(fieldNames.contains("facility"));
            assertTrue(fieldNames.contains("priority"));
            assertTrue(fieldNames.contains("program"));
            assertTrue(fieldNames.contains("pid"));
            assertTrue(fieldNames.contains("message"));
            assertTrue(fieldNames.contains("stackTrace"));  // always implicitly there
            assertTrue(fieldNames.contains("_raw"));  // always implicitly there

            final GrokRecordReader deserializer = new GrokRecordReader(in, grok, schema, schema, true);
            final Record record = deserializer.nextRecord();

            assertEquals("May 22 15:58:23", record.getValue("timestamp"));
            assertEquals("my-host", record.getValue("logsource"));
            assertNull(record.getValue("facility"));
            assertNull(record.getValue("priority"));
            assertEquals("nifi", record.getValue("program"));
            assertEquals("12345", record.getValue("pid"));
            assertEquals("My Message", record.getValue("message"));
            assertEquals("May 22 15:58:23 my-host nifi[12345]:My Message", record.getValue("_raw"));

            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }

    @Test
    public void testSkipUnmatchedRecordFirstLine() throws GrokException, IOException, MalformedRecordException {
        final String nonMatchingRecord = "hello there";
        final String matchingRecord = "1 2 3 4 5";

        final String input = nonMatchingRecord + "\n" + matchingRecord;
        final byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);

        try (final InputStream in = new ByteArrayInputStream(inputBytes)) {
            final Grok grok = new Grok();
            grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
            grok.compile("%{NUMBER:first} %{NUMBER:second} %{NUMBER:third} %{NUMBER:fourth} %{NUMBER:fifth}");

            final RecordSchema schema = GrokReader.createRecordSchema(grok);
            final List<String> fieldNames = schema.getFieldNames();
            assertEquals(7, fieldNames.size());
            assertTrue(fieldNames.contains("first"));
            assertTrue(fieldNames.contains("second"));
            assertTrue(fieldNames.contains("third"));
            assertTrue(fieldNames.contains("fourth"));
            assertTrue(fieldNames.contains("fifth"));

            final GrokRecordReader deserializer = new GrokRecordReader(in, grok, schema, schema, false);
            final Record record = deserializer.nextRecord();

            assertEquals("1", record.getValue("first"));
            assertEquals("2", record.getValue("second"));
            assertEquals("3", record.getValue("third"));
            assertEquals("4", record.getValue("fourth"));
            assertEquals("5", record.getValue("fifth"));

            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }

    @Test
    public void testSkipUnmatchedRecordMiddle() throws GrokException, IOException, MalformedRecordException {
        final String nonMatchingRecord = "hello there";
        final String matchingRecord = "1 2 3 4 5";

        final String input = matchingRecord + "\n" + nonMatchingRecord + "\n" + matchingRecord;
        final byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);

        try (final InputStream in = new ByteArrayInputStream(inputBytes)) {
            final Grok grok = new Grok();
            grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
            grok.compile("%{NUMBER:first} %{NUMBER:second} %{NUMBER:third} %{NUMBER:fourth} %{NUMBER:fifth}");

            final RecordSchema schema = GrokReader.createRecordSchema(grok);
            final List<String> fieldNames = schema.getFieldNames();
            assertEquals(7, fieldNames.size());
            assertTrue(fieldNames.contains("first"));
            assertTrue(fieldNames.contains("second"));
            assertTrue(fieldNames.contains("third"));
            assertTrue(fieldNames.contains("fourth"));
            assertTrue(fieldNames.contains("fifth"));

            final GrokRecordReader deserializer = new GrokRecordReader(in, grok, schema, schema, false);
            for (int i = 0; i < 2; i++) {
                final Record record = deserializer.nextRecord();

                assertEquals("1", record.getValue("first"));
                assertEquals("2", record.getValue("second"));
                assertEquals("3", record.getValue("third"));
                assertEquals("4", record.getValue("fourth"));
                assertEquals("5", record.getValue("fifth"));
            }

            assertNull(deserializer.nextRecord());
            deserializer.close();
        }
    }
}