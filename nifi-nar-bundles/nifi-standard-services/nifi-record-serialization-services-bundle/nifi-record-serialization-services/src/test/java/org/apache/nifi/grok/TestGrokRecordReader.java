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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.Record;
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

            final GrokRecordReader deserializer = new GrokRecordReader(fis, grok, GrokReader.createRecordSchema(grok), true);

            final String[] logLevels = new String[] {"INFO", "WARN", "ERROR", "FATAL", "FINE"};
            final String[] messages = new String[] {"Test Message 1", "Red", "Green", "Blue", "Yellow"};

            for (int i = 0; i < logLevels.length; i++) {
                final Object[] values = deserializer.nextRecord().getValues();

                assertNotNull(values);
                assertEquals(4, values.length); // values[] contains 4 elements: timestamp, level, message, STACK_TRACE
                assertEquals("2016-11-08 21:24:23,029", values[0]);
                assertEquals(logLevels[i], values[1]);
                assertEquals(messages[i], values[2]);
                assertNull(values[3]);
            }

            assertNull(deserializer.nextRecord());
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
        final GrokRecordReader deserializer = new GrokRecordReader(bais, grok, GrokReader.createRecordSchema(grok), true);

        final Object[] values = deserializer.nextRecord().getValues();

        assertNotNull(values);
        assertEquals(6, values.length); // values[] contains 4 elements: timestamp, level, message, STACK_TRACE
        assertEquals("2016-08-04 13:26:32,473", values[0]);
        assertEquals("INFO", values[1]);
        assertEquals("Leader Election Notification Thread-1", values[2]);
        assertEquals("o.a.n.LoggerClass", values[3]);
        assertEquals("", values[4]);
        assertEquals("org.apache.nifi.exception.UnitTestException: Testing to ensure we are able to capture stack traces", values[5]);
    }



    @Test
    public void testParseNiFiSampleLog() throws IOException, GrokException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/grok/nifi-log-sample.log"))) {
            final Grok grok = new Grok();
            grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
            grok.compile("%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} \\[%{DATA:thread}\\] %{DATA:class} %{GREEDYDATA:message}");

            final GrokRecordReader deserializer = new GrokRecordReader(fis, grok, GrokReader.createRecordSchema(grok), true);

            final String[] logLevels = new String[] {"INFO", "INFO", "INFO", "WARN", "WARN"};

            for (int i = 0; i < logLevels.length; i++) {
                final Object[] values = deserializer.nextRecord().getValues();

                assertNotNull(values);
                assertEquals(6, values.length); // values[] contains 6 elements: timestamp, level, thread, class, message, STACK_TRACE
                assertEquals(logLevels[i], values[1]);
                assertNull(values[5]);
            }

            assertNull(deserializer.nextRecord());
        }
    }

    @Test
    public void testParseNiFiSampleMultilineWithStackTrace() throws IOException, GrokException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/grok/nifi-log-sample-multiline-with-stacktrace.log"))) {
            final Grok grok = new Grok();
            grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
            grok.compile("%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} \\[%{DATA:thread}\\] %{DATA:class} %{GREEDYDATA:message}?");

            final GrokRecordReader deserializer = new GrokRecordReader(fis, grok, GrokReader.createRecordSchema(grok), true);

            final String[] logLevels = new String[] {"INFO", "INFO", "ERROR", "WARN", "WARN"};

            for (int i = 0; i < logLevels.length; i++) {
                final Record record = deserializer.nextRecord();
                final Object[] values = record.getValues();

                assertNotNull(values);
                assertEquals(6, values.length); // values[] contains 6 elements: timestamp, level, thread, class, message, STACK_TRACE
                assertEquals(logLevels[i], values[1]);
                if ("ERROR".equals(values[1])) {
                    final String msg = (String) values[4];
                    assertEquals("One\nTwo\nThree", msg);
                    assertNotNull(values[5]);
                } else {
                    assertNull(values[5]);
                }
            }

            assertNull(deserializer.nextRecord());
        }
    }


    @Test
    public void testParseStackTrace() throws GrokException, IOException, MalformedRecordException {
        try (final InputStream fis = new FileInputStream(new File("src/test/resources/grok/error-with-stack-trace.log"))) {
            final Grok grok = new Grok();
            grok.addPatternFromFile("src/main/resources/default-grok-patterns.txt");
            grok.compile("%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}");

            final GrokRecordReader deserializer = new GrokRecordReader(fis, grok, GrokReader.createRecordSchema(grok), true);

            final String[] logLevels = new String[] {"INFO", "ERROR", "INFO"};
            final String[] messages = new String[] {"message without stack trace",
                "Log message with stack trace",
                "message without stack trace"};

            for (int i = 0; i < logLevels.length; i++) {
                final Object[] values = deserializer.nextRecord().getValues();

                assertNotNull(values);
                assertEquals(4, values.length); // values[] contains 4 elements: timestamp, level, message, STACK_TRACE
                assertEquals(logLevels[i], values[1]);
                assertEquals(messages[i], values[2]);

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
                }
            }

            assertNull(deserializer.nextRecord());
        }
    }

}
