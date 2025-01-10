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

import org.apache.nifi.syslog.events.SyslogEvent;
import org.apache.nifi.syslog.parsers.SyslogParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSyslogParser {

    static final Charset CHARSET = StandardCharsets.UTF_8;

    private SyslogParser parser;

    @BeforeEach
    public void setup() {
        parser = new SyslogParser(CHARSET);
    }

    @Test
    public void testRFC3164SingleDigitDay() {
        final String pri = "10";
        final String stamp = "Oct  1 13:14:04";
        final String host = "my.host.com";
        final String body = "some body message";
        final String message = "<" + pri + ">" + stamp + " " + host + " " + body;

        final byte[] bytes = message.getBytes(CHARSET);
        final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.clear();
        buffer.put(bytes);

        final SyslogEvent event = parser.parseEvent(buffer);
        assertNotNull(event);
        assertEquals(pri, event.getPriority());
        assertEquals("2", event.getSeverity());
        assertEquals("1", event.getFacility());
        assertNull(event.getVersion());
        assertEquals(stamp, event.getTimeStamp());
        assertEquals(host, event.getHostName());
        assertEquals(body, event.getMsgBody());
        assertEquals(message, event.getFullMessage());
        assertTrue(event.isValid());
    }

    @Test
    public void testRFC3164DoubleDigitDay() {
        final String pri = "31";
        final String stamp = "Oct 13 14:14:43";
        final String host = "localhost";
        final String body = "AppleCameraAssistant[470]: DeviceMessageNotificationCallback: kIOPMMessageSystemPowerEventOccurred: 0x00000000";
        final String message = "<" + pri + ">" + stamp + " " + host + " " + body;

        final byte[] bytes = message.getBytes(CHARSET);
        final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.clear();
        buffer.put(bytes);

        final SyslogEvent event = parser.parseEvent(buffer);
        assertNotNull(event);
        assertEquals(pri, event.getPriority());
        assertEquals("7", event.getSeverity());
        assertEquals("3", event.getFacility());
        assertNull(event.getVersion());
        assertEquals(stamp, event.getTimeStamp());
        assertEquals(host, event.getHostName());
        assertEquals(body, event.getMsgBody());
        assertEquals(message, event.getFullMessage());
        assertTrue(event.isValid());
    }

    @Test
    public void testRFC3164WithVersion() {
        final String pri = "31";
        final String version = "1";
        final String stamp = "Oct 13 14:14:43";
        final String host = "localhost";
        final String body = "AppleCameraAssistant[470]: DeviceMessageNotificationCallback: kIOPMMessageSystemPowerEventOccurred: 0x00000000";
        final String message = "<" + pri + ">" + version + " " + stamp + " " + host + " " + body;

        final byte[] bytes = message.getBytes(CHARSET);
        final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.clear();
        buffer.put(bytes);

        final SyslogEvent event = parser.parseEvent(buffer);
        assertNotNull(event);
        assertEquals(pri, event.getPriority());
        assertEquals("7", event.getSeverity());
        assertEquals("3", event.getFacility());
        assertEquals(version, event.getVersion());
        assertEquals(stamp, event.getTimeStamp());
        assertEquals(host, event.getHostName());
        assertEquals(body, event.getMsgBody());
        assertEquals(message, event.getFullMessage());
        assertTrue(event.isValid());
    }

    @Test
    public void testRFC5424WithVersion() {
        final String pri = "34";
        final String version = "1";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String body = "su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8";

        final String message = "<" + pri + ">" + version + " " + stamp + " " + host + " " + body;

        final byte[] bytes = message.getBytes(CHARSET);
        final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.clear();
        buffer.put(bytes);

        final SyslogEvent event = parser.parseEvent(buffer);
        assertNotNull(event);
        assertEquals(pri, event.getPriority());
        assertEquals("2", event.getSeverity());
        assertEquals("4", event.getFacility());
        assertEquals(version, event.getVersion());
        assertEquals(stamp, event.getTimeStamp());
        assertEquals(host, event.getHostName());
        assertEquals(body, event.getMsgBody());
        assertEquals(message, event.getFullMessage());
        assertTrue(event.isValid());
    }

    @Test
    public void testRFC5424WithoutVersion() {
        final String pri = "34";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String body = "su - ID47 - BOM'su root' failed for lonvick on /dev/pts/8";

        final String message = "<" + pri + ">" + stamp + " " + host + " " + body;

        final byte[] bytes = message.getBytes(CHARSET);
        final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.clear();
        buffer.put(bytes);

        final SyslogEvent event = parser.parseEvent(buffer);
        assertNotNull(event);
        assertEquals(pri, event.getPriority());
        assertEquals("2", event.getSeverity());
        assertEquals("4", event.getFacility());
        assertNull(event.getVersion());
        assertEquals(stamp, event.getTimeStamp());
        assertEquals(host, event.getHostName());
        assertEquals(body, event.getMsgBody());
        assertEquals(message, event.getFullMessage());
        assertTrue(event.isValid());
    }

    @Test
    public void testTrailingNewLine() {
        final String message = "<31>Oct 13 15:43:23 localhost.home some message\n";

        final byte[] bytes = message.getBytes(CHARSET);
        final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.clear();
        buffer.put(bytes);

        final SyslogEvent event = parser.parseEvent(buffer);
        assertNotNull(event);
        assertTrue(event.isValid());
    }

    @Test
    public void testVariety() {
        final List<String> messages = new ArrayList<>();

        // supported examples from RFC 3164
        messages.add("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for " +
                "lonvick on /dev/pts/8");
        messages.add("<13>Feb  5 17:32:18 10.0.0.99 Use the BFG!");
        messages.add("<165>Aug 24 05:34:00 CST 1987 mymachine myproc[10]: %% " +
                "It's time to make the do-nuts.  %%  Ingredients: Mix=OK, Jelly=OK # " +
                "Devices: Mixer=OK, Jelly_Injector=OK, Frier=OK # Transport: " +
                "Conveyer1=OK, Conveyer2=OK # %%");
        messages.add("<0>Oct 22 10:52:12 scapegoat 1990 Oct 22 10:52:01 TZ-6 " +
                "scapegoat.dmz.example.org 10.1.2.3 sched[0]: That's All Folks!");

        // supported examples from RFC 5424
        messages.add("<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - " +
                "ID47 - BOM'su root' failed for lonvick on /dev/pts/8");
        messages.add("<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc " +
                "8710 - - %% It's time to make the do-nuts.");

        // non-standard (but common) messages (RFC3339 dates, no version digit)
        messages.add("<13>2003-08-24T05:14:15Z localhost snarf?");
        messages.add("<13>2012-08-16T14:34:03-08:00 127.0.0.1 test shnap!");

        for (final String message : messages) {
            final byte[] bytes = message.getBytes(CHARSET);
            final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
            buffer.clear();
            buffer.put(bytes);

            final SyslogEvent event = parser.parseEvent(buffer);
            assertTrue(event.isValid());
        }
    }

    @Test
    public void testInvalidPriority() {
        final String message = "10 Oct 13 14:14:43 localhost some body of the message";

        final byte[] bytes = message.getBytes(CHARSET);
        final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.clear();
        buffer.put(bytes);

        final SyslogEvent event = parser.parseEvent(buffer);
        assertNotNull(event);
        assertFalse(event.isValid());
        assertEquals(message, event.getFullMessage());
    }

    @Test
    public void testParseWithSender() {
        final String sender = "127.0.0.1";
        final String message = "<31>Oct 13 15:43:23 localhost.home some message\n";

        final byte[] bytes = message.getBytes(CHARSET);
        final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.clear();
        buffer.put(bytes);

        final SyslogEvent event = parser.parseEvent(buffer, sender);
        assertNotNull(event);
        assertTrue(event.isValid());
        assertEquals(sender, event.getSender());
    }
}
