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

import org.apache.nifi.syslog.attributes.Syslog5424Attributes;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.events.Syslog5424Event;
import org.apache.nifi.syslog.keyproviders.SyslogPrefixedKeyProvider;
import org.apache.nifi.syslog.parsers.StrictSyslog5424Parser;
import org.apache.nifi.syslog.utils.NifiStructuredDataPolicy;
import org.apache.nifi.syslog.utils.NilHandlingPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseStrictSyslog5424ParserTest {

    private StrictSyslog5424Parser parser;

    protected abstract NilHandlingPolicy getPolicy();

    @BeforeEach
    public void setup() {
        parser = new StrictSyslog5424Parser(getPolicy(), NifiStructuredDataPolicy.FLATTEN, new SyslogPrefixedKeyProvider());
    }

    @Test
    public void testRFC5424WithVersion() {
        final String pri = "34";
        final String version = "1";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String appName = "su";
        final String procId = "-";
        final String msgId = "ID17";
        final String body = "'su root' failed for lonvick on /dev/pts/8";

        final String message = "<" + pri + ">" + version + " " + stamp + " " + host + " "
                + appName + " " + procId + " " + msgId + " " + "-" + " " + body;

        final Syslog5424Event event = parser.parseEvent(message);
        assertNotNull(event);
        assertTrue(event.isValid());
        assertFalse(event.getFieldMap().isEmpty());
        Map<String, Object> fieldMap = event.getFieldMap();
        assertEquals(pri, fieldMap.get(SyslogAttributes.SYSLOG_PRIORITY.key()));
        assertEquals("2", fieldMap.get(SyslogAttributes.SYSLOG_SEVERITY.key()));
        assertEquals("4", fieldMap.get(SyslogAttributes.SYSLOG_FACILITY.key()));
        assertEquals(version, fieldMap.get(SyslogAttributes.SYSLOG_VERSION.key()));
        assertEquals(stamp, fieldMap.get(SyslogAttributes.SYSLOG_TIMESTAMP.key()));
        assertEquals(host, fieldMap.get(SyslogAttributes.SYSLOG_HOSTNAME.key()));
        assertEquals(appName, fieldMap.get(Syslog5424Attributes.SYSLOG_APP_NAME.key()));
        assertEquals(msgId, fieldMap.get(Syslog5424Attributes.SYSLOG_MESSAGEID.key()));

        Pattern structuredPattern = new SyslogPrefixedKeyProvider().getStructuredElementIdParamNamePattern();
        fieldMap.forEach((key, value) -> {
            if (value != null) {
                assertFalse(structuredPattern.matcher(key).matches());
            }
        });

        assertEquals(body, fieldMap.get(SyslogAttributes.SYSLOG_BODY.key()));
        assertEquals(message, event.getFullMessage());
        assertNull(event.getSender());
    }

    @Test
    public void testRFC5424WithoutVersion() {
        final String pri = "34";
        final String version = "-";
        final String stamp = "2003-10-11T22:14:15.003Z";
        final String host = "mymachine.example.com";
        final String appName = "su";
        final String procId = "-";
        final String msgId = "ID17";
        final String body = "'su root' failed for lonvick on /dev/pts/8";

        final String message = "<" + pri + ">" + version + " " + stamp + " " + host + " "
                + appName + " " + procId + " " + msgId + " " + "-" + " " + body;

        final Syslog5424Event event = parser.parseEvent(message);
        assertFalse(event.isValid());
    }

    @Test
    public void testTrailingNewLine() {
        final String message = "<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - " +
                "ID47 - 'su root' failed for lonvick on /dev/pts/8\n";

        final Syslog5424Event event = parser.parseEvent(message);
        assertNotNull(event);
        assertTrue(event.isValid());
    }

    @Test
    public void testVariety() {
        final List<String> messages = new ArrayList<>();

        // supported examples from RFC 5424 including structured data with no message
        messages.add("<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - " +
                "ID47 - 'su root' failed for lonvick on /dev/pts/8");
        messages.add("<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc " +
                "8710 - - %% It's time to make the do-nuts.");
        messages.add("<14>1 2014-06-20T09:14:07+00:00 loggregator"
                + " d0602076-b14a-4c55-852a-981e7afeed38 DEA MSG-01"
                + " [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]"
                + "[exampleSDID@32480 iut=\"4\" eventSource=\"Other Application\" eventID=\"2022\"] Removing instance");

        for (final String message : messages) {
            final Syslog5424Event event = parser.parseEvent(message);
            assertTrue(event.isValid());
        }
    }

    @Test
    public void testMessagePartNotRequired() {
        final List<String> messages = new ArrayList<>();

        messages.add("<14>1 2014-06-20T09:14:07+00:00 loggregator"
            + " d0602076-b14a-4c55-852a-981e7afeed38 DEA MSG-01"
            + " [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]");

        messages.add("<14>1 2014-06-20T09:14:07+00:00 loggregator"
            + " d0602076-b14a-4c55-852a-981e7afeed38 DEA MSG-01"
            + " [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]"
            + "[exampleSDID@32480 iut=\"4\" eventSource=\"Other Application\" eventID=\"2022\"]");

        for (final String message : messages) {
            final Syslog5424Event event = parser.parseEvent(message);
            assertTrue(event.isValid());
            assertNull(event.getFieldMap().get(SyslogAttributes.SYSLOG_BODY.key()));
        }
    }

    @Test
    public void testInvalidPriority() {
        final String message = "10 Oct 13 14:14:43 localhost some body of the message";

        final Syslog5424Event event = parser.parseEvent(message);
        assertNotNull(event);
        assertFalse(event.isValid());
        assertEquals(message, event.getFullMessage());
    }

    @Test
    public void testParseWithBOM() {
        final String message = "<14>1 2014-06-20T09:14:07+00:00 loggregator"
            + " d0602076-b14a-4c55-852a-981e7afeed38 DEA MSG-01"
            + " [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]"
            + "[exampleSDID@32480 iut=\"4\" eventSource=\"Other Application\" eventID=\"2022\"] \uFEFFMessage with some Umlauts äöü";

        final Syslog5424Event event = parser.parseEvent(message);
        assertNotNull(event);
        assertTrue(event.isValid());
        assertEquals("Message with some Umlauts äöü", event.getFieldMap().get(SyslogAttributes.SYSLOG_BODY.key()));
    }
}


