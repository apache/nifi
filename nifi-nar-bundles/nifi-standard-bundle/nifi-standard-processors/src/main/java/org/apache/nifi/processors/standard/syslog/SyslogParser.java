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
package org.apache.nifi.processors.standard.syslog;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a Syslog message from a ByteBuffer into a SyslogEvent instance.
 *
 * The Syslog regular expressions below were adapted from the Apache Flume project.
 */
public class SyslogParser {

    public static final String SYSLOG_MSG_RFC5424_0 =
            "(?:\\<(\\d{1,3})\\>)" + // priority
                    "(?:(\\d)?\\s?)" + // version
      /* yyyy-MM-dd'T'HH:mm:ss.SZ or yyyy-MM-dd'T'HH:mm:ss.S+hh:mm or - (null stamp) */
                    "(?:" +
                    "(\\d{4}[-]\\d{2}[-]\\d{2}[T]\\d{2}[:]\\d{2}[:]\\d{2}" +
                    "(?:\\.\\d{1,6})?(?:[+-]\\d{2}[:]\\d{2}|Z)?)|-)" + // stamp
                    "\\s" + // separator
                    "(?:([\\w][\\w\\d\\.@\\-]*)|-)" + // host name or - (null)
                    "\\s" + // separator
                    "(.*)$"; // body

    public static final String SYSLOG_MSG_RFC3164_0 =
            "(?:\\<(\\d{1,3})\\>)" +
                    "(?:(\\d)?\\s?)" + // version
                    // stamp MMM d HH:mm:ss, single digit date has two spaces
                    "([A-Z][a-z][a-z]\\s{1,2}\\d{1,2}\\s\\d{2}[:]\\d{2}[:]\\d{2})" +
                    "\\s" + // separator
                    "([\\w][\\w\\d(\\.|\\:)@-]*)" + // host
                    "\\s(.*)$";  // body

    public static final Collection<Pattern> MESSAGE_PATTERNS;
    static {
        List<Pattern> patterns = new ArrayList<>();
        patterns.add(Pattern.compile(SYSLOG_MSG_RFC5424_0));
        patterns.add(Pattern.compile(SYSLOG_MSG_RFC3164_0));
        MESSAGE_PATTERNS = Collections.unmodifiableList(patterns);
    }

    // capture group positions from the above message patterns
    public static final int SYSLOG_PRIORITY_POS = 1;
    public static final int SYSLOG_VERSION_POS = 2;
    public static final int SYSLOG_TIMESTAMP_POS = 3;
    public static final int SYSLOG_HOSTNAME_POS = 4;
    public static final int SYSLOG_BODY_POS = 5;

    private Charset charset;

    public SyslogParser() {
        this(StandardCharsets.UTF_8);
    }

    public SyslogParser(final Charset charset) {
        this.charset = charset;
    }

    /**
     *  Parses a SyslogEvent from a byte buffer.
     *
     * @param buffer a byte buffer containing a syslog message
     * @return a SyslogEvent parsed from the byte array
     */
    public SyslogEvent parseEvent(final ByteBuffer buffer) {
        return parseEvent(buffer, null);
    }

    /**
     *  Parses a SyslogEvent from a byte buffer.
     *
     * @param buffer a byte buffer containing a syslog message
     * @param sender the hostname of the syslog server that sent the message
     * @return a SyslogEvent parsed from the byte array
     */
    public SyslogEvent parseEvent(final ByteBuffer buffer, final String sender) {
        if (buffer == null) {
            return null;
        }
        if (buffer.position() != 0) {
            buffer.flip();
        }
        byte bytes[] = new byte[buffer.limit()];
        buffer.get(bytes, 0, buffer.limit());
        return parseEvent(bytes, sender);
    }

    /**
     * Parses a SyslogEvent from a byte array.
     *
     * @param bytes a byte array containing a syslog message
     * @param sender the hostname of the syslog server that sent the message
     * @return a SyslogEvent parsed from the byte array
     */
    public SyslogEvent parseEvent(final byte[] bytes, final String sender) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        // remove trailing new line before parsing
        int length = bytes.length;
        if (bytes[length - 1] == '\n') {
            length = length - 1;
        }

        final String message = new String(bytes, 0, length, charset);

        final SyslogEvent.Builder builder = new SyslogEvent.Builder()
                .valid(false).fullMessage(message).rawMessage(bytes).sender(sender);

        for (Pattern pattern : MESSAGE_PATTERNS) {
            final Matcher matcher = pattern.matcher(message);
            if (!matcher.matches()) {
                continue;
            }

            final MatchResult res = matcher.toMatchResult();
            for (int grp = 1; grp <= res.groupCount(); grp++) {
                String value = res.group(grp);
                if (grp == SYSLOG_TIMESTAMP_POS) {
                    builder.timestamp(value);
                } else if (grp == SYSLOG_HOSTNAME_POS) {
                    builder.hostname(value);
                } else if (grp == SYSLOG_PRIORITY_POS) {
                    int pri = Integer.parseInt(value);
                    int sev = pri % 8;
                    int facility = pri / 8;
                    builder.priority(value);
                    builder.severity(String.valueOf(sev));
                    builder.facility(String.valueOf(facility));
                } else if (grp == SYSLOG_VERSION_POS) {
                    builder.version(value);
                } else if (grp == SYSLOG_BODY_POS) {
                    builder.msgBody(value);
                }
            }

            builder.valid(true);
            break;
        }

        // either invalid w/original msg, or fully parsed event
        return builder.build();
    }

    public String getCharsetName() {
        return charset == null ? StandardCharsets.UTF_8.name() : charset.name();
    }
}
