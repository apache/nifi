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

import com.github.palindromicity.syslog.KeyProvider;
import com.github.palindromicity.syslog.NilPolicy;
import com.github.palindromicity.syslog.SyslogParserBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Parses a Syslog message from a ByteBuffer into a Syslog5424Event instance.
 * For 5424 we use simple-syslog-5424 since it parsers out structured data.
 */
public class StrictSyslog5424Parser {
    private Charset charset;
    private com.github.palindromicity.syslog.SyslogParser parser;

    public StrictSyslog5424Parser() {
        this(StandardCharsets.UTF_8, NilPolicy.NULL);
    }

    public StrictSyslog5424Parser(final Charset charset, final NilPolicy nilPolicy) {
        this.charset = charset;
        parser = new SyslogParserBuilder()
                .withNilPolicy(nilPolicy)
                .withKeyProvider(new NifiKeyProvider())
                .build();
    }

    /**
     *  Parses a Syslog5424Event from a byte buffer.
     *
     * @param buffer a byte buffer containing a syslog message
     * @return a Syslog5424Event parsed from the byte array
     */
    public Syslog5424Event parseEvent(final ByteBuffer buffer) {
        return parseEvent(buffer, null);
    }

    /**
     *  Parses a Syslog5424Event from a byte buffer.
     *
     * @param buffer a byte buffer containing a syslog message
     * @param sender the hostname of the syslog server that sent the message
     * @return a Syslog5424Event parsed from the byte array
     */
    public Syslog5424Event parseEvent(final ByteBuffer buffer, final String sender) {
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
     * Parses a Syslog5424Event from a byte array.
     *
     * @param bytes a byte array containing a syslog message
     * @param sender the hostname of the syslog server that sent the message
     * @return a Syslog5424Event parsed from the byte array
     */
    public Syslog5424Event parseEvent(final byte[] bytes, final String sender) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        // remove trailing new line before parsing
        int length = bytes.length;
        if (bytes[length - 1] == '\n') {
            length = length - 1;
        }

        final String message = new String(bytes, 0, length, charset);

        final Syslog5424Event.Builder builder = new Syslog5424Event.Builder()
                .valid(false).fullMessage(message).rawMessage(bytes).sender(sender);

        try {
            parser.parseLine(message,(map)-> {
                builder.fieldMap(convertMap(map));
            });
            builder.valid(true);
            return builder.build();
        } catch (Exception e) {
            // this is not a valid 5424 message
            builder.valid(false);
        }

        // either invalid w/original msg, or fully parsed event
        return builder.build();
    }

    public String getCharsetName() {
        return charset == null ? StandardCharsets.UTF_8.name() : charset.name();
    }

    private static Map<String,String> convertMap(Map<String, Object> map) {
        Map<String,String> returnMap = new HashMap<>();
        map.forEach((key,value) -> returnMap.put(key,(String)value));
        return returnMap;
    }

    public static class NifiKeyProvider implements KeyProvider {
        private Pattern pattern;

        public NifiKeyProvider(){}

        @Override
        public String getMessage() {
            return SyslogAttributes.BODY.key();
        }

        @Override
        public String getHeaderAppName() {
            return Syslog5424Attributes.APP_NAME.key();
        }

        @Override
        public String getHeaderHostName() {
            return SyslogAttributes.HOSTNAME.key();
        }

        @Override
        public String getHeaderPriority() {
            return SyslogAttributes.PRIORITY.key();
        }

        @Override
        public String getHeaderFacility() {
            return SyslogAttributes.FACILITY.key();
        }

        @Override
        public String getHeaderSeverity() {
            return SyslogAttributes.SEVERITY.key();
        }


        @Override
        public String getHeaderProcessId() {
            return Syslog5424Attributes.PROCID.key();
        }

        @Override
        public String getHeaderTimeStamp() {
            return SyslogAttributes.TIMESTAMP.key();
        }

        @Override
        public String getHeaderMessageId() {
            return Syslog5424Attributes.MESSAGEID.key();
        }

        @Override
        public String getHeaderVersion() {
            return SyslogAttributes.VERSION.key();
        }

        @Override
        public String getStructuredBase() {
            return Syslog5424Attributes.STRUCTURED_BASE.key();
        }

        @Override
        public String getStructuredElementIdFormat() {
            return Syslog5424Attributes.STRUCTURED_ELEMENT_ID_FMT.key();
        }

        @Override
        public String getStructuredElementIdParamNameFormat() {
            return Syslog5424Attributes.STRUCTURED_ELEMENT_ID_PNAME_FMT.key();
        }

        @Override
        public Pattern getStructuredElementIdParamNamePattern() {
            if (pattern == null) {
                pattern = Pattern.compile(Syslog5424Attributes.STRUCTURED_ELEMENT_ID_PNAME_PATTERN.key());
            }
            return pattern;
        }
    }
}
