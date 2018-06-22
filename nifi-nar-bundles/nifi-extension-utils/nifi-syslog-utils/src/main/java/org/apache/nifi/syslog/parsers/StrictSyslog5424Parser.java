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
package org.apache.nifi.syslog.parsers;

import com.github.palindromicity.syslog.KeyProvider;
import com.github.palindromicity.syslog.NilPolicy;
import com.github.palindromicity.syslog.StructuredDataPolicy;
import com.github.palindromicity.syslog.SyslogParserBuilder;
import org.apache.nifi.syslog.events.Syslog5424Event;
import org.apache.nifi.syslog.keyproviders.SyslogPrefixedKeyProvider;
import org.apache.nifi.syslog.utils.NifiStructuredDataPolicy;
import org.apache.nifi.syslog.utils.NilHandlingPolicy;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Parses a Syslog message from a ByteBuffer into a Syslog5424Event instance.
 * For 5424 we use simple-syslog-5424 since it parsers out structured data.
 */
public class StrictSyslog5424Parser {
    private Charset charset;
    private com.github.palindromicity.syslog.SyslogParser parser;

    public StrictSyslog5424Parser() {
        this(StandardCharsets.UTF_8, NilHandlingPolicy.NULL, NifiStructuredDataPolicy.FLATTEN, new SyslogPrefixedKeyProvider());
    }

    public StrictSyslog5424Parser(final Charset charset, final NilHandlingPolicy nilPolicy,
                                  NifiStructuredDataPolicy structuredDataPolicy, KeyProvider keyProvider) {
        this.charset = charset;
        parser = new SyslogParserBuilder()
                .withNilPolicy(NilPolicy.valueOf(nilPolicy.name()))
                .withStructuredDataPolicy(StructuredDataPolicy.valueOf(structuredDataPolicy.name()))
                .withKeyProvider(keyProvider)
                .build();
    }

    /**
     * Parses a Syslog5424Event from a {@code ByteBuffer}.
     *
     * @param buffer a {@code ByteBuffer} containing a syslog message
     * @return a Syslog5424Event parsed from the {@code {@code byte array}}
     */
    public Syslog5424Event parseEvent(final ByteBuffer buffer) {
        return parseEvent(buffer, null);
    }

    /**
     * Parses a Syslog5424Event from a {@code ByteBuffer}.
     *
     * @param buffer a {@code ByteBuffer} containing a syslog message
     * @param sender the hostname of the syslog server that sent the message
     * @return a Syslog5424Event parsed from the {@code byte array}
     */
    public Syslog5424Event parseEvent(final ByteBuffer buffer, final String sender) {
        if (buffer == null) {
            return null;
        }
        return parseEvent(bufferToBytes(buffer), sender);
    }

    /**
     * Parses a Syslog5424Event from a {@code byte array}.
     *
     * @param bytes  a {@code byte array} containing a syslog message
     * @param sender the hostname of the syslog server that sent the message
     * @return a Syslog5424Event parsed from the {@code byte array}
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
            parser.parseLine(message, builder::fieldMap);
            builder.valid(true);
        } catch (Exception e) {
            // this is not a valid 5424 message
            builder.valid(false);
            builder.exception(e);
        }

        // either invalid w/original msg, or fully parsed event
        return builder.build();
    }

    public String getCharsetName() {
        return charset == null ? StandardCharsets.UTF_8.name() : charset.name();
    }


    private byte[] bufferToBytes(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        if (buffer.position() != 0) {
            buffer.flip();
        }
        byte bytes[] = new byte[buffer.limit()];
        buffer.get(bytes, 0, buffer.limit());
        return bytes;
    }
}
