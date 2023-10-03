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
import org.apache.nifi.syslog.utils.NifiStructuredDataPolicy;
import org.apache.nifi.syslog.utils.NilHandlingPolicy;

/**
 * Parses a Syslog message from a ByteBuffer into a Syslog5424Event instance.
 * For 5424 we use simple-syslog-5424 since it parsers out structured data.
 */
public class StrictSyslog5424Parser {
    private final com.github.palindromicity.syslog.SyslogParser parser;

    public StrictSyslog5424Parser(final NilHandlingPolicy nilPolicy,
                                  final NifiStructuredDataPolicy structuredDataPolicy, final KeyProvider keyProvider) {
        parser = new SyslogParserBuilder()
                .withNilPolicy(NilPolicy.valueOf(nilPolicy.name()))
                .withStructuredDataPolicy(StructuredDataPolicy.valueOf(structuredDataPolicy.name()))
                .withKeyProvider(keyProvider)
                .build();
    }

    /**
     * Parses a Syslog5424Event from a String
     *
     * @param line a {@code String} containing a syslog message
     * @return a Syslog5424Event parsed from the input line
     */
    public Syslog5424Event parseEvent(final String line) {
        final Syslog5424Event.Builder builder = new Syslog5424Event.Builder()
                .valid(false).fullMessage(line);

        try {
            parser.parseLine(line, builder::fieldMap);
            builder.valid(true);
        } catch (Exception e) {
            // this is not a valid 5424 message
            builder.valid(false);
            builder.exception(e);
        }

        // either invalid w/original msg, or fully parsed event
        return builder.build();
    }
}
