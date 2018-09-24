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

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.events.Syslog5424Event;
import org.apache.nifi.syslog.parsers.StrictSyslog5424Parser;
import org.apache.nifi.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class Syslog5424RecordReader implements RecordReader {
    private final BufferedReader reader;
    private RecordSchema schema;
    private final StrictSyslog5424Parser parser;

    public Syslog5424RecordReader(StrictSyslog5424Parser parser, InputStream in, RecordSchema schema){
        this.reader = new BufferedReader(new InputStreamReader(in));
        this.schema = schema;
        this.parser = parser;
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {
        String line = reader.readLine();

        if ( line == null ) {
            // a null return from readLine() signals the end of the stream
            return null;
        }

        if (StringUtils.isBlank(line)) {
            // while an empty string is an error
            throw new MalformedRecordException("Encountered a blank message!");
        }


        final MalformedRecordException malformedRecordException;
        Syslog5424Event event = parser.parseEvent(ByteBuffer.wrap(line.getBytes(parser.getCharsetName())));

        if (!event.isValid()) {
            if (event.getException() != null) {
                malformedRecordException = new MalformedRecordException(
                        String.format("Failed to parse %s as a Syslog message: it does not conform to any of the RFC "+
                                "formats supported", line), event.getException());
            } else {
                malformedRecordException = new MalformedRecordException(
                        String.format("Failed to parse %s as a Syslog message: it does not conform to any of the RFC" +
                                " formats supported", line));
            }
            throw malformedRecordException;
        }

        Map<String,Object> modifiedMap = new HashMap<>(event.getFieldMap());
        modifiedMap.put(SyslogAttributes.TIMESTAMP.key(),convertTimeStamp((String)event.getFieldMap().get(SyslogAttributes.TIMESTAMP.key())));

        return new MapRecord(schema,modifiedMap);
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return schema;
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    private Timestamp convertTimeStamp(String timeString) {
        /*
        From RFC 5424: https://tools.ietf.org/html/rfc5424#page-11

        The TIMESTAMP field is a formalized timestamp derived from [RFC3339].

                Whereas [RFC3339] makes allowances for multiple syntaxes, this
        document imposes further restrictions.  The TIMESTAMP value MUST
        follow these restrictions:

        o  The "T" and "Z" characters in this syntax MUST be upper case.

        o  Usage of the "T" character is REQUIRED.

        o  Leap seconds MUST NOT be used.
        */

        if (timeString == null) {
            return null;
        }
        return Timestamp.from(Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(timeString)));
    }
}
