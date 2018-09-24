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
import org.apache.nifi.syslog.events.SyslogEvent;
import org.apache.nifi.syslog.parsers.SyslogParser;
import org.apache.nifi.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SyslogRecordReader implements RecordReader {
    private final BufferedReader reader;
    private RecordSchema schema;
    private final SyslogParser parser;

    public SyslogRecordReader(SyslogParser parser, InputStream in, RecordSchema schema) {
        this.reader = new BufferedReader(new InputStreamReader(in));
        this.schema = schema;
        this.parser = parser;
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {
        String line = reader.readLine();

        if (line == null) {
            // a null return from readLine() signals the end of the stream
            return null;
        }

        if (StringUtils.isBlank(line)) {
            // while an empty string is an error
            throw new MalformedRecordException("Encountered a blank message!");
        }


        final MalformedRecordException malformedRecordException;
        SyslogEvent event = parser.parseEvent(ByteBuffer.wrap(line.getBytes(parser.getCharsetName())));

        if (!event.isValid()) {
            malformedRecordException = new MalformedRecordException(
                    String.format("Failed to parse %s as a Syslog message: it does not conform to any of the RFC" +
                            " formats supported", line));
            throw malformedRecordException;
        }

        final Map<String, Object> syslogMap = new HashMap<>(8);
        syslogMap.put(SyslogAttributes.PRIORITY.key(), event.getPriority());
        syslogMap.put(SyslogAttributes.SEVERITY.key(), event.getSeverity());
        syslogMap.put(SyslogAttributes.FACILITY.key(), event.getFacility());
        syslogMap.put(SyslogAttributes.VERSION.key(), event.getVersion());
        syslogMap.put(SyslogAttributes.TIMESTAMP.key(), event.getTimeStamp());
        syslogMap.put(SyslogAttributes.HOSTNAME.key(), event.getHostName());
        syslogMap.put(SyslogAttributes.BODY.key(), event.getMsgBody());

        return new MapRecord(schema, syslogMap);
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return schema;
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }
}
