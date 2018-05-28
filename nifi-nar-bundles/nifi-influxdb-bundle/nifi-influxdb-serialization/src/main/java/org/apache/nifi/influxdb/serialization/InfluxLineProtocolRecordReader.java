/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.influxdb.serialization;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.record.listen.IOUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InfluxLineProtocolRecordReader implements RecordReader {

    protected static final String MEASUREMENT = "measurement";
    protected static final String TAG_SET = "tags";
    protected static final String FIELD_SET = "fields";
    protected static final String TIMESTAMP = "timestamp";

    private final BufferedReader reader;

    private final SimpleRecordSchema schema;

    public InfluxLineProtocolRecordReader(@Nullable final InputStream in,
                                          @NonNull final Charset charset) {

        Objects.requireNonNull(charset, "Charset is required");

        this.reader = in != null ? new BufferedReader(new InputStreamReader(in, charset)) : null;

        List<RecordField> fields = new ArrayList<>();

        // measurement
        fields.add(new RecordField(MEASUREMENT, RecordFieldType.STRING.getDataType(), false));

        // tags
        DataType mapDataType = RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType());
        fields.add(new RecordField(TAG_SET, mapDataType, true));

        // fields
        DataType choiceDataType = RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.FLOAT.getDataType(),
                RecordFieldType.LONG.getDataType(),
                RecordFieldType.BOOLEAN.getDataType(),
                RecordFieldType.STRING.getDataType());
        fields.add(new RecordField(FIELD_SET, RecordFieldType.MAP.getMapDataType(choiceDataType), false));

        // timestamp
        fields.add(new RecordField(TIMESTAMP, RecordFieldType.LONG.getDataType(), true));

        schema = new SimpleRecordSchema(fields);
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields)
            throws IOException, MalformedRecordException {

        String protocol = reader.readLine();

        return mapInlineProtocol(protocol, coerceTypes, dropUnknownFields);
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {

        return schema;
    }

    @Override
    public void close() throws IOException {

        IOUtils.closeQuietly(reader);
    }

    @Nullable
    private Record mapInlineProtocol(@Nullable final String protocol,
                                     final boolean coerceTypes,
                                     final boolean dropUnknownFields)

            throws MalformedRecordException {

        if (StringUtils.isBlank(protocol)) {
            return null;
        }

        InfluxLineProtocolParser parser = InfluxLineProtocolParser.parse(protocol);

        Map<String, Object> values = new HashMap<>();
        values.put(MEASUREMENT, parser.getMeasurement());
        values.put(TAG_SET, parser.getTags());
        values.put(FIELD_SET, parser.getFields());
        values.put(TIMESTAMP, parser.getTimestamp());

        return new MapRecord(schema, values, coerceTypes, dropUnknownFields);
    }
}
