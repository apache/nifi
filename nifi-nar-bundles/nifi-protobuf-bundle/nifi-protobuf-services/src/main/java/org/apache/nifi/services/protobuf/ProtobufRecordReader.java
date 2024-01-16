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
package org.apache.nifi.services.protobuf;

import com.squareup.wire.schema.Schema;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.protobuf.converter.ProtobufDataConverter;

import java.io.IOException;
import java.io.InputStream;

public class ProtobufRecordReader implements RecordReader {

    private final Schema protoSchema;
    private final String messageType;
    private final InputStream inputStream;
    private RecordSchema recordSchema;
    private boolean inputProcessed;

    public ProtobufRecordReader(Schema protoSchema, String messageType, InputStream inputStream, RecordSchema recordSchema) {
        this.protoSchema = protoSchema;
        this.messageType = messageType;
        this.inputStream = inputStream;
        this.recordSchema = recordSchema;
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException {
        if (!inputProcessed) {
            final ProtobufDataConverter dataConverter = new ProtobufDataConverter(protoSchema, messageType, recordSchema, coerceTypes, dropUnknownFields);
            final Record record = dataConverter.createRecord(inputStream);
            inputProcessed = true;
            recordSchema = record.getSchema();
            return record;
        }

        return null;
    }

    @Override
    public RecordSchema getSchema() {
        return recordSchema;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
