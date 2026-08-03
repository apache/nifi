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
import org.apache.nifi.schemaregistry.services.MessageIndexWriter;
import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.SchemaReferenceWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.protobuf.converter.ProtobufDataSerializer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Writes Records as Protocol Buffers binary content. When a {@link SchemaReferenceWriter} is
 * configured, a Confluent wire-format header (magic byte and schema identifier) is written first;
 * when a {@link MessageIndexWriter} is configured, the Confluent message index array follows the
 * header. The serialized Protobuf payload is written last.
 * <p>
 * The Confluent framing (header and message index) is written once at the beginning of the record
 * set, mirroring {@code WriteAvroResultWithExternalSchema}; the typical Confluent use case writes a
 * single message per FlowFile.
 */
public class WriteProtobufResultWithExternalSchema extends AbstractRecordSetWriter {

    private final RecordSchema recordSchema;
    private final SchemaDefinition schemaDefinition;
    private final MessageName messageName;
    private final SchemaReferenceWriter schemaReferenceWriter;
    private final MessageIndexWriter messageIndexWriter;
    private final Map<String, String> variables;
    private final ProtobufDataSerializer serializer;
    private final OutputStream buffered;
    private boolean closed = false;

    public WriteProtobufResultWithExternalSchema(final Schema schema,
                                                 final MessageName messageName,
                                                 final RecordSchema recordSchema,
                                                 final SchemaDefinition schemaDefinition,
                                                 final SchemaReferenceWriter schemaReferenceWriter,
                                                 final MessageIndexWriter messageIndexWriter,
                                                 final Map<String, String> variables,
                                                 final OutputStream out) {
        super(out);
        this.recordSchema = recordSchema;
        this.schemaDefinition = schemaDefinition;
        this.messageName = messageName;
        this.schemaReferenceWriter = schemaReferenceWriter;
        this.messageIndexWriter = messageIndexWriter;
        this.variables = variables;
        this.buffered = new BufferedOutputStream(out);
        this.serializer = new ProtobufDataSerializer(schema, messageName.getFullyQualifiedName());
    }

    @Override
    protected void onBeginRecordSet() throws IOException {
        writeConfluentFraming(buffered);
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        flush();
        return Map.of();
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        // Concatenated top-level Protobuf messages cannot be delimited: a standard decoder would merge them into a
        // single message (repeated fields accumulate, singular fields take last-wins). Only a single record per
        // FlowFile can be represented, which also matches the Confluent one-message-per-record convention.
        if (getRecordCount() > 0) {
            throw new IOException("Protobuf output supports only a single record because concatenated Protobuf messages cannot be delimited");
        }

        // If we are not writing an active record set, then we need to ensure that the Confluent framing is written.
        if (!isActiveRecordSet()) {
            flush();
            writeConfluentFraming(buffered);
        }

        final byte[] payload = serializer.serialize(record);
        buffered.write(payload);
        return Map.of();
    }

    private void writeConfluentFraming(final OutputStream out) throws IOException {
        if (schemaReferenceWriter != null) {
            schemaReferenceWriter.writeHeader(recordSchema, out);
        }
        if (messageIndexWriter != null) {
            messageIndexWriter.writeMessageIndex(variables, schemaDefinition, messageName, out);
        }
    }

    @Override
    public void flush() throws IOException {
        buffered.flush();
    }

    @Override
    public String getMimeType() {
        return "application/octet-stream";
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        // Ensure buffered content is flushed to the underlying stream before it is closed, including the
        // write-without-active-record-set path where onFinishRecordSet is never invoked.
        flush();
        super.close();
    }
}
