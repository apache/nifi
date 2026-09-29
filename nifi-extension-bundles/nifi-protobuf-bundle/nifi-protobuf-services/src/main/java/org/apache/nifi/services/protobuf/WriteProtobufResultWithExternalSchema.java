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
 * Writes a single Record as Protocol Buffers binary content. When configured, the
 * Schema Reference Writer and Message Index Writer write format-specific information
 * before the Protobuf payload, in that order.
 * <p>
 * Only one Record can be written because raw Protocol Buffers messages do not contain
 * boundaries that allow concatenated messages to be decoded independently.
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
    protected Map<String, String> onFinishRecordSet() throws IOException {
        flush();
        return getSchemaReferenceAttributes();
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        // Concatenated top-level Protobuf messages cannot be delimited: a standard decoder would merge them into a
        // single message (repeated fields accumulate, singular fields take last-wins), so only a single record per
        // FlowFile can be represented.
        if (getRecordCount() > 0) {
            throw new IOException("Protobuf output supports only a single record because concatenated Protobuf messages cannot be delimited");
        }

        final byte[] payload = serializer.serialize(record);
        writeFraming(buffered);
        buffered.write(payload);
        return getSchemaReferenceAttributes();
    }

    private Map<String, String> getSchemaReferenceAttributes() {
        return schemaReferenceWriter == null ? Map.of() : schemaReferenceWriter.getAttributes(recordSchema);
    }

    private void writeFraming(final OutputStream out) throws IOException {
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
        // Flushes buffered content and closes the underlying stream even if flushing fails; repeated calls have no effect
        buffered.close();
    }
}
