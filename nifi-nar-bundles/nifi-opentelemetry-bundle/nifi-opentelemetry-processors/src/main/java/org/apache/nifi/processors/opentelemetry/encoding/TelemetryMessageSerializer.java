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
package org.apache.nifi.processors.opentelemetry.encoding;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.hubspot.jackson.datatype.protobuf.ProtobufJacksonConfig;
import com.hubspot.jackson.datatype.protobuf.builtin.serializers.MessageSerializer;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * OpenTelemetry extension of Protobuf Message Serializer supporting OTLP 1.0.0 customization of selected fields
 */
public class TelemetryMessageSerializer extends MessageSerializer {
    private static final Set<String> HEXADECIMAL_BYTE_STRING_FIELDS = Arrays.stream(HexadecimalByteStringField.values())
            .map(HexadecimalByteStringField::getField)
            .collect(Collectors.toSet());

    protected TelemetryMessageSerializer(final ProtobufJacksonConfig config) {
        super(config);
    }

    /**
     * Write value as JSON with hexadecimal encoding for selected ByteString fields
     *
     * @param field Message Field Descriptor
     * @param value Value to be serialized
     * @param generator JSON Generator
     * @param serializerProvider JSON Serializer Provider
     * @throws IOException Thrown on failures serializing values
     */
    @Override
    protected void writeValue(
            Descriptors.FieldDescriptor field,
            Object value,
            JsonGenerator generator,
            SerializerProvider serializerProvider
    ) throws IOException {
        final String jsonName = field.getJsonName();
        if (HEXADECIMAL_BYTE_STRING_FIELDS.contains(jsonName)) {
            final ByteString byteString = (ByteString) value;
            final String encoded = getEncodedByteString(byteString);
            generator.writeString(encoded);
        } else {
            super.writeValue(field, value, generator, serializerProvider);
        }
    }

    private String getEncodedByteString(final ByteString byteString) {
        final ByteBuffer buffer = byteString.asReadOnlyByteBuffer();
        return Hex.encodeHexString(buffer, false);
    }
}
