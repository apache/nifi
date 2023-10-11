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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.hubspot.jackson.datatype.protobuf.ProtobufJacksonConfig;
import com.hubspot.jackson.datatype.protobuf.builtin.deserializers.MessageDeserializer;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ByteString Field Deserializer supporting conversion from hexadecimal to ByteString for selected fields
 *
 * @param <T> Message Type
 * @param <V> Message Builder Type
 */
public class ByteStringFieldDeserializer<T extends Message, V extends Message.Builder> extends MessageDeserializer<T, V> {
    private static final Set<String> HEXADECIMAL_BYTE_STRING_FIELDS = Arrays.stream(HexadecimalByteStringField.values())
            .map(HexadecimalByteStringField::getField)
            .collect(Collectors.toSet());

    /**
     * Deserializer constructor with Message Type class to be deserialized
     *
     * @param messageType Message Type class to be deserialized
     * @param config Jackson Configuration for Protobuf
     */
    public ByteStringFieldDeserializer(final Class<T> messageType, final ProtobufJacksonConfig config) {
        super(messageType, config);
    }

    /**
     * Read value from JSON Parser and decode hexadecimal ByteString fields when found
     *
     * @param builder Protobuf Message Builder
     * @param field Message Field Descriptor
     * @param defaultInstance Protobuf default instance of Message
     * @param parser JSON Parser
     * @param context JSON Deserialization Context for parsing
     * @return Object value read
     * @throws IOException Thrown on parsing failures
     */
    @Override
    protected Object readValue(
            final Message.Builder builder,
            final Descriptors.FieldDescriptor field,
            final Message defaultInstance,
            final JsonParser parser,
            final DeserializationContext context
    ) throws IOException {
        final String jsonName = field.getJsonName();

        final Object value;
        if (HEXADECIMAL_BYTE_STRING_FIELDS.contains(jsonName)) {
            final String encoded = parser.getValueAsString();
            if (encoded == null) {
                value = null;
            } else {
                try {
                    final byte[] decoded = Hex.decodeHex(encoded);
                    value = ByteString.copyFrom(decoded);
                } catch (DecoderException e) {
                    throw new IOException(String.format("Hexadecimal Field [%s] decoding failed", jsonName), e);
                }
            }
        } else {
            value = super.readValue(builder, field, defaultInstance, parser, context);
        }

        return value;
    }
}
