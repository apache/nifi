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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.protobuf.Message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Standard Request Mapper supporting Protobuf to JSON conversion following OTLP 1.0.0 conventions
 */
public class StandardRequestMapper implements RequestMapper {

    private final ObjectMapper objectMapper;

    /**
     * Standard Request Mapper constructor configures Jackson ObjectMapper with Protobuf Module and OTLP serializers
     */
    public StandardRequestMapper() {
        objectMapper = new ObjectMapper();
        // OTLP 1.0.0 requires enumerated values to be serialized as integers
        objectMapper.enable(SerializationFeature.WRITE_ENUMS_USING_INDEX);

        final StandardProtobufModule protobufModule = new StandardProtobufModule();
        objectMapper.registerModule(protobufModule);
    }

    @Override
    public <T extends Message> T readValue(final InputStream inputStream, final Class<T> messageClass) throws IOException {
        Objects.requireNonNull(inputStream, "Input Stream required");
        Objects.requireNonNull(messageClass, "Message Class required");
        return objectMapper.readValue(inputStream, messageClass);
    }

    @Override
    public void writeValue(final OutputStream outputStream, final Message message) throws IOException {
        Objects.requireNonNull(outputStream, "Output Stream required");
        Objects.requireNonNull(message, "Message required");
        objectMapper.writeValue(outputStream, message);
    }
}
