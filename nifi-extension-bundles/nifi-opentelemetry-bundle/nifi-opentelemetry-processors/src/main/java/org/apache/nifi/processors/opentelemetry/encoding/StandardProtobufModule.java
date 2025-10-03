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

import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.google.protobuf.Message;
import com.hubspot.jackson.datatype.protobuf.ProtobufJacksonConfig;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

/**
 * Standard extension of Protobuf Jackson Module supporting OTLP JSON Protobuf Encoding specifications
 */
public class StandardProtobufModule extends ProtobufModule {
    private static final ProtobufJacksonConfig protobufJacksonConfig = ProtobufJacksonConfig.builder()
            .properUnsignedNumberSerialization(true)
            .build();

    public StandardProtobufModule() {
        super(protobufJacksonConfig);
    }

    @Override
    public void setupModule(SetupContext context) {
        super.setupModule(context);

        final TelemetryMessageSerializer telemetryMessageSerializer = new TelemetryMessageSerializer(protobufJacksonConfig);
        final SimpleSerializers serializers = new SimpleSerializers();

        for (final HexadecimalMessageType hexadecimalMessageType : HexadecimalMessageType.values()) {
            final Class<? extends Message> messageType = hexadecimalMessageType.getMessageType();
            serializers.addSerializer(messageType, telemetryMessageSerializer);
        }

        context.addSerializers(serializers);

        final StandardMessageDeserializerFactory deserializerFactory = new StandardMessageDeserializerFactory(protobufJacksonConfig);
        context.addDeserializers(deserializerFactory);
    }
}
