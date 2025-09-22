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

package org.apache.nifi.c2.serializer;

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.Optional;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C2JacksonSerializer implements C2Serializer {

    private static final Logger logger = LoggerFactory.getLogger(C2JacksonSerializer.class);

    private final ObjectMapper objectMapper;

    public C2JacksonSerializer() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(OperandType.class, new OperandTypeDeserializer());
        objectMapper.registerModule(module);
    }

    @Override
    public <T> Optional<String> serialize(T object) {
        if (object == null) {
            logger.trace("C2 Object was null. Nothing to serialize. Returning empty.");
            return empty();
        }

        String contentString = null;
        try {
            contentString = objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            logger.error("Object serialization to JSON failed", e);
        }

        return ofNullable(contentString);
    }

    @Override
    public <T> Optional<T> deserialize(String content, Class<T> valueType) {
        if (content == null) {
            logger.trace("Content for deserialization was null. Returning empty");
            return empty();
        }

        T responseObject = null;
        try {
            responseObject = objectMapper.readValue(content, valueType);
        } catch (JsonProcessingException e) {
            logger.error("Object deserialization from JSON failed", e);
        }

        return ofNullable(responseObject);
    }

    @Override
    public <T> Optional<T> convert(Object content, TypeReference<T> valueType) {
        if (content == null) {
            logger.trace("Content for conversion was null. Returning empty");
            return empty();
        }

        try {
            return ofNullable(objectMapper.convertValue(content, valueType));
        } catch (IllegalArgumentException e) {
            logger.error("Object conversion failed", e);
            return empty();
        }
    }
}
