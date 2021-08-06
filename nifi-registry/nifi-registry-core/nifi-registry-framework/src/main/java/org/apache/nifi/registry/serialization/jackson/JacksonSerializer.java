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
package org.apache.nifi.registry.serialization.jackson;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.registry.serialization.SerializationConstants;
import org.apache.nifi.registry.serialization.SerializationException;
import org.apache.nifi.registry.serialization.VersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;

/**
 * A Serializer that uses Jackson for serializing/deserializing.
 */
public abstract class JacksonSerializer<T> implements VersionedSerializer<T> {

    private static final Logger logger = LoggerFactory.getLogger(JacksonSerializer.class);

    private static final String JSON_HEADER = "\"header\"";
    private static final String DATA_MODEL_VERSION = "dataModelVersion";

    private final ObjectMapper objectMapper = ObjectMapperProvider.getMapper();

    @Override
    public void serialize(int dataModelVersion, T t, OutputStream out) throws SerializationException {
        if (t == null) {
            throw new IllegalArgumentException("The object to serialize cannot be null");
        }

        if (out == null) {
            throw new IllegalArgumentException("OutputStream cannot be null");
        }

        final SerializationContainer<T> container = new SerializationContainer<>();
        container.setHeader(Collections.singletonMap(DATA_MODEL_VERSION, String.valueOf(dataModelVersion)));
        container.setContent(t);

        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(out, container);
        } catch (IOException e) {
            throw new SerializationException("Unable to serialize object", e);
        }
    }

    @Override
    public T deserialize(InputStream input) throws SerializationException {
        final TypeReference<SerializationContainer<T>> typeRef = getDeserializeTypeRef();
        try {
            final SerializationContainer<T> container = objectMapper.readValue(input, typeRef);
            return container.getContent();
        } catch (IOException e) {
            throw new SerializationException("Unable to deserialize object", e);
        }
    }

    abstract TypeReference<SerializationContainer<T>> getDeserializeTypeRef() throws SerializationException;

    @Override
    public int readDataModelVersion(InputStream input) throws SerializationException {
        final byte[] headerBytes = new byte[SerializationConstants.MAX_HEADER_BYTES];
        final int readHeaderBytes;
        try {
            readHeaderBytes = input.read(headerBytes);
        } catch (IOException e) {
            throw new SerializationException("Could not read additional bytes to parse as serialization version 2 or later. "
                    + e.getMessage(), e);
        }

        // Seek '"header"'.
        final String headerStr = new String(headerBytes, 0, readHeaderBytes, StandardCharsets.UTF_8);
        final int headerIndex = headerStr.indexOf(JSON_HEADER);
        if (headerIndex < 0) {
            throw new SerializationException(String.format("Could not find %s in the first %d bytes",
                    JSON_HEADER, readHeaderBytes));
        }

        final int headerStart = headerStr.indexOf("{", headerIndex);
        if (headerStart < 0) {
            throw new SerializationException(String.format("Could not find '{' starting header object in the first %d bytes.", readHeaderBytes));
        }

        final int headerEnd = headerStr.indexOf("}", headerStart);
        if (headerEnd < 0) {
            throw new SerializationException(String.format("Could not find '}' ending header object in the first %d bytes.", readHeaderBytes));
        }

        final String headerObjectStr = headerStr.substring(headerStart, headerEnd + 1);
        logger.debug("headerObjectStr={}", headerObjectStr);

        try {
            final TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {};
            final HashMap<String, String> header = objectMapper.readValue(headerObjectStr, typeRef);
            if (!header.containsKey(DATA_MODEL_VERSION)) {
                throw new SerializationException("Missing " + DATA_MODEL_VERSION);
            }

            return Integer.parseInt(header.get(DATA_MODEL_VERSION));
        } catch (IOException e) {
            throw new SerializationException(String.format("Failed to parse header string '%s' due to %s", headerObjectStr, e), e);
        } catch (NumberFormatException e) {
            throw new SerializationException(String.format("Failed to parse version string due to %s", e.getMessage()), e);
        }
    }
}
