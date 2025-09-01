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

package org.apache.nifi.minifi.commons.service;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.serialization.FlowSerializationException;

public class StandardFlowSerDeService implements FlowSerDeService {

    private final ObjectMapper objectMapper;

    StandardFlowSerDeService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public static StandardFlowSerDeService defaultInstance() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setAnnotationIntrospector(new JakartaXmlBindAnnotationIntrospector(objectMapper.getTypeFactory()));
        objectMapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
        return new StandardFlowSerDeService(objectMapper);
    }

    @Override
    public byte[] serialize(VersionedDataflow flow) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            JsonFactory factory = new JsonFactory();
            JsonGenerator generator = factory.createGenerator(byteArrayOutputStream);
            generator.setCodec(objectMapper);
            generator.writeObject(flow);
            generator.flush();
            byteArrayOutputStream.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new FlowSerializationException("Unable to serialize flow", e);
        }
    }

    @Override
    public VersionedDataflow deserialize(byte[] flow) {
        try {
            return objectMapper.readValue(flow, VersionedDataflow.class);
        } catch (Exception e) {
            throw new FlowSerializationException("Unable to deserialize flow", e);
        }
    }
}
