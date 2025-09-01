/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.registry.flow.git.serialize;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of {@link FlowSnapshotSerializer} that is Jackson's ObjectMapper.
 */
public class JacksonFlowSnapshotSerializer implements FlowSnapshotSerializer {

    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .defaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL))
            .annotationIntrospector(new JakartaXmlBindAnnotationIntrospector(TypeFactory.defaultInstance()))
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .enable(SerializationFeature.INDENT_OUTPUT)
            .addModule(new VersionedComponentModule())
            .addModule(new SortedStringCollectionsModule())
            .build();

    @Override
    public String serialize(final RegisteredFlowSnapshot flowSnapshot) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(flowSnapshot);
    }

    @Override
    public RegisteredFlowSnapshot deserialize(final InputStream inputStream) throws IOException {
        return OBJECT_MAPPER.readValue(inputStream, RegisteredFlowSnapshot.class);
    }

}
