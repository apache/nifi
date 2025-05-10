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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Implementation of {@link FlowSnapshotSerializer} that is Jackson's ObjectMapper.
 */
public class JacksonFlowSnapshotSerializer implements FlowSnapshotSerializer {

    private static final Set<String> EXCLUDE_JSON_FIELDS = Set.of("instanceIdentifier", "instanceGroupId");

    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            .defaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL))
            .annotationIntrospector(new JakartaXmlBindAnnotationIntrospector(TypeFactory.defaultInstance()))
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .enable(SerializationFeature.INDENT_OUTPUT)
            .build();

    @Override
    public String serialize(final RegisteredFlowSnapshot flowSnapshot) throws IOException {
        final JsonNode tree = OBJECT_MAPPER.valueToTree(flowSnapshot);
        final JsonNode normalized = normalize(tree);
        return OBJECT_MAPPER.writeValueAsString(normalized);
    }

    @Override
    public RegisteredFlowSnapshot deserialize(final InputStream inputStream) throws IOException {
        return OBJECT_MAPPER.readValue(inputStream, RegisteredFlowSnapshot.class);
    }

    private static JsonNode normalize(JsonNode node) {
        if (node.isObject()) {
            ObjectNode in = (ObjectNode) node;
            ObjectNode out = JsonNodeFactory.instance.objectNode();

            // collect and sort field names
            List<String> fieldNames = new ArrayList<>();
            in.fieldNames().forEachRemaining(fieldNames::add);
            Collections.sort(fieldNames);

            for (String name : fieldNames) {
                if (EXCLUDE_JSON_FIELDS.contains(name)) {
                    // skip this field entirely
                    continue;
                }
                out.set(name, normalize(in.get(name)));
            }
            return out;

        } else if (node.isArray()) {
            ArrayNode in = (ArrayNode) node;
            // recursively normalize all elements
            List<JsonNode> elems = StreamSupport.stream(in.spliterator(), false)
                    .map(JacksonFlowSnapshotSerializer::normalize)
                    .collect(Collectors.toList());

            if (!elems.isEmpty()) {
                JsonNode first = elems.get(0);
                if (first.isObject() && first.has("identifier")) {
                    elems.sort(Comparator.comparing(n -> n.get("identifier").textValue()));
                } else if (first.isTextual()) {
                    elems.sort(Comparator.comparing(JsonNode::textValue));
                }
            }

            ArrayNode out = JsonNodeFactory.instance.arrayNode();
            elems.forEach(out::add);
            return out;
        }

        // primitives, nulls, etc.
        return node;
    }

}
