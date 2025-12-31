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
package org.apache.nifi.registry.flow.git.serialize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.nifi.flow.VersionedListenPortDefinition;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.flow.VersionedResourceDefinition;
import org.apache.nifi.flow.VersionedResourceType;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JacksonFlowSnapshotSerializerTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testOrdering() throws IOException {
        final JacksonFlowSnapshotSerializer serializer = new JacksonFlowSnapshotSerializer();

        final RegisteredFlowSnapshot flowSnapshot = new RegisteredFlowSnapshot();
        final VersionedProcessGroup versionedProcessGroup = new VersionedProcessGroup();

        final VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setIdentifier("myParamContext");
        versionedParameterContext.setInheritedParameterContexts(List.of("inheritedContext2", "inheritedContext3", "inheritedContext1"));

        VersionedParameter parameter1 = new VersionedParameter();
        parameter1.setName("name1");
        VersionedParameter parameter2 = new VersionedParameter();
        parameter2.setName("name2");
        VersionedParameter parameter3 = new VersionedParameter();
        parameter3.setName("name3");

        versionedParameterContext.setParameters(Set.of(parameter2, parameter1, parameter3));

        final VersionedPropertyDescriptor descriptor = new VersionedPropertyDescriptor();
        final VersionedResourceDefinition resourceDefinition = new VersionedResourceDefinition();
        resourceDefinition.setResourceTypes(Set.of(VersionedResourceType.TEXT, VersionedResourceType.URL, VersionedResourceType.FILE));
        descriptor.setResourceDefinition(resourceDefinition);
        final VersionedListenPortDefinition listenPortDefinition = new VersionedListenPortDefinition();
        listenPortDefinition.setTransportProtocol(VersionedListenPortDefinition.TransportProtocol.TCP);
        listenPortDefinition.setApplicationProtocols(List.of("http/1.1", "h2"));
        descriptor.setListenPortDefinition(listenPortDefinition);

        final VersionedProcessor processor1 = new VersionedProcessor();
        processor1.setIdentifier("proc1");
        processor1.setAutoTerminatedRelationships(Set.of("success", "failure"));
        processor1.setPropertyDescriptors(Map.of("prop1", descriptor));
        final VersionedProcessor processor2 = new VersionedProcessor();
        processor2.setIdentifier("proc2");
        final VersionedProcessor processor3 = new VersionedProcessor();
        processor3.setIdentifier("proc3");

        versionedProcessGroup.setIdentifier("pg1");
        versionedProcessGroup.setName("Process Group 1");
        versionedProcessGroup.setProcessors(Set.of(processor2, processor1, processor3));

        flowSnapshot.setFlowContents(versionedProcessGroup);
        flowSnapshot.setParameterContexts(Map.of("myParamContext", versionedParameterContext));

        final String jsonString = serializer.serialize(flowSnapshot);

        final JsonNode flow = OBJECT_MAPPER.readTree(jsonString);

        final JsonNode processGroup = flow.get("flowContents");
        final ArrayNode processors = (ArrayNode) processGroup.get("processors");

        final JsonNode parameterContexts = flow.get("parameterContexts");
        final JsonNode parameterContext = parameterContexts.get("myParamContext");
        final ArrayNode parameters = (ArrayNode) parameterContext.get("parameters");

        assertEquals(3, processors.size());
        assertEquals("proc1", processors.get(0).get("identifier").asText());
        assertEquals("[ \"failure\", \"success\" ]", processors.get(0).get("autoTerminatedRelationships").toPrettyString());
        assertEquals("[ \"FILE\", \"TEXT\", \"URL\" ]", processors.get(0).get("propertyDescriptors").get("prop1").get("resourceDefinition").get("resourceTypes").toPrettyString());
        assertEquals("TCP", processors.get(0).get("propertyDescriptors").get("prop1").get("listenPortDefinition").get("transportProtocol").asText());
        assertEquals("[ \"h2\", \"http/1.1\" ]", processors.get(0).get("propertyDescriptors").get("prop1").get("listenPortDefinition").get("applicationProtocols").toPrettyString());

        assertEquals("proc2", processors.get(1).get("identifier").asText());
        assertEquals("proc3", processors.get(2).get("identifier").asText());

        assertEquals(1, parameterContexts.size());
        assertEquals("[ \"inheritedContext2\", \"inheritedContext3\", \"inheritedContext1\" ]", parameterContext.get("inheritedParameterContexts").toPrettyString());

        assertEquals(3, parameters.size());
        assertEquals("name1", parameters.get(0).get("name").asText());
        assertEquals("name2", parameters.get(1).get("name").asText());
        assertEquals("name3", parameters.get(2).get("name").asText());
    }

}
