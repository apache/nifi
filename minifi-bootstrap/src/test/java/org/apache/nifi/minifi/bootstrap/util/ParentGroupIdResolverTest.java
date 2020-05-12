/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.bootstrap.util;

import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ParentGroupIdResolverTest {

    @Test
    public void testRemoteInputPortParentId() throws IOException, SchemaLoaderException {
        List<String> configLines = new ArrayList<>();
        configLines.add("MiNiFi Config Version: 2");
        configLines.add("Remote Process Groups:");
        configLines.add("- name: rpgOne");
        configLines.add("  id: rpgOneId");
        configLines.add("  Input Ports:");
        configLines.add("  - id: one");
        configLines.add("Process Groups:");
        configLines.add("- Remote Process Groups:");
        configLines.add("  - name: rpgTwo");
        configLines.add("    id: rpgTwoId");
        configLines.add("    Input Ports:");
        configLines.add("    - id: two");
        ParentGroupIdResolver parentGroupIdResolver = createParentGroupIdResolver(configLines);
        assertEquals("rpgOneId", parentGroupIdResolver.getRemoteInputPortParentId("one"));
        assertEquals("rpgTwoId", parentGroupIdResolver.getRemoteInputPortParentId("two"));
        assertNull(parentGroupIdResolver.getRemoteOutputPortParentId("one"));
        assertNull(parentGroupIdResolver.getRemoteOutputPortParentId("two"));
        assertNull(parentGroupIdResolver.getInputPortParentId("one"));
        assertNull(parentGroupIdResolver.getInputPortParentId("two"));
        assertNull(parentGroupIdResolver.getOutputPortParentId("one"));
        assertNull(parentGroupIdResolver.getOutputPortParentId("two"));
        assertNull(parentGroupIdResolver.getProcessorParentId("one"));
        assertNull(parentGroupIdResolver.getProcessorParentId("two"));
        assertNull(parentGroupIdResolver.getFunnelParentId("one"));
        assertNull(parentGroupIdResolver.getFunnelParentId("two"));
    }
    @Test
    public void testRemoteOutputPortParentId() throws IOException, SchemaLoaderException {
        List<String> configLines = new ArrayList<>();
        configLines.add("MiNiFi Config Version: 3");
        configLines.add("Remote Process Groups:");
        configLines.add("- name: rpgOne");
        configLines.add("  id: rpgOneId");
        configLines.add("  Output Ports:");
        configLines.add("  - id: one");
        configLines.add("Process Groups:");
        configLines.add("- Remote Process Groups:");
        configLines.add("  - name: rpgTwo");
        configLines.add("    id: rpgTwoId");
        configLines.add("    Output Ports:");
        configLines.add("    - id: two");
        ParentGroupIdResolver parentGroupIdResolver = createParentGroupIdResolver(configLines);
        assertNull(parentGroupIdResolver.getRemoteInputPortParentId("one"));
        assertNull(parentGroupIdResolver.getRemoteInputPortParentId("two"));
        assertEquals("rpgOneId", parentGroupIdResolver.getRemoteOutputPortParentId("one"));
        assertEquals("rpgTwoId", parentGroupIdResolver.getRemoteOutputPortParentId("two"));
        assertNull(parentGroupIdResolver.getInputPortParentId("one"));
        assertNull(parentGroupIdResolver.getInputPortParentId("two"));
        assertNull(parentGroupIdResolver.getOutputPortParentId("one"));
        assertNull(parentGroupIdResolver.getOutputPortParentId("two"));
        assertNull(parentGroupIdResolver.getProcessorParentId("one"));
        assertNull(parentGroupIdResolver.getProcessorParentId("two"));
        assertNull(parentGroupIdResolver.getFunnelParentId("one"));
        assertNull(parentGroupIdResolver.getFunnelParentId("two"));
    }

    @Test
    public void testInputPortParentId() throws IOException, SchemaLoaderException {
        List<String> configLines = new ArrayList<>();
        configLines.add("MiNiFi Config Version: 2");
        configLines.add("Input Ports:");
        configLines.add("- id: one");
        configLines.add("Process Groups:");
        configLines.add("- id: pgTwo");
        configLines.add("  Input Ports:");
        configLines.add("  - id: two");
        ParentGroupIdResolver parentGroupIdResolver = createParentGroupIdResolver(configLines);
        assertNull(parentGroupIdResolver.getRemoteInputPortParentId("one"));
        assertNull(parentGroupIdResolver.getRemoteInputPortParentId("two"));
        assertNull(parentGroupIdResolver.getRemoteOutputPortParentId("one"));
        assertNull(parentGroupIdResolver.getRemoteOutputPortParentId("two"));
        assertEquals(ConfigTransformer.ROOT_GROUP, parentGroupIdResolver.getInputPortParentId("one"));
        assertEquals("pgTwo", parentGroupIdResolver.getInputPortParentId("two"));
        assertNull(parentGroupIdResolver.getOutputPortParentId("one"));
        assertNull(parentGroupIdResolver.getOutputPortParentId("two"));
        assertNull(parentGroupIdResolver.getProcessorParentId("one"));
        assertNull(parentGroupIdResolver.getProcessorParentId("two"));
        assertNull(parentGroupIdResolver.getFunnelParentId("one"));
        assertNull(parentGroupIdResolver.getFunnelParentId("two"));
    }

    @Test
    public void testOutputPortParentId() throws IOException, SchemaLoaderException {
        List<String> configLines = new ArrayList<>();
        configLines.add("MiNiFi Config Version: 2");
        configLines.add("Output Ports:");
        configLines.add("- id: one");
        configLines.add("Process Groups:");
        configLines.add("- id: pgTwo");
        configLines.add("  Output Ports:");
        configLines.add("  - id: two");
        ParentGroupIdResolver parentGroupIdResolver = createParentGroupIdResolver(configLines);
        assertNull(parentGroupIdResolver.getRemoteInputPortParentId("one"));
        assertNull(parentGroupIdResolver.getRemoteInputPortParentId("two"));
        assertNull(parentGroupIdResolver.getRemoteOutputPortParentId("one"));
        assertNull(parentGroupIdResolver.getRemoteOutputPortParentId("two"));
        assertNull(parentGroupIdResolver.getInputPortParentId("one"));
        assertNull(parentGroupIdResolver.getInputPortParentId("two"));
        assertEquals(ConfigTransformer.ROOT_GROUP, parentGroupIdResolver.getOutputPortParentId("one"));
        assertEquals("pgTwo", parentGroupIdResolver.getOutputPortParentId("two"));
        assertNull(parentGroupIdResolver.getProcessorParentId("one"));
        assertNull(parentGroupIdResolver.getProcessorParentId("two"));
        assertNull(parentGroupIdResolver.getFunnelParentId("one"));
        assertNull(parentGroupIdResolver.getFunnelParentId("two"));
    }

    @Test
    public void testProcessorParentId() throws IOException, SchemaLoaderException {
        List<String> configLines = new ArrayList<>();
        configLines.add("MiNiFi Config Version: 2");
        configLines.add("Processors:");
        configLines.add("- id: one");
        configLines.add("Process Groups:");
        configLines.add("- id: pgTwo");
        configLines.add("  Processors:");
        configLines.add("  - id: two");
        ParentGroupIdResolver parentGroupIdResolver = createParentGroupIdResolver(configLines);
        assertNull(parentGroupIdResolver.getRemoteInputPortParentId("one"));
        assertNull(parentGroupIdResolver.getRemoteInputPortParentId("two"));
        assertNull(parentGroupIdResolver.getRemoteOutputPortParentId("one"));
        assertNull(parentGroupIdResolver.getRemoteOutputPortParentId("two"));
        assertNull(parentGroupIdResolver.getInputPortParentId("one"));
        assertNull(parentGroupIdResolver.getInputPortParentId("two"));
        assertNull(parentGroupIdResolver.getOutputPortParentId("one"));
        assertNull(parentGroupIdResolver.getOutputPortParentId("two"));
        assertEquals(ConfigTransformer.ROOT_GROUP, parentGroupIdResolver.getProcessorParentId("one"));
        assertEquals("pgTwo", parentGroupIdResolver.getProcessorParentId("two"));
        assertNull(parentGroupIdResolver.getFunnelParentId("one"));
        assertNull(parentGroupIdResolver.getFunnelParentId("two"));
    }

    @Test
    public void testFunnelParentId() throws IOException, SchemaLoaderException {
        List<String> configLines = new ArrayList<>();
        configLines.add("MiNiFi Config Version: 2");
        configLines.add("Funnels:");
        configLines.add("- id: one");
        configLines.add("Process Groups:");
        configLines.add("- id: pgTwo");
        configLines.add("  Funnels:");
        configLines.add("  - id: two");
        ParentGroupIdResolver parentGroupIdResolver = createParentGroupIdResolver(configLines);
        assertNull(parentGroupIdResolver.getRemoteInputPortParentId("one"));
        assertNull(parentGroupIdResolver.getRemoteInputPortParentId("two"));
        assertNull(parentGroupIdResolver.getRemoteOutputPortParentId("one"));
        assertNull(parentGroupIdResolver.getRemoteOutputPortParentId("two"));
        assertNull(parentGroupIdResolver.getInputPortParentId("one"));
        assertNull(parentGroupIdResolver.getInputPortParentId("two"));
        assertNull(parentGroupIdResolver.getOutputPortParentId("one"));
        assertNull(parentGroupIdResolver.getOutputPortParentId("two"));
        assertNull(parentGroupIdResolver.getProcessorParentId("one"));
        assertNull(parentGroupIdResolver.getProcessorParentId("two"));
        assertEquals(ConfigTransformer.ROOT_GROUP, parentGroupIdResolver.getFunnelParentId("one"));
        assertEquals("pgTwo", parentGroupIdResolver.getFunnelParentId("two"));
    }

    private ParentGroupIdResolver createParentGroupIdResolver(List<String> configLines) throws IOException, SchemaLoaderException {
        return new ParentGroupIdResolver(SchemaLoader.loadConfigSchemaFromYaml(new ByteArrayInputStream(configLines.stream().collect(Collectors.joining("\n"))
                .getBytes(StandardCharsets.UTF_8))).getProcessGroupSchema());
    }
}
