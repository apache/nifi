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
package org.apache.nifi.atlas;

import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NiFiTypes {

    public static final String TYPE_ASSET = "Asset";
    public static final String TYPE_REFERENCEABLE = "Referenceable";
    public static final String TYPE_PROCESS = "Process";
    public static final String TYPE_DATASET = "DataSet";
    public static final String TYPE_NIFI_COMPONENT = "nifi_component";
    public static final String TYPE_NIFI_FLOW = "nifi_flow";
    public static final String TYPE_NIFI_FLOW_PATH = "nifi_flow_path";
    public static final String TYPE_NIFI_DATA = "nifi_data";
    public static final String TYPE_NIFI_QUEUE = "nifi_queue";
    public static final String TYPE_NIFI_INPUT_PORT = "nifi_input_port";
    public static final String TYPE_NIFI_OUTPUT_PORT = "nifi_output_port";

    public static final String ATTR_GUID = "guid";
    public static final String ATTR_TYPENAME = "typeName";
    public static final String ATTR_NAME = "name";
    public static final String ATTR_CLUSTER_NAME = "clusterName";
    public static final String ATTR_DESCRIPTION = "description";
    public static final String ATTR_INPUTS = "inputs";
    public static final String ATTR_OUTPUTS = "outputs";
    public static final String ATTR_URL = "url";
    public static final String ATTR_URI = "uri";
    public static final String ATTR_PATH = "path";
    public static final String ATTR_QUALIFIED_NAME = "qualifiedName";
    public static final String ATTR_NIFI_FLOW = "nifiFlow";
    public static final String ATTR_FLOW_PATHS = "flowPaths";
    public static final String ATTR_QUEUES = "queues";
    public static final String ATTR_INPUT_PORTS = "inputPorts";
    public static final String ATTR_OUTPUT_PORTS = "outputPorts";

    @FunctionalInterface
    interface EntityDefinition {
        void define(AtlasEntityDef entity, Set<String> superTypes, List<AtlasAttributeDef> attributes);
    }

    private static String arrayOf(String typeName) {
        return "array<" + typeName + ">";
    }

    private static EntityDefinition NIFI_FLOW = (entity, superTypes, attributes) -> {
        entity.setVersion(1L);
        superTypes.add(TYPE_REFERENCEABLE);
        superTypes.add(TYPE_ASSET);

        final AtlasAttributeDef url = new AtlasAttributeDef(ATTR_URL, "string");

        final AtlasAttributeDef flowPaths = new AtlasAttributeDef(ATTR_FLOW_PATHS, arrayOf(TYPE_NIFI_FLOW_PATH));
        flowPaths.setIsOptional(true);
        // Set ownedRef so that child flowPaths entities those no longer exist can be deleted when a NiFi is updated.
        final AtlasConstraintDef ownedRef = new AtlasConstraintDef("ownedRef");
        flowPaths.addConstraint(ownedRef);

        final AtlasAttributeDef queues = new AtlasAttributeDef(ATTR_QUEUES, arrayOf(TYPE_NIFI_QUEUE));
        queues.setIsOptional(true);
        queues.addConstraint(ownedRef);

        final AtlasAttributeDef inputPorts = new AtlasAttributeDef(ATTR_INPUT_PORTS, arrayOf(TYPE_NIFI_INPUT_PORT));
        inputPorts.setIsOptional(true);
        inputPorts.addConstraint(ownedRef);

        final AtlasAttributeDef outputPorts = new AtlasAttributeDef(ATTR_OUTPUT_PORTS, arrayOf(TYPE_NIFI_OUTPUT_PORT));
        outputPorts.setIsOptional(true);
        outputPorts.addConstraint(ownedRef);

        attributes.add(url);
        attributes.add(flowPaths);
        attributes.add(queues);
        attributes.add(inputPorts);
        attributes.add(outputPorts);
    };

    private static EntityDefinition NIFI_COMPONENT = (entity, superTypes, attributes) -> {
        entity.setVersion(1L);

        final AtlasAttributeDef nifiFlow = new AtlasAttributeDef(ATTR_NIFI_FLOW, TYPE_NIFI_FLOW);
        nifiFlow.setIsOptional(true);

        attributes.add(nifiFlow);
    };

    private static EntityDefinition NIFI_FLOW_PATH = (entity, superTypes, attributes) -> {
        entity.setVersion(1L);
        superTypes.add(TYPE_PROCESS);
        superTypes.add(TYPE_NIFI_COMPONENT);

        final AtlasAttributeDef url = new AtlasAttributeDef(ATTR_URL, "string");

        attributes.add(url);
    };

    private static EntityDefinition NIFI_DATA = (entity, superTypes, attributes) -> {
        entity.setVersion(1L);
        superTypes.add(TYPE_DATASET);
        superTypes.add(TYPE_NIFI_COMPONENT);
    };

    private static EntityDefinition NIFI_QUEUE = (entity, superTypes, attributes) -> {
        entity.setVersion(1L);
        superTypes.add(TYPE_DATASET);
        superTypes.add(TYPE_NIFI_COMPONENT);
    };

    private static EntityDefinition NIFI_INPUT_PORT = (entity, superTypes, attributes) -> {
        entity.setVersion(1L);
        superTypes.add(TYPE_DATASET);
        superTypes.add(TYPE_NIFI_COMPONENT);
    };

    private static EntityDefinition NIFI_OUTPUT_PORT = (entity, superTypes, attributes) -> {
        entity.setVersion(1L);
        superTypes.add(TYPE_DATASET);
        superTypes.add(TYPE_NIFI_COMPONENT);
    };

    static Map<String, EntityDefinition> ENTITIES = new HashMap<>();
    static {
        ENTITIES.put(TYPE_NIFI_COMPONENT, NIFI_COMPONENT);
        ENTITIES.put(TYPE_NIFI_DATA, NIFI_DATA);
        ENTITIES.put(TYPE_NIFI_QUEUE, NIFI_QUEUE);
        ENTITIES.put(TYPE_NIFI_INPUT_PORT, NIFI_INPUT_PORT);
        ENTITIES.put(TYPE_NIFI_OUTPUT_PORT, NIFI_OUTPUT_PORT);
        ENTITIES.put(TYPE_NIFI_FLOW_PATH, NIFI_FLOW_PATH);
        ENTITIES.put(TYPE_NIFI_FLOW, NIFI_FLOW);
    }

    static final String[] NIFI_TYPES = ENTITIES.keySet().toArray(new String[ENTITIES.size()]);

}
