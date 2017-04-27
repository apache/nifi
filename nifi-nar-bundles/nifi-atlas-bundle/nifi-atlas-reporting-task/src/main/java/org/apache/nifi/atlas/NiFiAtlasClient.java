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

import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_CREATED_BY_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_FLOW_PATHS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INCOMING_FLOW_PATHS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUT_PORTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTGOING_FLOW_PATHS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUT_PORTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUEUES;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.ENTITIES;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

public class NiFiAtlasClient {

    private static final Logger logger = LoggerFactory.getLogger(NiFiAtlasClient.class);

    private static NiFiAtlasClient nifiClient;
    private AtlasClientV2 atlasClient;

    private NiFiAtlasClient() {
        super();
    }

    public static NiFiAtlasClient getInstance() {
        if (nifiClient == null) {
            synchronized (NiFiAtlasClient.class) {
                if (nifiClient == null) {
                    nifiClient = new NiFiAtlasClient();
                }
            }
        }
        return nifiClient;
    }

    public void initialize(final boolean force, final String[] baseUrls, final String user, final String password, final File atlasConfDir) {
        if (atlasClient != null && !force) {
            // Already initialized and keep using it.
            return;
        }

        synchronized (NiFiAtlasClient.class) {

            if (atlasClient != null && !force) {
                // Already initialized and keep using it.
                return;
            }

            if (atlasClient != null) {
                logger.info("{} had been setup but replacing it with new one.", atlasClient);
                ApplicationProperties.forceReload();
            }

            if (atlasConfDir != null) {
                // If atlasConfDir is not set, atlas-application.properties will be searched under classpath.
                Properties props = System.getProperties();
                final String atlasConfProp = "atlas.conf";
                props.setProperty(atlasConfProp, atlasConfDir.getAbsolutePath());
                logger.debug("{} has been set to: {}", atlasConfProp, props.getProperty(atlasConfProp));
            }

            atlasClient = new AtlasClientV2(baseUrls, new String[]{user, password});

        }
    }

    /**
     * This is an utility method to delete unused types.
     * Should be used during development or testing only.
     * @param typeNames to delete
     */
    void deleteTypeDefs(String ... typeNames) throws AtlasServiceException {
        final AtlasTypesDef existingTypeDef = getTypeDefs(typeNames);
        try {
            atlasClient.deleteAtlasTypeDefs(existingTypeDef);
        } catch (UniformInterfaceException e) {
            if (e.getResponse().getStatus() == 204) {
                // 204 is a successful response.
                // NOTE: However after executing this, Atlas should be restarted to work properly.
                logger.info("Deleted type defs: {}", existingTypeDef);
            } else {
                throw e;
            }
        }
    }

    /**
     * Create or update NiFi types in Atlas type system.
     * @param update If false, doesn't perform anything if there is existing type def for the name.
     */
    public void registerNiFiTypeDefs(boolean update) throws AtlasServiceException {
        final Set<String> typeNames = ENTITIES.keySet();
        final Map<String, AtlasEntityDef> existingDefs = getTypeDefs(typeNames.toArray(new String[typeNames.size()])).getEntityDefs().stream()
                .collect(Collectors.toMap(def -> def.getName(), def -> def));


        final AtomicBoolean shouldUpdate = new AtomicBoolean(false);

        final AtlasTypesDef type = new AtlasTypesDef();

        typeNames.stream().filter(typeName -> {
            final AtlasEntityDef existingDef = existingDefs.get(typeName);
            if (existingDef != null) {
                // type is already defined.
                if (!update) {
                    return false;
                }
                shouldUpdate.set(true);
            }
            return true;
        }).forEach(typeName -> {
            final NiFiTypes.EntityDefinition def = ENTITIES.get(typeName);

            final AtlasEntityDef entity = new AtlasEntityDef();
            type.getEntityDefs().add(entity);

            entity.setName(typeName);

            Set<String> superTypes = new HashSet<>();
            List<AtlasAttributeDef> attributes = new ArrayList<>();

            def.define(entity, superTypes, attributes);

            entity.setSuperTypes(superTypes);
            entity.setAttributeDefs(attributes);
        });

        // Create or Update.
        final AtlasTypesDef atlasTypeDefsResult = shouldUpdate.get()
                ? atlasClient.updateAtlasTypeDefs(type)
                : atlasClient.createAtlasTypeDefs(type);
        logger.debug("Result={}", atlasTypeDefsResult);
    }

    private AtlasTypesDef getTypeDefs(String ... typeNames) throws AtlasServiceException {
        final AtlasTypesDef typeDefs = new AtlasTypesDef();
        for (int i = 0; i < typeNames.length; i++) {
            final MultivaluedMap<String, String> searchParams = new MultivaluedMapImpl();
            searchParams.add(SearchFilter.PARAM_NAME, typeNames[i]);
            final AtlasTypesDef typeDef = atlasClient.getAllTypeDefs(new SearchFilter(searchParams));
            typeDefs.getEntityDefs().addAll(typeDef.getEntityDefs());
        }
        logger.debug("typeDefs={}", typeDefs);
        return typeDefs;
    }

    public void registerNiFiFlow(NiFiFlow nifiFlow, List<NiFiFlowPath> paths) throws AtlasServiceException {
        final String nifiFlowName = nifiFlow.getFlowName();
        final String url = nifiFlow.getUrl();

        final Map<AtlasObjectId, AtlasEntity> inputPorts = nifiFlow.getInputPorts();
        final Map<AtlasObjectId, AtlasEntity> outputPorts = nifiFlow.getOutputPorts();
        final Map<AtlasObjectId, AtlasEntity> queues = nifiFlow.getQueues();
        final Map<AtlasObjectId, AtlasEntity> createdData = nifiFlow.getCreatedData();

        // Number of entity objects before adding paths. +1 is the root flow.
        final int offsetToPaths = 1 + inputPorts.size() + outputPorts.size() + queues.size() + createdData.size();
        final List<AtlasEntity> entities = new ArrayList<>(offsetToPaths + paths.size());
        final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(entities);

        // Create parent flow entity.
        final AtlasEntity flowEntity = new AtlasEntity();
        entities.add(flowEntity);
        flowEntity.setTypeName(TYPE_NIFI_FLOW);
        flowEntity.setVersion(1L);
        flowEntity.setAttribute(ATTR_NAME, nifiFlowName);
        flowEntity.setAttribute(ATTR_QUALIFIED_NAME, nifiFlow.getRootProcessGroupId());
        flowEntity.setAttribute(ATTR_URL, url);
        flowEntity.setAttribute(ATTR_DESCRIPTION, nifiFlow.getDescription());

        // Create nifi_flow entity to make nifiFlowId available for other entities.
        EntityMutationResponse mutationResponse = atlasClient.createEntities(atlasEntities);
        logger.debug("mutation response={}", mutationResponse);

        // Create DataSet entities those are created by this NiFi flow.
        entities.addAll(inputPorts.values());
        entities.addAll(outputPorts.values());
        entities.addAll(queues.values());
        entities.addAll(createdData.values());

        mutationResponse = atlasClient.createEntities(atlasEntities);
        logger.debug("mutation response={}", mutationResponse);

        // Create path entities.
        for (int i = 0; i < paths.size(); i++) {
            NiFiFlowPath path = paths.get(i);

            final AtlasEntity pathEntity = new AtlasEntity();
            entities.add(pathEntity);
            pathEntity.setTypeName(TYPE_NIFI_FLOW_PATH);
            pathEntity.setAttribute(ATTR_NIFI_FLOW, nifiFlow.getId());
            pathEntity.setVersion(1L);

            final StringBuilder description = new StringBuilder();
            path.getProcessorIds().forEach(pid -> {
                final ProcessorDTO processor = nifiFlow.getProcessors().get(pid);
                if (description.length() > 0) {
                    description.append(", ");
                }
                description.append(String.format("%s::%s", processor.getName(), pid));
            });

            pathEntity.setAttribute(ATTR_NAME, path.getName());
            pathEntity.setAttribute(ATTR_DESCRIPTION, description.toString());

            // Use first processor's id as qualifiedName.
            pathEntity.setAttribute(ATTR_QUALIFIED_NAME, path.getId());
            pathEntity.setAttribute(ATTR_URL, url);

            // At this point, nifi_flow is already created, so DataSet can have a pointer to nifi_flow.
            registerDataSetIO(path, nifiFlow, pathEntity);
        }

        // Setup links from nifiFlow.
        registerDataSetIO(nifiFlow, nifiFlow, flowEntity);

        // Create entities without relationships, Atlas doesn't allow storing ObjectId that doesn't exist.
        mutationResponse = atlasClient.createEntities(atlasEntities);
        logger.debug("mutation response={}", mutationResponse);


        // Now loop through entities again to add relationships.
        final List<AtlasObjectId> flowPaths = new ArrayList<>();
        flowEntity.setAttribute(ATTR_FLOW_PATHS, flowPaths);

        for (int i = 0; i < paths.size(); i++) {
            NiFiFlowPath path = paths.get(i);

            final AtlasEntity pathEntity = entities.get(offsetToPaths + i);

            final List<AtlasObjectId> incomingPaths = path.getIncomingPaths().stream()
                    .map(p -> createObjectId(p)).collect(Collectors.toList());
            final List<AtlasObjectId> outgoingPaths = path.getOutgoingPaths().stream()
                    .map(p -> createObjectId(p)).collect(Collectors.toList());

            pathEntity.setAttribute(ATTR_INCOMING_FLOW_PATHS, incomingPaths);
            pathEntity.setAttribute(ATTR_OUTGOING_FLOW_PATHS, outgoingPaths);

            // Add path reference for parent flow.
            flowPaths.add(createObjectId(path));
        }

        flowEntity.setAttribute(ATTR_QUEUES, queues.keySet());
        flowEntity.setAttribute(ATTR_INPUT_PORTS, inputPorts.keySet());
        flowEntity.setAttribute(ATTR_OUTPUT_PORTS, outputPorts.keySet());
        flowEntity.setAttribute(ATTR_CREATED_BY_NIFI_FLOW, createdData.keySet());

        // Send updated entities.
        try {
            mutationResponse = atlasClient.createEntities(atlasEntities);
        } catch (AtlasServiceException e) {
            if (e.getStatus().getStatusCode() == 404 && e.getMessage().contains("ATLAS-404-00-00B")) {
                // NOTE: If previously existed nifi_flow_path entity is removed because the path is removed from NiFi,
                // then Atlas respond with 404 even though the entity is successfully updated.
                // Following exception is thrown in this case. Just log it.
                // org.apache.atlas.AtlasServiceException:
                // Metadata service API org.apache.atlas.AtlasBaseClient$APIInfo@45a37759
                // failed with status 404 (Not Found) Response Body
                // ({"errorCode":"ATLAS-404-00-00B","errorMessage":"Given instance is invalid/not found:
                // Could not find entities in the repository with guids: [96d24487-cd66-4795-b552-f00b426fed26]"})
                logger.debug("Received error response from Atlas but it should be stored." + e);
            } else {
                throw e;
            }
        }
        logger.debug("mutation response={}", mutationResponse);
    }

    private AtlasObjectId createObjectId(NiFiFlowPath path) {
        return new AtlasObjectId(TYPE_NIFI_FLOW_PATH, ATTR_QUALIFIED_NAME, path.getId());
    }

    private void registerDataSetIO(AtlasProcess process, NiFiFlow nifiFlow, AtlasEntity entity) throws AtlasServiceException {
        final Set<AtlasObjectId> inputs = process.getInputs();
        final Set<AtlasObjectId> outputs = process.getOutputs();

        for (AtlasObjectId in : inputs) {
            if (outputs.contains(in)) {
                // TODO: is it true?
                throw new IllegalStateException("Looping dataset will cause infinite loop.");
            }
        }

        Function<Set<AtlasObjectId>, Set<AtlasObjectId>> onlyAvailableIds = atlasObjectIds -> atlasObjectIds.stream()
                .filter(id -> {
                    try {
                        searchEntityDef(id);
                        return true;
                    } catch (AtlasServiceException e) {
                        // try to create one.
                        final AtlasEntity dataSetEntity = nifiFlow.createDataSetEntity(id);
                        if (dataSetEntity != null) {
                            try {
                                registerEntity(dataSetEntity);
                                return true;
                            } catch (AtlasServiceException createE) {
                                logger.warn("Failed to create DataSet entity for {} in Atlas due to {}. Skipping.", id, createE);
                                return false;
                            }
                        }

                        logger.warn("Failed to get DataSet entity for {} from Atlas. Skipping.", id);
                        return false;
                    }
                }).collect(Collectors.toSet());


        entity.setAttribute(ATTR_INPUTS, onlyAvailableIds.apply(inputs));
        entity.setAttribute(ATTR_OUTPUTS, onlyAvailableIds.apply(outputs));
    }

    private void registerEntity(AtlasEntity entity) throws AtlasServiceException {
        final AtlasEntity.AtlasEntityWithExtInfo entityEx = new AtlasEntity.AtlasEntityWithExtInfo(entity);
        final EntityMutationResponse mutationResponse = atlasClient.createEntity(entityEx);
        logger.info("mutation response={}", mutationResponse);
    }

    public AtlasEntity.AtlasEntityWithExtInfo searchEntityDef(AtlasObjectId id) throws AtlasServiceException {
        final Map<String, String> attributes = new HashMap<>();
        id.getUniqueAttributes().entrySet().stream().filter(entry -> entry.getValue() != null)
                .forEach(entry -> attributes.put(entry.getKey(), entry.getValue().toString()));
        return atlasClient.getEntityByAttribute(id.getTypeName(), attributes);
    }
}
