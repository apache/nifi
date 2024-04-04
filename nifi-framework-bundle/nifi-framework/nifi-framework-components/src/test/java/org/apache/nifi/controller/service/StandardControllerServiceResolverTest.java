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
package org.apache.nifi.controller.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StandardControllerServiceResolverTest {

    private static final String BASE_SNAPSHOT_LOCATION = "src/test/resources/snapshots";
    private static final String CHILD_REFERENCES_SERVICES_FROM_PARENT_LOCATION = BASE_SNAPSHOT_LOCATION + "/versioned-child-services-from-parent";
    private static final String STANDARD_EXTERNAL_SERVICE_REFERENCE = BASE_SNAPSHOT_LOCATION + "/standard-external-service-reference";

    private NiFiRegistryFlowMapper flowMapper;
    private ControllerServiceProvider controllerServiceProvider;
    private ControllerServiceApiLookup controllerServiceApiLookup;

    private NiFiUser nifiUser;
    private ProcessGroup parentGroup;

    private ControllerServiceResolver serviceResolver;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setup() {
        Authorizer authorizer = mock(Authorizer.class);
        FlowManager flowManager = mock(FlowManager.class);
        flowMapper = mock(NiFiRegistryFlowMapper.class);
        controllerServiceProvider = mock(ControllerServiceProvider.class);
        controllerServiceApiLookup = mock(ControllerServiceApiLookup.class);

        nifiUser = mock(NiFiUser.class);

        parentGroup = mock(ProcessGroup.class);
        when(parentGroup.getIdentifier()).thenReturn("parentGroup");
        when(parentGroup.getControllerServices(true)).thenReturn(Collections.emptySet());
        when(flowManager.getGroup(parentGroup.getIdentifier())).thenReturn(parentGroup);

        serviceResolver = new StandardControllerServiceResolver(authorizer, flowManager, flowMapper,
                controllerServiceProvider, controllerServiceApiLookup);
    }

    @Test
    public void testVersionedControlledChildResolveServicesFromParent() throws IOException {
        // Load individual snapshots
        final RegisteredFlowSnapshot parent = loadSnapshot(CHILD_REFERENCES_SERVICES_FROM_PARENT_LOCATION + "/parent.json");
        final RegisteredFlowSnapshot child = loadSnapshot(CHILD_REFERENCES_SERVICES_FROM_PARENT_LOCATION + "/child.json");

        // Find the child group inside parent where the child snapshot would have been loaded, and simulate loading the contents
        final VersionedProcessGroup matchingChildGroup = findChildGroupByName(parent.getFlowContents(), child.getFlowContents().getName());
        assertNotNull(matchingChildGroup);
        matchingChildGroup.setProcessors(child.getFlowContents().getProcessors());
        matchingChildGroup.setControllerServices(child.getFlowContents().getControllerServices());

        // Create container with snapshots
        final FlowSnapshotContainer snapshotContainer = new FlowSnapshotContainer(parent);
        snapshotContainer.addChildSnapshot(child, matchingChildGroup);

        // Before resolving, verify that child processor references different service ids
        final VersionedProcessor childConvertRecord = findProcessorByName(matchingChildGroup, "ConvertRecord");
        assertNotNull(childConvertRecord);

        final String childConvertRecordReaderId = childConvertRecord.getProperties().get("record-reader");
        final String childConvertRecordWriterId = childConvertRecord.getProperties().get("record-writer");
        assertNotNull(childConvertRecordReaderId);
        assertNotNull(childConvertRecordWriterId);

        final VersionedControllerService parentReader = findServiceByName(parent.getFlowContents(), "MyAvroReader");
        final VersionedControllerService parentWriter = findServiceByName(parent.getFlowContents(), "MyAvroRecordSetWriter");
        assertNotNull(parentReader);
        assertNotNull(parentWriter);

        assertNotEquals(childConvertRecordReaderId, parentReader.getIdentifier());
        assertNotEquals(childConvertRecordWriterId, parentWriter.getIdentifier());

        // Setup the ControllerServiceAPI lookup for ConvertRecord
        final Map<String, ControllerServiceAPI> convertRecordRequiredApis = new HashMap<>();
        convertRecordRequiredApis.put("record-reader", createServiceApi("org.apache.nifi.serialization.RecordReaderFactory",
                "org.apache.nifi", "nifi-standard-services-api-nar", "1.21.0-SNAPSHOT"));
        convertRecordRequiredApis.put("record-writer", createServiceApi("org.apache.nifi.serialization.RecordSetWriterFactory",
                "org.apache.nifi", "nifi-standard-services-api-nar", "1.21.0-SNAPSHOT"));

        when(controllerServiceApiLookup.getRequiredServiceApis(childConvertRecord.getType(), childConvertRecord.getBundle()))
                .thenReturn(convertRecordRequiredApis);

        // Resolve inherited services
        serviceResolver.resolveInheritedControllerServices(snapshotContainer, parentGroup.getIdentifier(), nifiUser);

        // Verify child processor now references ids from parent
        final String resolvedConvertRecordReaderId = childConvertRecord.getProperties().get("record-reader");
        assertEquals(parentReader.getIdentifier(), resolvedConvertRecordReaderId);

        final String resolvedConvertRecordWriterId = childConvertRecord.getProperties().get("record-writer");
        assertEquals(parentWriter.getIdentifier(), resolvedConvertRecordWriterId);
    }

    @Test
    public void testExternalServiceResolvedFromOutsideSnapshot() throws IOException {
        final RegisteredFlowSnapshot snapshot = loadSnapshot(STANDARD_EXTERNAL_SERVICE_REFERENCE + "/convert-record-external-schema-registry.json");
        final FlowSnapshotContainer snapshotContainer = new FlowSnapshotContainer(snapshot);

        // Get the AvroReader and the id of the SR it is using before resolving external services
        final VersionedControllerService avroReader = findServiceByName(snapshot.getFlowContents(), "AvroReader");
        assertNotNull(avroReader);

        final String avroReaderSchemaRegistryId = avroReader.getProperties().get("schema-registry");
        assertNotNull(avroReaderSchemaRegistryId);

        // Setup the ControllerServiceAPI lookup for AvroReader
        final ControllerServiceAPI schemaRegistryServiceApi = createServiceApi("org.apache.nifi.schemaregistry.services.SchemaRegistry",
                "org.apache.nifi", "nifi-standard-services-api-nar", "1.21.0-SNAPSHOT");

        final Map<String, ControllerServiceAPI> avroReaderRequiredApis = new HashMap<>();
        avroReaderRequiredApis.put("schema-registry", schemaRegistryServiceApi);

        when(controllerServiceApiLookup.getRequiredServiceApis(avroReader.getType(), avroReader.getBundle()))
                .thenReturn(avroReaderRequiredApis);

        // Setup an existing service from the parent group that implements the same API with same name
        final ControllerServiceNode schemaRegistryServiceNode = mock(ControllerServiceNode.class);
        when(parentGroup.getControllerServices(true)).thenReturn(Collections.singleton(schemaRegistryServiceNode));
        when(schemaRegistryServiceNode.isAuthorized(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class))).thenReturn(true);

        final VersionedControllerService schemaRegistryVersionedService = new VersionedControllerService();
        schemaRegistryVersionedService.setIdentifier("external-schema-registry");
        schemaRegistryVersionedService.setName("AvroSchemaRegistry");
        schemaRegistryVersionedService.setControllerServiceApis(Collections.singletonList(schemaRegistryServiceApi));

        when(flowMapper.mapControllerService(schemaRegistryServiceNode, controllerServiceProvider, Collections.emptySet(), Collections.emptyMap()))
                .thenReturn(schemaRegistryVersionedService);

        // Resolve inherited services
        serviceResolver.resolveInheritedControllerServices(snapshotContainer, parentGroup.getIdentifier(), nifiUser);

        // Verify the SR id in the AvroReader has been resolved to the external service id
        final String avroReaderSchemaRegistryIdAfterResolution = avroReader.getProperties().get("schema-registry");
        assertNotNull(avroReaderSchemaRegistryIdAfterResolution);
        assertNotEquals(avroReaderSchemaRegistryId, avroReaderSchemaRegistryIdAfterResolution);
        assertEquals(schemaRegistryVersionedService.getIdentifier(), avroReaderSchemaRegistryIdAfterResolution);
    }

    @Test
    public void testExternalServiceNotResolvedFromOutsideSnapshotBecauseMultipleWithSameNameAndType() throws IOException {
        final RegisteredFlowSnapshot snapshot = loadSnapshot(STANDARD_EXTERNAL_SERVICE_REFERENCE + "/convert-record-external-schema-registry.json");
        final FlowSnapshotContainer snapshotContainer = new FlowSnapshotContainer(snapshot);

        // Get the AvroReader and the id of the SR it is using before resolving external services
        final VersionedControllerService avroReader = findServiceByName(snapshot.getFlowContents(), "AvroReader");
        assertNotNull(avroReader);

        final String avroReaderSchemaRegistryId = avroReader.getProperties().get("schema-registry");
        assertNotNull(avroReaderSchemaRegistryId);

        // Setup the ControllerServiceAPI lookup for AvroReader
        final ControllerServiceAPI schemaRegistryServiceApi = createServiceApi("org.apache.nifi.schemaregistry.services.SchemaRegistry",
                "org.apache.nifi", "nifi-standard-services-api-nar", "1.21.0-SNAPSHOT");

        final Map<String, ControllerServiceAPI> avroReaderRequiredApis = new HashMap<>();
        avroReaderRequiredApis.put("schema-registry", schemaRegistryServiceApi);

        when(controllerServiceApiLookup.getRequiredServiceApis(avroReader.getType(), avroReader.getBundle()))
                .thenReturn(avroReaderRequiredApis);

        // Setup two existing services from the parent group that implements the same API with same name
        final ControllerServiceNode schemaRegistryServiceNode1 = mock(ControllerServiceNode.class);
        final ControllerServiceNode schemaRegistryServiceNode2 = mock(ControllerServiceNode.class);
        when(parentGroup.getControllerServices(true)).thenReturn(new HashSet<>(Arrays.asList(schemaRegistryServiceNode1, schemaRegistryServiceNode2)));
        when(schemaRegistryServiceNode1.isAuthorized(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class))).thenReturn(true);
        when(schemaRegistryServiceNode2.isAuthorized(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class))).thenReturn(true);

        final VersionedControllerService schemaRegistryVersionedService1 = new VersionedControllerService();
        schemaRegistryVersionedService1.setIdentifier("external-schema-registry-1");
        schemaRegistryVersionedService1.setName("AvroSchemaRegistry");
        schemaRegistryVersionedService1.setControllerServiceApis(Collections.singletonList(schemaRegistryServiceApi));

        when(flowMapper.mapControllerService(schemaRegistryServiceNode1, controllerServiceProvider, Collections.emptySet(), Collections.emptyMap()))
                .thenReturn(schemaRegistryVersionedService1);

        final VersionedControllerService schemaRegistryVersionedService2 = new VersionedControllerService();
        schemaRegistryVersionedService2.setIdentifier("external-schema-registry-2");
        schemaRegistryVersionedService2.setName("AvroSchemaRegistry");
        schemaRegistryVersionedService2.setControllerServiceApis(Collections.singletonList(schemaRegistryServiceApi));

        when(flowMapper.mapControllerService(schemaRegistryServiceNode2, controllerServiceProvider, Collections.emptySet(), Collections.emptyMap()))
                .thenReturn(schemaRegistryVersionedService2);

        // Resolve inherited services
        serviceResolver.resolveInheritedControllerServices(snapshotContainer, parentGroup.getIdentifier(), nifiUser);

        // Verify the SR id in the AvroReader has not been resolved due to there being to possible choices
        final String avroReaderSchemaRegistryIdAfterResolution = avroReader.getProperties().get("schema-registry");
        assertNotNull(avroReaderSchemaRegistryIdAfterResolution);
        assertEquals(avroReaderSchemaRegistryId, avroReaderSchemaRegistryIdAfterResolution);
    }

    private RegisteredFlowSnapshot loadSnapshot(final String snapshotFile) throws IOException {
        return objectMapper.readValue(new File(snapshotFile), RegisteredFlowSnapshot.class);
    }

    private VersionedProcessGroup findChildGroupByName(final VersionedProcessGroup group, final String childGroupName) {
        if (group.getProcessGroups() == null || group.getProcessGroups().isEmpty()) {
            return null;
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            if (childGroup.getName().equals(childGroupName)) {
                return childGroup;
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            final VersionedProcessGroup matchingChild = findChildGroupByName(childGroup, childGroupName);
            if (matchingChild != null) {
                return matchingChild;
            }
        }

        return null;
    }

    private VersionedControllerService findServiceByName(final VersionedProcessGroup group, final String serviceName) {
        if (group.getControllerServices() == null) {
            return null;
        }

        return group.getControllerServices().stream()
                .filter(service -> service.getName().equals(serviceName))
                .findFirst()
                .orElse(null);
    }

    private VersionedProcessor findProcessorByName(final VersionedProcessGroup group, final String processorName) {
        if (group.getProcessors() == null) {
            return null;
        }

        return group.getProcessors().stream()
                .filter(processor -> processor.getName().equals(processorName))
                .findFirst()
                .orElse(null);
    }

    private ControllerServiceAPI createServiceApi(final String type, final String group, final String artifact, final String version) {
        final ControllerServiceAPI serviceAPI = new ControllerServiceAPI();
        serviceAPI.setType(type);
        serviceAPI.setBundle(new Bundle(group, artifact, version));
        return serviceAPI;
    }

}
