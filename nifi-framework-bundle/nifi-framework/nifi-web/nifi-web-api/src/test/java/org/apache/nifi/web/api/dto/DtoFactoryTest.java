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
package org.apache.nifi.web.api.dto;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarManifest;
import org.apache.nifi.nar.NarNode;
import org.apache.nifi.nar.NarSource;
import org.apache.nifi.nar.NarState;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DtoFactoryTest {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    void testAllowableValuesControllerService() {
        final Set<String> csIdentifiers = new HashSet<>(Arrays.asList("uuid-2", "uuid-3", "uuid-1"));
        final ControllerServiceProvider controllerServiceProvider = mock(ControllerServiceProvider.class);
        when(controllerServiceProvider.getControllerServiceIdentifiers(any(), any())).thenReturn(csIdentifiers);
        final ControllerServiceNode service1 = mock(ControllerServiceNode.class);
        final ControllerServiceNode service2 = mock(ControllerServiceNode.class);
        final ControllerServiceNode service3 = mock(ControllerServiceNode.class);
        when(service1.getIdentifier()).thenReturn("uuid-1");
        when(service2.getIdentifier()).thenReturn("uuid-2");
        when(service3.getIdentifier()).thenReturn("uuid-3");
        when(service1.getName()).thenReturn("ReaderZ");
        when(service2.getName()).thenReturn("ReaderY");
        when(service3.getName()).thenReturn("ReaderX");
        when(service1.isAuthorized(any(), any(), any())).thenReturn(Boolean.TRUE);
        when(service2.isAuthorized(any(), any(), any())).thenReturn(Boolean.TRUE);
        when(service3.isAuthorized(any(), any(), any())).thenReturn(Boolean.TRUE);
        when(controllerServiceProvider.getControllerServiceNode(eq("uuid-1"))).thenReturn(service1);
        when(controllerServiceProvider.getControllerServiceNode(eq("uuid-2"))).thenReturn(service2);
        when(controllerServiceProvider.getControllerServiceNode(eq("uuid-3"))).thenReturn(service3);

        final DtoFactory dtoFactory = new DtoFactory();
        final EntityFactory entityFactory = new EntityFactory();
        dtoFactory.setControllerServiceProvider(controllerServiceProvider);
        dtoFactory.setEntityFactory(entityFactory);
        final StandardExtensionDiscoveringManager extensionManager =
                new StandardExtensionDiscoveringManager(Collections.singleton(ControllerService.class));
        extensionManager.discoverExtensions(
                Collections.singleton(SystemBundle.create(".", getClass().getClassLoader())));
        dtoFactory.setExtensionManager(extensionManager);

        final PropertyDescriptor propertyDescriptor = new PropertyDescriptor.Builder()
                .name("reader")
                .identifiesControllerService(ControllerService.class)
                .build();
        final PropertyDescriptorDTO dto = dtoFactory.createPropertyDescriptorDto(propertyDescriptor, null);
        final List<AllowableValueEntity> allowableValues = dto.getAllowableValues();
        final List<String> namesActual = allowableValues.stream()
                .map(v -> v.getAllowableValue().getDisplayName()).collect(Collectors.toList());
        logger.trace("{}", namesActual);
        assertEquals(Arrays.asList("ReaderX", "ReaderY", "ReaderZ"), namesActual);
    }

    @Test
    void testAllowableValuesFixed() {
        // from "org.apache.nifi.processors.aws.s3.AbstractS3Processor"
        final PropertyDescriptor signerOverride = new PropertyDescriptor.Builder()
                .name("Signer Override")
                .allowableValues(
                        new AllowableValue("Default Signature", "Default Signature"),
                        new AllowableValue("AWSS3V4SignerType", "Signature v4"),
                        new AllowableValue("S3SignerType", "Signature v2"))
                .defaultValue("Default Signature")
                .build();

        final DtoFactory dtoFactory = new DtoFactory();
        final EntityFactory entityFactory = new EntityFactory();
        dtoFactory.setEntityFactory(entityFactory);
        final PropertyDescriptorDTO dto = dtoFactory.createPropertyDescriptorDto(signerOverride, null);
        final List<AllowableValueEntity> allowableValues = dto.getAllowableValues();
        final List<String> namesActual = allowableValues.stream()
                .map(v -> v.getAllowableValue().getDisplayName()).collect(Collectors.toList());
        logger.trace("{}", namesActual);
        assertEquals(Arrays.asList("Default Signature", "Signature v4", "Signature v2"), namesActual);
    }

    @Test
    public void testCreateNarSummaryDtoWhenInstalled() {
        final NarManifest narManifest = NarManifest.builder()
                .group("com.foo")
                .id("my-processor")
                .version("1.0.0")
                .buildTimestamp("2024-01-26T00:11:29Z")
                .createdBy("Maven NAR Plugin")
                .build();

        final NarNode narNode = NarNode.builder()
                .identifier(UUID.randomUUID().toString())
                .narFile(new File("does-not-ext"))
                .narFileDigest("nar-digest")
                .manifest(narManifest)
                .source(NarSource.UPLOAD)
                .sourceIdentifier("1234")
                .state(NarState.INSTALLED)
                .build();

        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        when(extensionManager.getTypes(narManifest.getCoordinate())).thenReturn(Collections.emptySet());

        final DtoFactory dtoFactory = new DtoFactory();
        dtoFactory.setExtensionManager(extensionManager);

        final NarSummaryDTO summaryDTO = dtoFactory.createNarSummaryDto(narNode);
        assertEquals(narNode.getIdentifier(), summaryDTO.getIdentifier());
        assertEquals(narManifest.getBuildTimestamp(), summaryDTO.getBuildTime());
        assertEquals(narManifest.getCreatedBy(), summaryDTO.getCreatedBy());
        assertEquals(narNode.getNarFileDigest(), summaryDTO.getDigest());
        assertEquals(narNode.getState().getValue(), summaryDTO.getState());
        assertEquals(narNode.getSource().name(), summaryDTO.getSourceType());
        assertEquals(narNode.getSourceIdentifier(), summaryDTO.getSourceIdentifier());
        assertEquals(0, summaryDTO.getExtensionCount());
        assertTrue(summaryDTO.isInstallComplete());
        assertNull(summaryDTO.getFailureMessage());
        assertNull(summaryDTO.getDependencyCoordinate());

        final NarCoordinateDTO coordinateDTO = summaryDTO.getCoordinate();
        verifyCoordinateDTO(narManifest, coordinateDTO);
    }

    @Test
    public void testCreateNarSummaryDtoWhenInstalling() {
        final NarManifest narManifest = NarManifest.builder()
                .group("com.foo")
                .id("my-processor")
                .version("1.0.0")
                .dependencyGroup("com.dependency")
                .dependencyId("my-dependency")
                .dependencyVersion("2.0.0")
                .buildTimestamp("2024-01-26T00:11:29Z")
                .createdBy("Maven NAR Plugin")
                .build();

        final NarNode narNode = NarNode.builder()
                .identifier(UUID.randomUUID().toString())
                .narFile(new File("does-not-ext"))
                .narFileDigest("nar-digest")
                .manifest(narManifest)
                .source(NarSource.UPLOAD)
                .sourceIdentifier("1234")
                .state(NarState.INSTALLING)
                .build();

        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        when(extensionManager.getTypes(narManifest.getCoordinate())).thenReturn(Collections.emptySet());

        final DtoFactory dtoFactory = new DtoFactory();
        dtoFactory.setExtensionManager(extensionManager);

        final NarSummaryDTO summaryDTO = dtoFactory.createNarSummaryDto(narNode);
        assertEquals(narNode.getIdentifier(), summaryDTO.getIdentifier());
        assertEquals(narManifest.getBuildTimestamp(), summaryDTO.getBuildTime());
        assertEquals(narManifest.getCreatedBy(), summaryDTO.getCreatedBy());
        assertEquals(narNode.getNarFileDigest(), summaryDTO.getDigest());
        assertEquals(narNode.getState().getValue(), summaryDTO.getState());
        assertEquals(narNode.getSource().name(), summaryDTO.getSourceType());
        assertEquals(narNode.getSourceIdentifier(), summaryDTO.getSourceIdentifier());
        assertEquals(0, summaryDTO.getExtensionCount());
        assertFalse(summaryDTO.isInstallComplete());
        assertNull(summaryDTO.getFailureMessage());

        final NarCoordinateDTO coordinateDTO = summaryDTO.getCoordinate();
        verifyCoordinateDTO(narManifest, coordinateDTO);

        final NarCoordinateDTO dependencyCoordinateDTO = summaryDTO.getDependencyCoordinate();
        verifyDependencyCoordinateDTO(narManifest, dependencyCoordinateDTO);
    }

    private void verifyCoordinateDTO(final NarManifest narManifest, final NarCoordinateDTO coordinateDTO) {
        assertEquals(narManifest.getGroup(), coordinateDTO.getGroup());
        assertEquals(narManifest.getId(), coordinateDTO.getArtifact());
        assertEquals(narManifest.getVersion(), coordinateDTO.getVersion());
    }

    private void verifyDependencyCoordinateDTO(final NarManifest narManifest, final NarCoordinateDTO coordinateDTO) {
        assertEquals(narManifest.getDependencyGroup(), coordinateDTO.getGroup());
        assertEquals(narManifest.getDependencyId(), coordinateDTO.getArtifact());
        assertEquals(narManifest.getDependencyVersion(), coordinateDTO.getVersion());
    }

    private Bundle createBundle(final String group, final String id, final String version) {
        final BundleCoordinate coordinate = new BundleCoordinate(group, id, version);
        final BundleDetails details = new BundleDetails.Builder()
                .workingDir(new File("."))
                .coordinate(coordinate)
                .build();
        return new Bundle(details, getClass().getClassLoader());
    }

    @Test
    void testControllerServiceMultipleVersionsAvailableGhostWithOneCompatibleBundle() {
        final String group = "com.example";
        final String id = "test-service";

        final BundleCoordinate currentCoordinate = new BundleCoordinate(group, id, "1.0.0");
        final Bundle compatible = createBundle(group, id, "1.1.0");

        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final String canonicalClassName = "com.example.ControllerService";
        when(extensionManager.getBundles(canonicalClassName)).thenReturn(Collections.singletonList(compatible));

        final ControllerServiceNode serviceNode = mock(ControllerServiceNode.class);
        when(serviceNode.getIdentifier()).thenReturn("svc-1");
        when(serviceNode.getName()).thenReturn("Service");
        when(serviceNode.getCanonicalClassName()).thenReturn(canonicalClassName);
        when(serviceNode.getBundleCoordinate()).thenReturn(currentCoordinate);
        when(serviceNode.getAnnotationData()).thenReturn(null);
        when(serviceNode.getComments()).thenReturn(null);
        when(serviceNode.getBulletinLevel()).thenReturn(LogLevel.INFO);
        when(serviceNode.getState()).thenReturn(ControllerServiceState.DISABLED);
        when(serviceNode.isSupportsSensitiveDynamicProperties()).thenReturn(false);
        when(serviceNode.isRestricted()).thenReturn(false);
        when(serviceNode.isDeprecated()).thenReturn(false);
        when(serviceNode.isExtensionMissing()).thenReturn(true); // ghost component
        when(serviceNode.getVersionedComponentId()).thenReturn(java.util.Optional.empty());
        when(serviceNode.getRawPropertyValues()).thenReturn(Collections.emptyMap());
        final ControllerService controllerService = mock(ControllerService.class);
        when(controllerService.getPropertyDescriptors()).thenReturn(Collections.emptyList());
        when(serviceNode.getControllerServiceImplementation()).thenReturn(controllerService);
        when(serviceNode.getValidationStatus(anyLong(), any())).thenReturn(ValidationStatus.VALID);
        when(serviceNode.getValidationErrors()).thenReturn(Collections.emptyList());

        final DtoFactory dtoFactory = new DtoFactory();
        dtoFactory.setExtensionManager(extensionManager);

        final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(serviceNode);
        assertTrue(dto.getMultipleVersionsAvailable(), "Ghost service with one compatible bundle should allow change version");
    }

    @Test
    void testControllerServiceMultipleVersionsAvailableNotGhostWithOneCompatibleBundle() {
        final String group = "com.example";
        final String id = "test-service";

        final BundleCoordinate currentCoordinate = new BundleCoordinate(group, id, "1.0.0");
        final Bundle compatible = createBundle(group, id, "1.1.0");

        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final String canonicalClassName = "com.example.ControllerService";
        when(extensionManager.getBundles(canonicalClassName)).thenReturn(Collections.singletonList(compatible));

        final ControllerServiceNode serviceNode = mock(ControllerServiceNode.class);
        when(serviceNode.getIdentifier()).thenReturn("svc-1");
        when(serviceNode.getName()).thenReturn("Service");
        when(serviceNode.getCanonicalClassName()).thenReturn(canonicalClassName);
        when(serviceNode.getBundleCoordinate()).thenReturn(currentCoordinate);
        when(serviceNode.getAnnotationData()).thenReturn(null);
        when(serviceNode.getComments()).thenReturn(null);
        when(serviceNode.getBulletinLevel()).thenReturn(LogLevel.INFO);
        when(serviceNode.getState()).thenReturn(ControllerServiceState.DISABLED);
        when(serviceNode.isSupportsSensitiveDynamicProperties()).thenReturn(false);
        when(serviceNode.isRestricted()).thenReturn(false);
        when(serviceNode.isDeprecated()).thenReturn(false);
        when(serviceNode.isExtensionMissing()).thenReturn(false); // not ghost
        when(serviceNode.getVersionedComponentId()).thenReturn(java.util.Optional.empty());
        when(serviceNode.getRawPropertyValues()).thenReturn(Collections.emptyMap());
        final ControllerService controllerService = mock(ControllerService.class);
        when(controllerService.getPropertyDescriptors()).thenReturn(Collections.emptyList());
        when(serviceNode.getControllerServiceImplementation()).thenReturn(controllerService);
        when(serviceNode.getValidationStatus(anyLong(), any())).thenReturn(ValidationStatus.VALID);
        when(serviceNode.getValidationErrors()).thenReturn(Collections.emptyList());

        final DtoFactory dtoFactory = new DtoFactory();
        dtoFactory.setExtensionManager(extensionManager);

        final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(serviceNode);
        assertFalse(dto.getMultipleVersionsAvailable(), "Non-ghost service with one compatible bundle should not allow change version");
    }

    @Test
    void testControllerServiceMultipleVersionsAvailableNotGhostWithTwoCompatibleBundles() {
        final String group = "com.example";
        final String id = "test-service";

        final BundleCoordinate currentCoordinate = new BundleCoordinate(group, id, "1.0.0");
        final Bundle compatible1 = createBundle(group, id, "1.1.0");
        final Bundle compatible2 = createBundle(group, id, "1.2.0");

        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final String canonicalClassName = "com.example.ControllerService";
        when(extensionManager.getBundles(canonicalClassName)).thenReturn(Arrays.asList(compatible1, compatible2));

        final ControllerServiceNode serviceNode = mock(ControllerServiceNode.class);
        when(serviceNode.getIdentifier()).thenReturn("svc-1");
        when(serviceNode.getName()).thenReturn("Service");
        when(serviceNode.getCanonicalClassName()).thenReturn(canonicalClassName);
        when(serviceNode.getBundleCoordinate()).thenReturn(currentCoordinate);
        when(serviceNode.getAnnotationData()).thenReturn(null);
        when(serviceNode.getComments()).thenReturn(null);
        when(serviceNode.getBulletinLevel()).thenReturn(LogLevel.INFO);
        when(serviceNode.getState()).thenReturn(ControllerServiceState.DISABLED);
        when(serviceNode.isSupportsSensitiveDynamicProperties()).thenReturn(false);
        when(serviceNode.isRestricted()).thenReturn(false);
        when(serviceNode.isDeprecated()).thenReturn(false);
        when(serviceNode.isExtensionMissing()).thenReturn(false); // not ghost
        when(serviceNode.getVersionedComponentId()).thenReturn(java.util.Optional.empty());
        when(serviceNode.getRawPropertyValues()).thenReturn(Collections.emptyMap());
        final ControllerService controllerService = mock(ControllerService.class);
        when(controllerService.getPropertyDescriptors()).thenReturn(Collections.emptyList());
        when(serviceNode.getControllerServiceImplementation()).thenReturn(controllerService);
        when(serviceNode.getValidationStatus(anyLong(), any())).thenReturn(ValidationStatus.VALID);
        when(serviceNode.getValidationErrors()).thenReturn(Collections.emptyList());

        final DtoFactory dtoFactory = new DtoFactory();
        dtoFactory.setExtensionManager(extensionManager);

        final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(serviceNode);
        assertTrue(dto.getMultipleVersionsAvailable(), "Non-ghost service with two compatible bundles should allow change version");
    }

    @Test
    void testFlowRegistryClientMultipleVersionsAvailableGhostWithOneCompatibleBundle() {
        final String group = "com.example";
        final String id = "test-registry-client";

        final BundleCoordinate currentCoordinate = new BundleCoordinate(group, id, "1.0.0");
        final Bundle compatible = createBundle(group, id, "1.1.0");

        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final String canonicalClassName = "com.example.FlowRegistryClient";
        when(extensionManager.getBundles(canonicalClassName)).thenReturn(Collections.singletonList(compatible));

        final FlowRegistryClientNode clientNode = mock(FlowRegistryClientNode.class);
        when(clientNode.getIdentifier()).thenReturn("client-1");
        when(clientNode.getName()).thenReturn("Client");
        when(clientNode.getDescription()).thenReturn("desc");
        when(clientNode.getCanonicalClassName()).thenReturn(canonicalClassName);
        when(clientNode.getBundleCoordinate()).thenReturn(currentCoordinate);
        when(clientNode.getAnnotationData()).thenReturn(null);
        when(clientNode.isSupportsSensitiveDynamicProperties()).thenReturn(false);
        when(clientNode.isBranchingSupported()).thenReturn(false);
        when(clientNode.isRestricted()).thenReturn(false);
        when(clientNode.isDeprecated()).thenReturn(false);
        when(clientNode.isExtensionMissing()).thenReturn(true); // ghost component
        when(clientNode.getRawPropertyValues()).thenReturn(Collections.emptyMap());
        when(clientNode.getPropertyDescriptors()).thenReturn(Collections.emptyList());
        when(clientNode.getValidationStatus(anyLong(), any())).thenReturn(ValidationStatus.VALID);
        when(clientNode.getValidationErrors()).thenReturn(Collections.emptyList());

        final DtoFactory dtoFactory = new DtoFactory();
        dtoFactory.setExtensionManager(extensionManager);

        final FlowRegistryClientDTO dto = dtoFactory.createRegistryDto(clientNode);
        assertTrue(dto.getMultipleVersionsAvailable(), "Ghost registry client with one compatible bundle should allow change version");
    }

    @Test
    void testCreateConnectionDtoWithRetriedRelationships() {
        // Set up test relationships
        final Relationship successRelationship = new Relationship.Builder().name("success").build();
        final Relationship failureRelationship = new Relationship.Builder().name("failure").build();
        final Relationship retryRelationship = new Relationship.Builder().name("retry").build();

        // Mock the process group
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getIdentifier()).thenReturn("group-id");

        // Mock the source connectable
        final Connectable sourceConnectable = mock(Connectable.class);
        when(sourceConnectable.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(sourceConnectable.getIdentifier()).thenReturn("source-id");
        when(sourceConnectable.getName()).thenReturn("Source Processor");
        when(sourceConnectable.getProcessGroupIdentifier()).thenReturn("group-id");
        when(sourceConnectable.getProcessGroup()).thenReturn(processGroup);
        // Configure retry settings: only "failure" and "retry" relationships are retried
        when(sourceConnectable.isRelationshipRetried(successRelationship)).thenReturn(false);
        when(sourceConnectable.isRelationshipRetried(failureRelationship)).thenReturn(true);
        when(sourceConnectable.isRelationshipRetried(retryRelationship)).thenReturn(true);

        // Mock the destination connectable
        final Connectable destinationConnectable = mock(Connectable.class);
        when(destinationConnectable.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(destinationConnectable.getIdentifier()).thenReturn("dest-id");
        when(destinationConnectable.getName()).thenReturn("Destination Processor");
        when(destinationConnectable.getProcessGroupIdentifier()).thenReturn("group-id");
        when(destinationConnectable.getProcessGroup()).thenReturn(processGroup);

        // Mock the flow file queue
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(flowFileQueue.getBackPressureObjectThreshold()).thenReturn(10000L);
        when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn("1 GB");
        when(flowFileQueue.getFlowFileExpiration()).thenReturn("0 sec");
        when(flowFileQueue.getPriorities()).thenReturn(Collections.emptyList());
        when(flowFileQueue.getLoadBalanceStrategy()).thenReturn(LoadBalanceStrategy.DO_NOT_LOAD_BALANCE);
        when(flowFileQueue.getPartitioningAttribute()).thenReturn(null);
        when(flowFileQueue.getLoadBalanceCompression()).thenReturn(LoadBalanceCompression.DO_NOT_COMPRESS);
        when(flowFileQueue.isActivelyLoadBalancing()).thenReturn(false);

        // Mock the connection - only "failure" and "retry" are selected (success is not selected)
        final Connection connection = mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("connection-id");
        when(connection.getName()).thenReturn("Test Connection");
        when(connection.getSource()).thenReturn(sourceConnectable);
        when(connection.getDestination()).thenReturn(destinationConnectable);
        when(connection.getProcessGroup()).thenReturn(processGroup);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(connection.getRelationships()).thenReturn(Arrays.asList(failureRelationship, retryRelationship));
        when(connection.getBendPoints()).thenReturn(Collections.emptyList());
        when(connection.getLabelIndex()).thenReturn(0);
        when(connection.getZIndex()).thenReturn(0L);
        when(connection.getVersionedComponentId()).thenReturn(java.util.Optional.empty());

        // Create the DTO factory
        final DtoFactory dtoFactory = new DtoFactory();

        // Test the creation of ConnectionDTO
        final ConnectionDTO connectionDto = dtoFactory.createConnectionDto(connection);

        // Assertions
        assertEquals("connection-id", connectionDto.getId());
        assertEquals("Test Connection", connectionDto.getName());

        // Verify selected relationships
        assertEquals(2, connectionDto.getSelectedRelationships().size());
        assertTrue(connectionDto.getSelectedRelationships().contains("failure"));
        assertTrue(connectionDto.getSelectedRelationships().contains("retry"));

        // Verify retried relationships - should only contain the selected relationships that are also retried
        assertEquals(2, connectionDto.getRetriedRelationships().size());
        assertTrue(connectionDto.getRetriedRelationships().contains("failure"));
        assertTrue(connectionDto.getRetriedRelationships().contains("retry"));
    }

    @Test
    void testCreateConnectionDtoWithPartiallyRetriedRelationships() {
        // Set up test relationships
        final Relationship successRelationship = new Relationship.Builder().name("success").build();
        final Relationship failureRelationship = new Relationship.Builder().name("failure").build();

        // Mock the process group
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getIdentifier()).thenReturn("group-id");

        // Mock the source connectable
        final Connectable sourceConnectable = mock(Connectable.class);
        when(sourceConnectable.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(sourceConnectable.getIdentifier()).thenReturn("source-id");
        when(sourceConnectable.getName()).thenReturn("Source Processor");
        when(sourceConnectable.getProcessGroupIdentifier()).thenReturn("group-id");
        when(sourceConnectable.getProcessGroup()).thenReturn(processGroup);
        // Configure retry settings: only "failure" relationship is retried, "success" is not
        when(sourceConnectable.isRelationshipRetried(successRelationship)).thenReturn(false);
        when(sourceConnectable.isRelationshipRetried(failureRelationship)).thenReturn(true);

        // Mock the destination connectable
        final Connectable destinationConnectable = mock(Connectable.class);
        when(destinationConnectable.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(destinationConnectable.getIdentifier()).thenReturn("dest-id");
        when(destinationConnectable.getName()).thenReturn("Destination Processor");
        when(destinationConnectable.getProcessGroupIdentifier()).thenReturn("group-id");
        when(destinationConnectable.getProcessGroup()).thenReturn(processGroup);

        // Mock the flow file queue
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(flowFileQueue.getBackPressureObjectThreshold()).thenReturn(10000L);
        when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn("1 GB");
        when(flowFileQueue.getFlowFileExpiration()).thenReturn("0 sec");
        when(flowFileQueue.getPriorities()).thenReturn(Collections.emptyList());
        when(flowFileQueue.getLoadBalanceStrategy()).thenReturn(LoadBalanceStrategy.DO_NOT_LOAD_BALANCE);
        when(flowFileQueue.getPartitioningAttribute()).thenReturn(null);
        when(flowFileQueue.getLoadBalanceCompression()).thenReturn(LoadBalanceCompression.DO_NOT_COMPRESS);
        when(flowFileQueue.isActivelyLoadBalancing()).thenReturn(false);

        // Mock the connection - both relationships are selected
        final Connection connection = mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("connection-id");
        when(connection.getName()).thenReturn("Test Connection");
        when(connection.getSource()).thenReturn(sourceConnectable);
        when(connection.getDestination()).thenReturn(destinationConnectable);
        when(connection.getProcessGroup()).thenReturn(processGroup);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(connection.getRelationships()).thenReturn(Arrays.asList(successRelationship, failureRelationship));
        when(connection.getBendPoints()).thenReturn(Collections.emptyList());
        when(connection.getLabelIndex()).thenReturn(0);
        when(connection.getZIndex()).thenReturn(0L);
        when(connection.getVersionedComponentId()).thenReturn(java.util.Optional.empty());

        // Create the DTO factory
        final DtoFactory dtoFactory = new DtoFactory();

        // Test the creation of ConnectionDTO
        final ConnectionDTO connectionDto = dtoFactory.createConnectionDto(connection);

        // Assertions
        assertEquals("connection-id", connectionDto.getId());
        assertEquals("Test Connection", connectionDto.getName());

        // Verify selected relationships - should contain both
        assertEquals(2, connectionDto.getSelectedRelationships().size());
        assertTrue(connectionDto.getSelectedRelationships().contains("success"));
        assertTrue(connectionDto.getSelectedRelationships().contains("failure"));

        // Verify retried relationships - should only contain "failure" since "success" is not retried
        assertEquals(1, connectionDto.getRetriedRelationships().size());
        assertTrue(connectionDto.getRetriedRelationships().contains("failure"));
        assertFalse(connectionDto.getRetriedRelationships().contains("success"));
    }

    @Test
    void testCreateConnectionDtoWithNoRetriedRelationships() {
        // Set up test relationships
        final Relationship successRelationship = new Relationship.Builder().name("success").build();
        final Relationship failureRelationship = new Relationship.Builder().name("failure").build();

        // Mock the process group
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getIdentifier()).thenReturn("group-id");

        // Mock the source connectable
        final Connectable sourceConnectable = mock(Connectable.class);
        when(sourceConnectable.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(sourceConnectable.getIdentifier()).thenReturn("source-id");
        when(sourceConnectable.getName()).thenReturn("Source Processor");
        when(sourceConnectable.getProcessGroupIdentifier()).thenReturn("group-id");
        when(sourceConnectable.getProcessGroup()).thenReturn(processGroup);
        // Configure retry settings: no relationships are retried
        when(sourceConnectable.isRelationshipRetried(successRelationship)).thenReturn(false);
        when(sourceConnectable.isRelationshipRetried(failureRelationship)).thenReturn(false);

        // Mock the destination connectable
        final Connectable destinationConnectable = mock(Connectable.class);
        when(destinationConnectable.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(destinationConnectable.getIdentifier()).thenReturn("dest-id");
        when(destinationConnectable.getName()).thenReturn("Destination Processor");
        when(destinationConnectable.getProcessGroupIdentifier()).thenReturn("group-id");
        when(destinationConnectable.getProcessGroup()).thenReturn(processGroup);

        // Mock the flow file queue
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(flowFileQueue.getBackPressureObjectThreshold()).thenReturn(10000L);
        when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn("1 GB");
        when(flowFileQueue.getFlowFileExpiration()).thenReturn("0 sec");
        when(flowFileQueue.getPriorities()).thenReturn(Collections.emptyList());
        when(flowFileQueue.getLoadBalanceStrategy()).thenReturn(LoadBalanceStrategy.DO_NOT_LOAD_BALANCE);
        when(flowFileQueue.getPartitioningAttribute()).thenReturn(null);
        when(flowFileQueue.getLoadBalanceCompression()).thenReturn(LoadBalanceCompression.DO_NOT_COMPRESS);
        when(flowFileQueue.isActivelyLoadBalancing()).thenReturn(false);

        // Mock the connection
        final Connection connection = mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("connection-id");
        when(connection.getName()).thenReturn("Test Connection");
        when(connection.getSource()).thenReturn(sourceConnectable);
        when(connection.getDestination()).thenReturn(destinationConnectable);
        when(connection.getProcessGroup()).thenReturn(processGroup);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(connection.getRelationships()).thenReturn(Arrays.asList(successRelationship, failureRelationship));
        when(connection.getBendPoints()).thenReturn(Collections.emptyList());
        when(connection.getLabelIndex()).thenReturn(0);
        when(connection.getZIndex()).thenReturn(0L);
        when(connection.getVersionedComponentId()).thenReturn(java.util.Optional.empty());

        // Create the DTO factory
        final DtoFactory dtoFactory = new DtoFactory();

        // Test the creation of ConnectionDTO
        final ConnectionDTO connectionDto = dtoFactory.createConnectionDto(connection);

        // Assertions
        assertEquals("connection-id", connectionDto.getId());
        assertEquals("Test Connection", connectionDto.getName());

        // Verify selected relationships
        assertEquals(2, connectionDto.getSelectedRelationships().size());
        assertTrue(connectionDto.getSelectedRelationships().contains("success"));
        assertTrue(connectionDto.getSelectedRelationships().contains("failure"));

        // Verify retried relationships - should be null since no relationships are retried
        assertNull(connectionDto.getRetriedRelationships());
    }

    @Test
    void testConnectionDtoCopy() {
        // Create original ConnectionDTO with retried relationships
        final ConnectionDTO original = new ConnectionDTO();
        original.setId("connection-id");
        original.setName("Test Connection");
        original.setSelectedRelationships(new HashSet<>(Arrays.asList("success", "failure", "retry")));
        original.setAvailableRelationships(new HashSet<>(Arrays.asList("success", "failure", "retry")));
        original.setRetriedRelationships(new HashSet<>(Arrays.asList("failure", "retry")));

        // Create the DTO factory
        final DtoFactory dtoFactory = new DtoFactory();

        // Test the copy method
        final ConnectionDTO copy = dtoFactory.copy(original);

        // Assertions
        assertEquals(original.getId(), copy.getId());
        assertEquals(original.getName(), copy.getName());
        assertEquals(original.getSelectedRelationships(), copy.getSelectedRelationships());
        assertEquals(original.getAvailableRelationships(), copy.getAvailableRelationships());
        assertEquals(original.getRetriedRelationships(), copy.getRetriedRelationships());

        // Verify they are separate objects (deep copy)
        assertNotSame(original.getSelectedRelationships(), copy.getSelectedRelationships());
        assertNotSame(original.getAvailableRelationships(), copy.getAvailableRelationships());
        assertNotSame(original.getRetriedRelationships(), copy.getRetriedRelationships());
    }
}
