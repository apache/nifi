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
package org.apache.nifi.flow.synchronization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.BackoffMechanism;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.groups.ComponentIdGenerator;
import org.apache.nifi.groups.ComponentScheduler;
import org.apache.nifi.groups.FlowFileConcurrency;
import org.apache.nifi.groups.FlowFileOutboundPolicy;
import org.apache.nifi.groups.FlowSynchronizationOptions;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ScheduledStateChangeListener;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.StandardParameterContextManager;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Reproduces NIFI-14944: After upgrading a versioned PG where v1 referenced external (root) Controller Services
 * and v2 adds scoped services, the processor continues to reference the root services instead of the new scoped ones.
 */
public class ScopedControllerServiceUpgradeTest {

    private static final String CONVERT_RECORD_TYPE = "org.apache.nifi.processors.standard.ConvertRecord";

    private StandardVersionedComponentSynchronizer synchronizer;
    private FlowSynchronizationOptions syncOptions;
    private FlowManager flowManager;
    private ControllerServiceProvider controllerServiceProvider;
    private ComponentScheduler componentScheduler;

    private ProcessGroup rootGroup;
    private ProcessGroup processGroup;

    private final Set<ControllerServiceNode> rootServices = new HashSet<>();
    private final Set<ControllerServiceNode> groupServices = new HashSet<>();

    private final Set<ProcessorNode> processors = new HashSet<>();

    private Map<String, String> convertRecordCurrentProps = new HashMap<>();

    @BeforeEach
    public void init() {
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        flowManager = mock(FlowManager.class);
        controllerServiceProvider = mock(ControllerServiceProvider.class);
        componentScheduler = mock(ComponentScheduler.class);

        final ReloadComponent reloadComponent = mock(ReloadComponent.class);
        final ParameterContextManager parameterContextManager = new StandardParameterContextManager();
        when(flowManager.getParameterContextManager()).thenReturn(parameterContextManager);
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(flowManager).withParameterContextResolution(any(Runnable.class));

        doAnswer(invocation -> null).when(componentScheduler).pause();
        doAnswer(invocation -> null).when(componentScheduler).resume();
        doAnswer(invocation -> CompletableFuture.completedFuture(null)).when(componentScheduler).enableControllerServicesAsync(anySet());
        doAnswer(invocation -> null).when(componentScheduler).transitionComponentState(any(RemoteGroupPort.class), any(ScheduledState.class));
        doAnswer(invocation -> null).when(componentScheduler).transitionComponentState(any(Connectable.class), any(ScheduledState.class));

        rootGroup = mock(ProcessGroup.class);

        when(rootGroup.getIdentifier()).thenReturn("root-pg");
        when(rootGroup.getControllerServices(false)).thenAnswer(inv -> new HashSet<>(rootServices));
        when(rootGroup.findControllerService(any(), anyBoolean(), anyBoolean())).thenAnswer(inv -> {
            final String id = inv.getArgument(0, String.class);
            return rootServices.stream().filter(cs -> id.equals(cs.getIdentifier())).findFirst().orElse(null);
        });

        processGroup = mock(ProcessGroup.class);

        when(processGroup.getIdentifier()).thenReturn("flow-contents-group");
        when(processGroup.getParent()).thenReturn(rootGroup);
        when(processGroup.getControllerServices(false)).thenAnswer(inv -> new HashSet<>(groupServices));
        when(processGroup.getProcessors()).thenAnswer(inv -> new HashSet<>(processors));
        when(processGroup.getProcessGroups()).thenReturn(Collections.emptySet());
        when(processGroup.getInputPorts()).thenReturn(Collections.emptySet());
        when(processGroup.getOutputPorts()).thenReturn(Collections.emptySet());
        when(processGroup.getFunnels()).thenReturn(Collections.emptySet());
        when(processGroup.getRemoteProcessGroups()).thenReturn(Collections.emptySet());
        when(processGroup.getConnections()).thenReturn(Collections.emptySet());
        when(processGroup.getFlowFileConcurrency()).thenReturn(FlowFileConcurrency.UNBOUNDED);
        when(processGroup.getFlowFileOutboundPolicy()).thenReturn(FlowFileOutboundPolicy.STREAM_WHEN_AVAILABLE);
        when(processGroup.getExecutionEngine()).thenReturn(ExecutionEngine.STANDARD);
        when(processGroup.getPosition()).thenReturn(new Position(0, 0));

        doAnswer(inv -> {
            final ControllerServiceNode cs = inv.getArgument(0, ControllerServiceNode.class);
            groupServices.add(cs);
            return null;
        }).when(processGroup).addControllerService(any(ControllerServiceNode.class));

        doAnswer(inv -> {
            final ProcessorNode p = inv.getArgument(0, ProcessorNode.class);
            processors.add(p);
            return null;
        }).when(processGroup).addProcessor(any(ProcessorNode.class));

        processors.add(createBaselineMockProcessor());

        // Create root (external) Controller Services with versioned IDs from v1
        rootServices.add(createControllerServiceNode("root-writer-id", Optional.of("f4ee3ffe-6778-3a00-9ab9-6d474f5aed00")));
        rootServices.add(createControllerServiceNode("root-reader-id", Optional.of("e39eadb7-505e-3117-8a88-ab963dfcf5d2")));

        when(flowManager.createControllerService(any(), any(), any(BundleCoordinate.class), anySet(), anyBoolean(), anyBoolean(), any()))
                .thenAnswer(inv -> {
                    // Create a new CS node mock with default (no versioned id yet); synchronizer will set properties after
                    return createControllerServiceNode(UUID.randomUUID().toString(), Optional.empty());
                });

        when(flowManager.createProcessor(any(), any(), any(BundleCoordinate.class), eq(true)))
            .thenAnswer(inv -> {
                final String type = inv.getArgument(0, String.class);
                    final ProcessorNode node = mock(ProcessorNode.class);
                instrumentProcessorNode(node, type);
                if (CONVERT_RECORD_TYPE.equals(type)) {
                    doAnswer(a -> {
                        final Map<String, String> props = a.getArgument(0);
                        convertRecordCurrentProps = new HashMap<>(props);
                        return null;
                    }).when(node).setProperties(anyMap(), eq(true), anySet());
                    when(node.getEffectivePropertyValue(any(PropertyDescriptor.class))).thenAnswer(ev -> {
                        final PropertyDescriptor d = ev.getArgument(0);
                        return convertRecordCurrentProps.get(d.getName());
                    });
                } else {
                    doAnswer(a -> null).when(node).setProperties(anyMap(), eq(true), anySet());
                }
                return node;
            });

        doAnswer(inv -> null).when(flowManager).onConnectionAdded(any());
        when(flowManager.createConnection(any(), any(), any(), any(), anySet())).then(inv -> {
            final Connection connection = mock(Connection.class);
            when(connection.getIdentifier()).thenReturn(UUID.randomUUID().toString());
            when(connection.getVersionedComponentId()).thenReturn(Optional.empty());
            // FlowFileQueue mock
            final FlowFileQueue queue = mock(FlowFileQueue.class);
            when(queue.isEmpty()).thenReturn(true);
            when(connection.getFlowFileQueue()).thenReturn(queue);
            return connection;
        });

        final VersionedFlowSynchronizationContext context = new VersionedFlowSynchronizationContext.Builder()
            .componentIdGenerator(proposedIdOrInstance())
            .componentScheduler(componentScheduler)
            .extensionManager(extensionManager)
            .flowManager(flowManager)
            .controllerServiceProvider(controllerServiceProvider)
            .flowMappingOptions(FlowMappingOptions.DEFAULT_OPTIONS)
            .processContextFactory(proc -> mock(ProcessContext.class))
            .configurationContextFactory(node -> null)
            .reloadComponent(reloadComponent)
            .build();

        synchronizer = new StandardVersionedComponentSynchronizer(context);

        syncOptions = new FlowSynchronizationOptions.Builder()
            .componentIdGenerator(proposedIdOrInstance())
            .componentComparisonIdLookup(v -> v.getIdentifier())
            .componentScheduler(componentScheduler)
            .scheduledStateChangeListener(new NoopScheduledStateChangeListener())
            .build();
    }

    @Test
    public void testNestedProcessorReferencesParentScopedServicesAfterUpgrade() throws Exception {
        // Create PG2 (child group of PG1) and register it on PG1
        final ProcessGroup pg2 = mock(ProcessGroup.class);
        when(pg2.getIdentifier()).thenReturn("pg2-instance");
        when(pg2.getVersionedComponentId()).thenReturn(java.util.Optional.of("pg2-id"));
        when(pg2.getParent()).thenReturn(processGroup);
        when(pg2.getControllerServices(false)).thenReturn(Collections.emptySet());

        final Set<ProcessorNode> pg2Processors = new HashSet<>();
        when(pg2.getProcessors()).thenAnswer(inv -> new HashSet<>(pg2Processors));
        when(pg2.getInputPorts()).thenReturn(Collections.emptySet());
        when(pg2.getOutputPorts()).thenReturn(Collections.emptySet());
        when(pg2.getFunnels()).thenReturn(Collections.emptySet());
        when(pg2.getRemoteProcessGroups()).thenReturn(Collections.emptySet());
        when(pg2.getConnections()).thenReturn(Collections.emptySet());
        when(pg2.getFlowFileConcurrency()).thenReturn(FlowFileConcurrency.UNBOUNDED);
        when(pg2.getFlowFileOutboundPolicy()).thenReturn(FlowFileOutboundPolicy.STREAM_WHEN_AVAILABLE);
        when(pg2.getExecutionEngine()).thenReturn(ExecutionEngine.STANDARD);
        when(pg2.getPosition()).thenReturn(new Position(0, 0));

        doAnswer(inv -> {
            final ProcessorNode p = inv.getArgument(0);
            // ensure nested processor nodes report PG2 as their group
            when(p.getProcessGroup()).thenReturn(pg2);
            pg2Processors.add(p);
            return null;
        }).when(pg2).addProcessor(any(ProcessorNode.class));

        when(processGroup.getProcessGroups()).thenReturn(Set.of(pg2));

        // Build v1: PG1 contains PG2 with ConvertRecord referencing external CS versioned IDs
        final VersionedProcessor v1Pg2Convert = new VersionedProcessor();
        v1Pg2Convert.setIdentifier("pg2-convert");
        v1Pg2Convert.setType(CONVERT_RECORD_TYPE);
        v1Pg2Convert.setBundle(new Bundle("org.apache.nifi", "nifi-standard-nar", "test"));
        v1Pg2Convert.setName("ConvertRecord");
        v1Pg2Convert.setBulletinLevel("WARN");
        v1Pg2Convert.setPosition(new org.apache.nifi.flow.Position(0, 0));
        v1Pg2Convert.setRunDurationMillis(0L);
        v1Pg2Convert.setSchedulingStrategy("TIMER_DRIVEN");
        v1Pg2Convert.setSchedulingPeriod("0 sec");
        v1Pg2Convert.setExecutionNode("ALL");
        v1Pg2Convert.setConcurrentlySchedulableTaskCount(1);

        final Map<String, String> v1Props = new HashMap<>();
        v1Props.put("Record Reader", "e39eadb7-505e-3117-8a88-ab963dfcf5d2");
        v1Props.put("Record Writer", "f4ee3ffe-6778-3a00-9ab9-6d474f5aed00");
        v1Pg2Convert.setProperties(v1Props);

        final VersionedPropertyDescriptor pdReader = new VersionedPropertyDescriptor();
        pdReader.setName("Record Reader");
        pdReader.setIdentifiesControllerService(true);

        final VersionedPropertyDescriptor pdWriter = new VersionedPropertyDescriptor();
        pdWriter.setName("Record Writer");
        pdWriter.setIdentifiesControllerService(true);
        v1Pg2Convert.setPropertyDescriptors(Map.of("Record Reader", pdReader, "Record Writer", pdWriter));

        final VersionedProcessGroup v1Pg2 = new VersionedProcessGroup();
        v1Pg2.setIdentifier("pg2-id");
        v1Pg2.setName("PG2");
        v1Pg2.setProcessors(Set.of(v1Pg2Convert));
        v1Pg2.setPosition(new org.apache.nifi.flow.Position(0, 0));

        final VersionedProcessGroup v1Pg1 = new VersionedProcessGroup();
        v1Pg1.setIdentifier("flow-contents-group");
        v1Pg1.setName("PG1");
        v1Pg1.setProcessGroups(Set.of(v1Pg2));
        v1Pg1.setPosition(new org.apache.nifi.flow.Position(0, 0));

        final VersionedExternalFlow extV1 = new VersionedExternalFlow();
        extV1.setFlowContents(v1Pg1);
        synchronizer.synchronize(processGroup, extV1, syncOptions);

        // After v1, nested ConvertRecord should reference external root services
        assertEquals("root-reader-id", convertRecordCurrentProps.get("Record Reader"));
        assertEquals("root-writer-id", convertRecordCurrentProps.get("Record Writer"));

        // Build v2: PG1 defines scoped CS; PG2 processor references new PG1 cs by versioned id
        final VersionedControllerService v2ReaderCs = new VersionedControllerService();
        v2ReaderCs.setIdentifier("473f65c4-4c45-32fe-8bcb-22f91ab0bc82");
        v2ReaderCs.setType("org.apache.nifi.json.JsonTreeReader");
        v2ReaderCs.setBundle(new Bundle("org.apache.nifi", "nifi-record-serialization-services-nar", "test"));
        v2ReaderCs.setName("JsonTreeReader");
        v2ReaderCs.setProperties(new HashMap<>());
        v2ReaderCs.setPropertyDescriptors(new HashMap<>());

        final VersionedControllerService v2WriterCs = new VersionedControllerService();
        v2WriterCs.setIdentifier("c708fbb8-3987-3e4a-8a75-aacb1cf52ede");
        v2WriterCs.setType("org.apache.nifi.json.JsonRecordSetWriter");
        v2WriterCs.setBundle(new Bundle("org.apache.nifi", "nifi-record-serialization-services-nar", "test"));
        v2WriterCs.setName("JsonRecordSetWriter");
        v2WriterCs.setProperties(new HashMap<>());
        v2WriterCs.setPropertyDescriptors(new HashMap<>());

        final VersionedProcessor v2Pg2Convert = new VersionedProcessor();
        v2Pg2Convert.setIdentifier("pg2-convert");
        v2Pg2Convert.setType(CONVERT_RECORD_TYPE);
        v2Pg2Convert.setBundle(new Bundle("org.apache.nifi", "nifi-standard-nar", "test"));
        v2Pg2Convert.setName("ConvertRecord");
        v2Pg2Convert.setBulletinLevel("WARN");
        v2Pg2Convert.setPosition(new org.apache.nifi.flow.Position(0, 0));
        v2Pg2Convert.setRunDurationMillis(0L);
        v2Pg2Convert.setSchedulingStrategy("TIMER_DRIVEN");
        v2Pg2Convert.setSchedulingPeriod("0 sec");
        v2Pg2Convert.setExecutionNode("ALL");
        v2Pg2Convert.setConcurrentlySchedulableTaskCount(1);

        final Map<String, String> v2Props = new HashMap<>();
        v2Props.put("Record Reader", "473f65c4-4c45-32fe-8bcb-22f91ab0bc82");
        v2Props.put("Record Writer", "c708fbb8-3987-3e4a-8a75-aacb1cf52ede");
        v2Pg2Convert.setProperties(v2Props);
        v2Pg2Convert.setPropertyDescriptors(Map.of("Record Reader", pdReader, "Record Writer", pdWriter));

        final VersionedProcessGroup v2Pg2 = new VersionedProcessGroup();
        v2Pg2.setIdentifier("pg2-id");
        v2Pg2.setName("PG2");
        v2Pg2.setProcessors(Set.of(v2Pg2Convert));
        v2Pg2.setPosition(new org.apache.nifi.flow.Position(0, 0));

        final VersionedProcessGroup v2Pg1 = new VersionedProcessGroup();
        v2Pg1.setIdentifier("flow-contents-group");
        v2Pg1.setName("PG1");
        v2Pg1.setControllerServices(Set.of(v2ReaderCs, v2WriterCs));
        v2Pg1.setProcessGroups(Set.of(v2Pg2));
        v2Pg1.setPosition(new org.apache.nifi.flow.Position(0, 0));

        final VersionedExternalFlow extV2 = new VersionedExternalFlow();
        extV2.setFlowContents(v2Pg1);
        synchronizer.synchronize(processGroup, extV2, syncOptions);

        // Expect nested processor properties to resolve to PG1 scoped service instances
        final String expectedReaderInstanceId = groupServices.stream()
                .filter(cs -> cs.getVersionedComponentId().orElse("").equals("473f65c4-4c45-32fe-8bcb-22f91ab0bc82"))
                .findFirst().orElseThrow().getIdentifier();

        final String expectedWriterInstanceId = groupServices.stream()
                .filter(cs -> cs.getVersionedComponentId().orElse("").equals("c708fbb8-3987-3e4a-8a75-aacb1cf52ede"))
                .findFirst().orElseThrow().getIdentifier();

        assertEquals(expectedReaderInstanceId, convertRecordCurrentProps.get("Record Reader"));
        assertEquals(expectedWriterInstanceId, convertRecordCurrentProps.get("Record Writer"));
    }

    @Test
    public void testUpgradeKeepsExternalControllerServiceReferences() throws Exception {
        final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Load v1 and synchronize: properties should resolve to root services
        final VersionedProcessGroup v1 = readFlowContents(mapper, "/flows/NIFI-14944_v1.json");
        final VersionedExternalFlow extV1 = new VersionedExternalFlow();
        extV1.setFlowContents(v1);
        synchronizer.synchronize(processGroup, extV1, syncOptions);

        // Sanity: after v1, ConvertRecord properties should map to root service ids
        final String v1Reader = convertRecordCurrentProps.get("Record Reader");
        final String v1Writer = convertRecordCurrentProps.get("Record Writer");
        assertEquals("root-reader-id", v1Reader, "v1 should resolve to root reader id");
        assertEquals("root-writer-id", v1Writer, "v1 should resolve to root writer id");

        // Load v2 and synchronize: BUG — properties remain mapped to root ids, not new scoped services
        final VersionedProcessGroup v2 = readFlowContents(mapper, "/flows/NIFI-14944_v2.json");
        final VersionedExternalFlow extV2 = new VersionedExternalFlow();
        extV2.setFlowContents(v2);
        synchronizer.synchronize(processGroup, extV2, syncOptions);

        final String v2Reader = convertRecordCurrentProps.get("Record Reader");
        final String v2Writer = convertRecordCurrentProps.get("Record Writer");

        // Expect properties to be updated to the newly created, in-scope controller services
        final String expectedReaderInstanceId = groupServices.stream()
                .filter(cs -> cs.getVersionedComponentId().orElse("").equals("473f65c4-4c45-32fe-8bcb-22f91ab0bc82"))
                .findFirst().orElseThrow().getIdentifier();
        final String expectedWriterInstanceId = groupServices.stream()
                .filter(cs -> cs.getVersionedComponentId().orElse("").equals("c708fbb8-3987-3e4a-8a75-aacb1cf52ede"))
                .findFirst().orElseThrow().getIdentifier();

        assertEquals(expectedReaderInstanceId, v2Reader, "Upgrade should reference scoped reader");
        assertEquals(expectedWriterInstanceId, v2Writer, "Upgrade should reference scoped writer");
    }

    private VersionedProcessGroup readFlowContents(final ObjectMapper mapper, final String resourcePath) throws Exception {
        try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
            final ObjectNode root = (ObjectNode) mapper.readTree(in);
            final ObjectNode contents = (ObjectNode) root.get("flowContents");
            contents.set("connections", mapper.createArrayNode());
            return mapper.treeToValue(contents, VersionedProcessGroup.class);
        }
    }

    private ComponentIdGenerator proposedIdOrInstance() {
        return (proposed, instance, group) -> proposed == null ? instance : proposed;
    }

    private ControllerServiceNode createControllerServiceNode(final String id, final Optional<String> versionedId) {
        final ControllerServiceNode node = mock(ControllerServiceNode.class);
        when(node.getIdentifier()).thenReturn(id);
        when(node.getProcessGroup()).thenReturn(rootGroup);
        when(node.getState()).thenReturn(ControllerServiceState.DISABLED);
        when(node.getBulletinLevel()).thenReturn(LogLevel.WARN);
        when(node.getBundleCoordinate()).thenReturn(new BundleCoordinate("group", "artifact", "version"));

        final AtomicReference<Optional<String>> vcid = new AtomicReference<>(versionedId);
        when(node.getVersionedComponentId()).thenAnswer(inv -> vcid.get());
        doAnswer(inv -> {
            vcid.set(Optional.ofNullable(inv.getArgument(0)));
            return null;
        }).when(node).setVersionedComponentId(anyString());

        return node;
    }

    private void instrumentProcessorNode(final ProcessorNode node, final String type) {
        when(node.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(node.getProcessGroup()).thenReturn(processGroup);
        when(node.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(node.getScheduledState()).thenReturn(org.apache.nifi.controller.ScheduledState.STOPPED);
        when(node.getBundleCoordinate()).thenReturn(new BundleCoordinate("group", "artifact", "version"));
        when(node.getBulletinLevel()).thenReturn(LogLevel.WARN);
        when(node.getExecutionNode()).thenReturn(ExecutionNode.ALL);
        when(node.getPosition()).thenReturn(new Position(0, 0));
        when(node.getSchedulingStrategy()).thenReturn(SchedulingStrategy.TIMER_DRIVEN);
        when(node.getSchedulingPeriod()).thenReturn("0 sec");
        when(node.getBackoffMechanism()).thenReturn(BackoffMechanism.PENALIZE_FLOWFILE);
        when(node.getProperties()).thenReturn(Collections.emptyMap());
        when(node.getAdditionalClasspathResources(anyList())).thenReturn(Collections.emptySet());
        when(node.getRelationship(any())).thenReturn(null);

        // Provide basic descriptors; mark reader/writer as CS properties
        when(node.getPropertyDescriptor(any())).thenAnswer(inv -> {
            final String name = inv.getArgument(0, String.class);
            return new PropertyDescriptor.Builder().name(name).build();
        });
    }

    private ProcessorNode createBaselineMockProcessor() {
        final ProcessorNode p = mock(ProcessorNode.class);
        when(p.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(p.getProcessGroup()).thenReturn(processGroup);
        when(p.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(p.getScheduledState()).thenReturn(org.apache.nifi.controller.ScheduledState.STOPPED);
        when(p.getBundleCoordinate()).thenReturn(new BundleCoordinate("group", "artifact", "version"));
        when(p.getBulletinLevel()).thenReturn(LogLevel.WARN);
        when(p.getExecutionNode()).thenReturn(ExecutionNode.ALL);
        when(p.getPosition()).thenReturn(new Position(0, 0));
        when(p.getSchedulingStrategy()).thenReturn(SchedulingStrategy.TIMER_DRIVEN);
        when(p.getSchedulingPeriod()).thenReturn("0 sec");
        when(p.getBackoffMechanism()).thenReturn(BackoffMechanism.PENALIZE_FLOWFILE);
        when(p.getProperties()).thenReturn(Collections.emptyMap());
        when(p.getAdditionalClasspathResources(anyList())).thenReturn(Collections.emptySet());
        when(p.getRelationship(any())).thenReturn(null);
        when(p.getPropertyDescriptor(any())).thenAnswer(inv -> new PropertyDescriptor.Builder().name(inv.getArgument(0, String.class)).build());
        return p;
    }

    private static class NoopScheduledStateChangeListener implements ScheduledStateChangeListener {
        @Override
        public void onScheduledStateChange(final ProcessorNode processor, final ScheduledState scheduledState) { }
        @Override
        public void onScheduledStateChange(final Port port, final ScheduledState scheduledState) { }
        @Override
        public void onScheduledStateChange(final ControllerServiceNode controllerService, final ScheduledState scheduledState) { }
        @Override
        public void onScheduledStateChange(final ReportingTaskNode reportingTask, final ScheduledState scheduledState) { }
    }
}
