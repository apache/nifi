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
package org.apache.nifi.c2.client.service;

import static org.apache.nifi.c2.protocol.api.RunStatus.RUNNING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.protocol.api.AgentManifest;
import org.apache.nifi.c2.protocol.api.AgentRepositories;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.FlowQueueStatus;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.apache.nifi.c2.protocol.api.ProcessorBulletin;
import org.apache.nifi.c2.protocol.api.ProcessorStatus;
import org.apache.nifi.c2.protocol.api.SupportedOperation;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@MockitoSettings(strictness = Strictness.LENIENT)
public class C2HeartbeatFactoryTest {

    private static final String AGENT_CLASS = "agentClass";
    private static final String FLOW_ID = "flowId";
    private static final String MANIFEST_HASH = "hash";
    private static final String RESOURCE_HASH = "resourceHash";

    @Mock
    private C2ClientConfig clientConfig;

    @Mock
    private FlowIdHolder flowIdHolder;

    @Mock
    private RuntimeInfoWrapper runtimeInfoWrapper;

    @Mock
    private ManifestHashProvider manifestHashProvider;

    @Mock
    private Supplier<ResourcesGlobalHash> resourcesGlobalHashSupplier;

    @InjectMocks
    private C2HeartbeatFactory c2HeartbeatFactory;

    @TempDir
    private File tempDir;

    @BeforeEach
    public void setup() {
        when(clientConfig.getConfDirectory()).thenReturn(tempDir.getAbsolutePath());
    }

    @Test
    void testCreateHeartbeat() {
        when(flowIdHolder.getFlowId()).thenReturn(FLOW_ID);
        when(clientConfig.getAgentClass()).thenReturn(AGENT_CLASS);
        when(runtimeInfoWrapper.getManifest()).thenReturn(createManifest());
        when(resourcesGlobalHashSupplier.get()).thenReturn(createResourcesGlobalHash());

        C2Heartbeat heartbeat = c2HeartbeatFactory.create(runtimeInfoWrapper);

        assertEquals(FLOW_ID, heartbeat.getFlowId());
        assertEquals(AGENT_CLASS, heartbeat.getAgentClass());
        assertEquals(RESOURCE_HASH, heartbeat.getResourceInfo().getHash());
    }

    @Test
    void testCreateGeneratesAgentAndDeviceIdIfNotPresent() {
        when(runtimeInfoWrapper.getManifest()).thenReturn(createManifest());
        when(resourcesGlobalHashSupplier.get()).thenReturn(createResourcesGlobalHash());

        C2Heartbeat heartbeat = c2HeartbeatFactory.create(runtimeInfoWrapper);

        assertNotNull(heartbeat.getAgentId());
        assertNotNull(heartbeat.getDeviceId());
        assertEquals(RESOURCE_HASH, heartbeat.getResourceInfo().getHash());
    }

    @Test
    void testCreatePopulatesFromRuntimeInfoWrapperForFullHeartbeat() {
        when(clientConfig.isFullHeartbeat()).thenReturn(true);
        when(resourcesGlobalHashSupplier.get()).thenReturn(createResourcesGlobalHash());

        AgentRepositories repos = new AgentRepositories();
        RuntimeManifest manifest = createManifest();
        Map<String, FlowQueueStatus> queueStatus = new HashMap<>();
        List<ProcessorBulletin> processorBulletins = new ArrayList<>();
        List<ProcessorStatus> processorStatus = new ArrayList<>();

        C2Heartbeat heartbeat = c2HeartbeatFactory.create(new RuntimeInfoWrapper(repos, manifest, queueStatus, processorBulletins, processorStatus, RUNNING));

        assertEquals(repos, heartbeat.getAgentInfo().getStatus().getRepositories());
        assertEquals(manifest, heartbeat.getAgentInfo().getAgentManifest());
        assertEquals(queueStatus, heartbeat.getFlowInfo().getQueues());
        assertEquals(processorBulletins, heartbeat.getFlowInfo().getProcessorBulletins());
        assertEquals(processorStatus, heartbeat.getFlowInfo().getProcessorStatuses());
        assertEquals(RUNNING, heartbeat.getFlowInfo().getRunStatus());
        assertEquals(RESOURCE_HASH, heartbeat.getResourceInfo().getHash());
    }

    @Test
    void testCreatePopulatesFromRuntimeInfoWrapperForLightHeartbeat() {
        when(clientConfig.isFullHeartbeat()).thenReturn(false);
        when(resourcesGlobalHashSupplier.get()).thenReturn(createResourcesGlobalHash());

        AgentRepositories repos = new AgentRepositories();
        RuntimeManifest manifest = createManifest();
        Map<String, FlowQueueStatus> queueStatus = new HashMap<>();
        List<ProcessorBulletin> processorBulletins = new ArrayList<>();
        List<ProcessorStatus> processorStatus = new ArrayList<>();

        C2Heartbeat heartbeat = c2HeartbeatFactory.create(new RuntimeInfoWrapper(repos, manifest, queueStatus, processorBulletins, processorStatus, RUNNING));

        assertEquals(repos, heartbeat.getAgentInfo().getStatus().getRepositories());
        assertNull(heartbeat.getAgentInfo().getAgentManifest());
        assertEquals(queueStatus, heartbeat.getFlowInfo().getQueues());
        assertEquals(processorBulletins, heartbeat.getFlowInfo().getProcessorBulletins());
        assertEquals(processorStatus, heartbeat.getFlowInfo().getProcessorStatuses());
        assertEquals(RUNNING, heartbeat.getFlowInfo().getRunStatus());
        assertEquals(RESOURCE_HASH, heartbeat.getResourceInfo().getHash());
    }

    @Test
    void testCreateThrowsExceptionWhenConfDirNotSet() {
        when(clientConfig.getConfDirectory()).thenReturn(String.class.getSimpleName());

        assertThrows(IllegalStateException.class, () -> c2HeartbeatFactory.create(mock(RuntimeInfoWrapper.class)));
    }

    @Test
    void testAgentManifestHashIsPopulatedInCaseOfRuntimeManifest() {
        RuntimeManifest manifest = createManifest();
        when(manifestHashProvider.calculateManifestHash(manifest.getBundles(), Collections.emptySet())).thenReturn(MANIFEST_HASH);
        when(resourcesGlobalHashSupplier.get()).thenReturn(createResourcesGlobalHash());

        C2Heartbeat heartbeat = c2HeartbeatFactory.create(new RuntimeInfoWrapper(new AgentRepositories(), manifest, new HashMap<>(), new ArrayList<>(), new ArrayList<>(), RUNNING));

        assertEquals(MANIFEST_HASH, heartbeat.getAgentInfo().getAgentManifestHash());
        assertEquals(RESOURCE_HASH, heartbeat.getResourceInfo().getHash());
        assertEquals(RUNNING, heartbeat.getFlowInfo().getRunStatus());
    }

    @Test
    void testAgentManifestHashIsPopulatedInCaseOfAgentManifest() {
        AgentManifest manifest = new AgentManifest(createManifest());
        SupportedOperation supportedOperation = new SupportedOperation();
        supportedOperation.setType(OperationType.HEARTBEAT);
        Set<SupportedOperation> supportedOperations = Collections.singleton(supportedOperation);
        manifest.setSupportedOperations(supportedOperations);
        when(manifestHashProvider.calculateManifestHash(manifest.getBundles(), supportedOperations)).thenReturn(MANIFEST_HASH);
        when(resourcesGlobalHashSupplier.get()).thenReturn(createResourcesGlobalHash());

        C2Heartbeat heartbeat = c2HeartbeatFactory.create(new RuntimeInfoWrapper(new AgentRepositories(), manifest, new HashMap<>(), new ArrayList<>(), new ArrayList<>(), RUNNING));

        assertEquals(MANIFEST_HASH, heartbeat.getAgentInfo().getAgentManifestHash());
        assertEquals(RESOURCE_HASH, heartbeat.getResourceInfo().getHash());
        assertEquals(RUNNING, heartbeat.getFlowInfo().getRunStatus());
    }

    private RuntimeManifest createManifest() {
        return createManifest(new Bundle());
    }

    private RuntimeManifest createManifest(Bundle... bundles) {
        RuntimeManifest manifest = new RuntimeManifest();
        manifest.setBundles(Arrays.asList(bundles));
        return manifest;
    }

    private ResourcesGlobalHash createResourcesGlobalHash() {
        ResourcesGlobalHash resourcesGlobalHash = new ResourcesGlobalHash();
        resourcesGlobalHash.setDigest(RESOURCE_HASH);
        return resourcesGlobalHash;
    }
}
