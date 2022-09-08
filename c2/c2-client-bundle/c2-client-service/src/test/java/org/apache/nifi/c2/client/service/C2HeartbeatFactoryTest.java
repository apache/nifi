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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.protocol.api.AgentRepositories;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.FlowQueueStatus;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class C2HeartbeatFactoryTest {

    private static final String AGENT_CLASS = "agentClass";
    private static final String FLOW_ID = "flowId";

    @Mock
    private C2ClientConfig clientConfig;

    @Mock
    private FlowIdHolder flowIdHolder;

    @Mock
    private RuntimeInfoWrapper runtimeInfoWrapper;

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

        C2Heartbeat heartbeat = c2HeartbeatFactory.create(runtimeInfoWrapper);

        assertEquals(FLOW_ID, heartbeat.getFlowId());
        assertEquals(AGENT_CLASS, heartbeat.getAgentClass());
    }

    @Test
    void testCreateGeneratesAgentAndDeviceIdIfNotPresent() {
        when(runtimeInfoWrapper.getManifest()).thenReturn(createManifest());

        C2Heartbeat heartbeat = c2HeartbeatFactory.create(runtimeInfoWrapper);

        assertNotNull(heartbeat.getAgentId());
        assertNotNull(heartbeat.getDeviceId());
    }

    @Test
    void testCreatePopulatesFromRuntimeInfoWrapperForFullHeartbeat() {
        when(clientConfig.isFullHeartbeat()).thenReturn(true);

        AgentRepositories repos = new AgentRepositories();
        RuntimeManifest manifest = createManifest();
        Map<String, FlowQueueStatus> queueStatus = new HashMap<>();

        C2Heartbeat heartbeat = c2HeartbeatFactory.create(new RuntimeInfoWrapper(repos, manifest, queueStatus));

        assertEquals(repos, heartbeat.getAgentInfo().getStatus().getRepositories());
        assertEquals(manifest, heartbeat.getAgentInfo().getAgentManifest());
        assertEquals(queueStatus, heartbeat.getFlowInfo().getQueues());
    }

    @Test
    void testCreatePopulatesFromRuntimeInfoWrapperForLightHeartbeat() {
        when(clientConfig.isFullHeartbeat()).thenReturn(false);

        AgentRepositories repos = new AgentRepositories();
        RuntimeManifest manifest = createManifest();
        Map<String, FlowQueueStatus> queueStatus = new HashMap<>();

        C2Heartbeat heartbeat = c2HeartbeatFactory.create(new RuntimeInfoWrapper(repos, manifest, queueStatus));

        assertEquals(repos, heartbeat.getAgentInfo().getStatus().getRepositories());
        assertNull(heartbeat.getAgentInfo().getAgentManifest());
        assertEquals(queueStatus, heartbeat.getFlowInfo().getQueues());
    }

    @Test
    void testCreateThrowsExceptionWhenConfDirNotSet() {
        when(clientConfig.getConfDirectory()).thenReturn(String.class.getSimpleName());

        assertThrows(IllegalStateException.class, () -> c2HeartbeatFactory.create(mock(RuntimeInfoWrapper.class)));
    }

    @Test
    void testManifestHashChangesWhenManifestBundleChanges() {
        Bundle bundle1 = new Bundle("group1", "artifact1", "version1");
        Bundle bundle2 = new Bundle("group2", "artifact2", "version2");
        RuntimeManifest manifest1 = createManifest(bundle1);
        RuntimeManifest manifest2 = createManifest(bundle2);
        RuntimeManifest manifest3 = createManifest(bundle1, bundle2);

        when(runtimeInfoWrapper.getManifest()).thenReturn(manifest1);
        C2Heartbeat heartbeat1 = c2HeartbeatFactory.create(runtimeInfoWrapper);
        String hash1 = heartbeat1.getAgentInfo().getAgentManifestHash();
        assertNotNull(hash1);

        // same manifest should result in the same hash
        assertEquals(hash1,  c2HeartbeatFactory.create(runtimeInfoWrapper).getAgentInfo().getAgentManifestHash());

        // different manifest should result in hash change
        when(runtimeInfoWrapper.getManifest()).thenReturn(manifest2);
        C2Heartbeat heartbeat2 = c2HeartbeatFactory.create(runtimeInfoWrapper);
        String hash2 = heartbeat2.getAgentInfo().getAgentManifestHash();
        assertNotEquals(hash2, hash1);

        // different manifest with multiple bundles should result in hash change compared to all previous
        when(runtimeInfoWrapper.getManifest()).thenReturn(manifest3);
        C2Heartbeat heartbeat3 = c2HeartbeatFactory.create(runtimeInfoWrapper);
        String hash3 = heartbeat3.getAgentInfo().getAgentManifestHash();
        assertNotEquals(hash3, hash1);
        assertNotEquals(hash3, hash2);
    }

    private RuntimeManifest createManifest() {
        return createManifest(new Bundle());
    }

    private RuntimeManifest createManifest(Bundle... bundles) {
        RuntimeManifest manifest = new RuntimeManifest();
        manifest.setBundles(Arrays.asList(bundles));

        return manifest;
    }
}
