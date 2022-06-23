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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.protocol.api.AgentRepositories;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.FlowQueueStatus;
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

    @InjectMocks
    private C2HeartbeatFactory c2HeartbeatFactory;

    @TempDir
    private File tempDir;

    @BeforeEach
    public void setup() {
        given(clientConfig.getConfDirectory()).willReturn(tempDir.getAbsolutePath());
    }

    @Test
    public void shouldCreateHeartbeat() {
        // given
        given(flowIdHolder.getFlowId()).willReturn(FLOW_ID);
        given(clientConfig.getAgentClass()).willReturn(AGENT_CLASS);

        // when
        C2Heartbeat heartbeat = c2HeartbeatFactory.create(mock(RuntimeInfoWrapper.class));

        // then
        assertEquals(FLOW_ID, heartbeat.getFlowId());
        assertEquals(AGENT_CLASS, heartbeat.getAgentClass());
    }

    @Test
    public void shouldGenerateAgentAndDeviceIdIfNotPresent() {
        // given + when
        C2Heartbeat heartbeat = c2HeartbeatFactory.create(mock(RuntimeInfoWrapper.class));

        // then
        assertNotNull(heartbeat.getAgentId());
        assertNotNull(heartbeat.getDeviceId());
    }

    @Test
    public void shouldPopulateFromRuntimeInfoWrapper() {
        // given
        AgentRepositories repos = new AgentRepositories();
        RuntimeManifest manifest = new RuntimeManifest();
        Map<String, FlowQueueStatus> queueStatus = new HashMap<>();

        // when
        C2Heartbeat heartbeat = c2HeartbeatFactory.create(new RuntimeInfoWrapper(repos, manifest, queueStatus));

        // then
        assertEquals(repos, heartbeat.getAgentInfo().getStatus().getRepositories());
        assertEquals(manifest, heartbeat.getAgentInfo().getAgentManifest());
        assertEquals(queueStatus, heartbeat.getFlowInfo().getQueues());
    }

    @Test
    public void shouldFailIfConfDirNotConfiguredProperly() {
        // given
        given(clientConfig.getConfDirectory()).willReturn("dummy");

        // when + then
        assertThrows(IllegalStateException.class, () -> c2HeartbeatFactory.create(mock(RuntimeInfoWrapper.class)));
    }
}
