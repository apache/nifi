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
package org.apache.nifi.spring;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.leader.election.StandaloneLeaderElectionManager;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaderElectionManagerFactoryBeanTest {
    @Mock
    ExtensionManager extensionManager;

    @Mock
    Bundle bundle;

    LeaderElectionManagerFactoryBean bean;

    @BeforeEach
    void setBean() {
        bean = new LeaderElectionManagerFactoryBean();
        bean.setExtensionManager(extensionManager);
    }

    @Test
    void testGetObjectStandalone() throws Exception {
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, Collections.emptyMap());

        bean.setProperties(properties);

        final LeaderElectionManager leaderElectionManager = bean.getObject();

        assertInstanceOf(StandaloneLeaderElectionManager.class, leaderElectionManager);
    }

    @Test
    void testGetObjectCluster() throws Exception {
        final Map<String, String> clusterProperties = new LinkedHashMap<>();
        clusterProperties.put(NiFiProperties.CLUSTER_IS_NODE, Boolean.TRUE.toString());
        clusterProperties.put(NiFiProperties.CLUSTER_LEADER_ELECTION_IMPLEMENTATION, MockLeaderElectionManager.class.getSimpleName());

        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, clusterProperties);

        bean.setProperties(properties);

        when(bundle.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
        when(extensionManager.getBundles(eq(MockLeaderElectionManager.class.getName()))).thenReturn(Collections.singletonList(bundle));
        final ExtensionDefinition extension = new ExtensionDefinition.Builder()
            .implementationClassName(MockLeaderElectionManager.class.getName())
            .bundle(bundle)
            .extensionType(LeaderElectionManager.class)
            .build();
        when(extensionManager.getExtensions(eq(LeaderElectionManager.class))).thenReturn(Collections.singleton(extension));

        final LeaderElectionManager leaderElectionManager = bean.getObject();

        assertInstanceOf(MockLeaderElectionManager.class, leaderElectionManager);
    }
}
