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
package org.apache.nifi.kubernetes.leader.election.command;

import org.apache.nifi.controller.leader.election.LeaderElectionRole;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachingLeaderElectionCommandProviderTest {

    private static final String CLUSTER_COORDINATOR = LeaderElectionRole.CLUSTER_COORDINATOR.getRoleName();

    private static final String PRIMARY_NODE = LeaderElectionRole.PRIMARY_NODE.getRoleName();

    private static final String LEADER = "Node-0";

    @Mock
    LeaderElectionCommandProvider mockProvider;

    @Test
    void testFindLeaderCachedWithinExpiration() {
        when(mockProvider.findLeader(CLUSTER_COORDINATOR)).thenReturn(Optional.of(LEADER));
        final CachingLeaderElectionCommandProvider provider = new CachingLeaderElectionCommandProvider(mockProvider);

        final Optional<String> first = provider.findLeader(CLUSTER_COORDINATOR);
        final Optional<String> second = provider.findLeader(CLUSTER_COORDINATOR);

        assertEquals(Optional.of(LEADER), first);
        assertEquals(Optional.of(LEADER), second);
        verify(mockProvider, times(1)).findLeader(CLUSTER_COORDINATOR);
    }

    @Test
    void testFindLeaderRefreshedAfterExpiration() {
        when(mockProvider.findLeader(CLUSTER_COORDINATOR)).thenReturn(Optional.of(LEADER));
        final Duration negativeExpiration = Duration.ofSeconds(-5);
        final CachingLeaderElectionCommandProvider provider = new CachingLeaderElectionCommandProvider(mockProvider, negativeExpiration);

        provider.findLeader(CLUSTER_COORDINATOR);
        provider.findLeader(CLUSTER_COORDINATOR);

        verify(mockProvider, times(2)).findLeader(CLUSTER_COORDINATOR);
    }

    @Test
    void testFindLeaderCachedPerName() {
        when(mockProvider.findLeader(CLUSTER_COORDINATOR)).thenReturn(Optional.of(LEADER));
        when(mockProvider.findLeader(PRIMARY_NODE)).thenReturn(Optional.empty());
        final CachingLeaderElectionCommandProvider provider = new CachingLeaderElectionCommandProvider(mockProvider);

        assertEquals(Optional.of(LEADER), provider.findLeader(CLUSTER_COORDINATOR));
        assertEquals(Optional.empty(), provider.findLeader(PRIMARY_NODE));
        assertEquals(Optional.of(LEADER), provider.findLeader(CLUSTER_COORDINATOR));

        verify(mockProvider, times(1)).findLeader(CLUSTER_COORDINATOR);
        verify(mockProvider, times(1)).findLeader(PRIMARY_NODE);
    }

    @Test
    void testGetCommandDelegates() {
        final Runnable command = () -> { };
        final Runnable onStartLeading = () -> { };
        final Runnable onStopLeading = () -> { };
        final Consumer<String> onNewLeader = leader -> { };
        when(mockProvider.getCommand(CLUSTER_COORDINATOR, LEADER, onStartLeading, onStopLeading, onNewLeader)).thenReturn(command);
        final CachingLeaderElectionCommandProvider provider = new CachingLeaderElectionCommandProvider(mockProvider);

        final Runnable returned = provider.getCommand(CLUSTER_COORDINATOR, LEADER, onStartLeading, onStopLeading, onNewLeader);

        assertSame(command, returned);
    }

    @Test
    void testCloseClearsCacheAndClosesDelegatedProvider() throws IOException {
        when(mockProvider.findLeader(CLUSTER_COORDINATOR)).thenReturn(Optional.of(LEADER));
        final CachingLeaderElectionCommandProvider provider = new CachingLeaderElectionCommandProvider(mockProvider);

        provider.findLeader(CLUSTER_COORDINATOR);
        provider.close();
        provider.findLeader(CLUSTER_COORDINATOR);

        verify(mockProvider).close();
        verify(mockProvider, times(2)).findLeader(CLUSTER_COORDINATOR);
    }
}
