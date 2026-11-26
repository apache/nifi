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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachingLeaderElectionCommandProviderTest {

    private static final String CLUSTER_COORDINATOR = LeaderElectionRole.CLUSTER_COORDINATOR.getRoleName();

    private static final String PRIMARY_NODE = LeaderElectionRole.PRIMARY_NODE.getRoleName();

    private static final String LEADER = "Node-0";

    private static final int CONCURRENT_CALLERS = 50;

    private static final long TIMEOUT_SECONDS = 10;

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
    void testFindLeaderConcurrentCallsShareSingleRequest() throws Exception {
        final CountDownLatch requestStarted = new CountDownLatch(1);
        final CountDownLatch requestRelease = new CountDownLatch(1);
        when(mockProvider.findLeader(PRIMARY_NODE)).thenAnswer(invocation -> {
            requestStarted.countDown();
            assertTrue(requestRelease.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            return Optional.of(LEADER);
        });
        final CachingLeaderElectionCommandProvider provider = new CachingLeaderElectionCommandProvider(mockProvider);

        final AtomicInteger callers = new AtomicInteger();
        final ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_CALLERS);
        try {
            final List<Future<Optional<String>>> results = new ArrayList<>();
            for (int i = 0; i < CONCURRENT_CALLERS; i++) {
                results.add(executorService.submit(() -> {
                    callers.incrementAndGet();
                    return provider.findLeader(PRIMARY_NODE);
                }));
            }

            // Hold the request open until every caller has started so that callers wait on the request in flight
            assertTrue(requestStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (callers.get() < CONCURRENT_CALLERS && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            requestRelease.countDown();

            for (final Future<Optional<String>> result : results) {
                assertEquals(Optional.of(LEADER), result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
        } finally {
            requestRelease.countDown();
            executorService.shutdownNow();
        }

        verify(mockProvider, times(1)).findLeader(PRIMARY_NODE);
    }

    @Test
    void testFindLeaderFailureNotCached() {
        final IllegalStateException failure = new IllegalStateException("Request failed");
        when(mockProvider.findLeader(PRIMARY_NODE)).thenThrow(failure).thenReturn(Optional.of(LEADER));
        final CachingLeaderElectionCommandProvider provider = new CachingLeaderElectionCommandProvider(mockProvider);

        final IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> provider.findLeader(PRIMARY_NODE));
        assertSame(failure, thrown);

        assertEquals(Optional.of(LEADER), provider.findLeader(PRIMARY_NODE));
        assertEquals(Optional.of(LEADER), provider.findLeader(PRIMARY_NODE));

        verify(mockProvider, times(2)).findLeader(PRIMARY_NODE);
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
