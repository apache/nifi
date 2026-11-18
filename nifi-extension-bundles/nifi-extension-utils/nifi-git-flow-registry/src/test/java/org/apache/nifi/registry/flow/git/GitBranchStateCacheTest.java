/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.nifi.registry.flow.git;

import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowRegistryPermissions;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowVersionInfo;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GitBranchStateCacheTest {
    private static final String BRANCH = "main";
    private static final String CHANGED_PATH = "bucket/changed.json";
    private static final String UNCHANGED_PATH = "bucket/unchanged.json";
    private static final long WAIT_SECONDS = 5;

    private GitRepositoryClient repositoryClient;
    private GitBranchStateCache cache;

    @BeforeEach
    void setUp() throws Exception {
        repositoryClient = mock(GitRepositoryClient.class);
        when(repositoryClient.getBranchHeadSha(BRANCH)).thenReturn(Optional.of("head-1"));
        cache = new GitBranchStateCache(() -> new MockComponentLog("cache", this));
    }

    @Test
    void testHeadCheckedOnEveryCall() throws Exception {
        final GitBranchStateCache.BranchState firstState = cache.getState(repositoryClient, BRANCH).orElseThrow();
        firstState.putLatestVersion(UNCHANGED_PATH, Optional.of("commit-1"));
        final GitBranchStateCache.BranchState secondState = cache.getState(repositoryClient, BRANCH).orElseThrow();

        assertSame(firstState, secondState);
        assertEquals("head-1", secondState.getHeadSha());
        assertEquals(Optional.of("commit-1"), secondState.getLatestVersion(UNCHANGED_PATH).orElseThrow().toOptional());
        verify(repositoryClient, times(2)).getBranchHeadSha(BRANCH);
        verify(repositoryClient, never()).getChangedPaths(anyString(), anyString());
    }

    @Test
    void testMovedHeadDropsOnlyChangedPaths() throws Exception {
        final GitBranchStateCache.BranchState firstState = cache.getState(repositoryClient, BRANCH).orElseThrow();
        firstState.putLatestVersion(CHANGED_PATH, Optional.of("commit-1"));
        firstState.putLatestVersion(UNCHANGED_PATH, Optional.of("commit-1"));
        firstState.putFlow(CHANGED_PATH, createFlow("changed"));
        firstState.putFlow(UNCHANGED_PATH, createFlow("unchanged"));

        when(repositoryClient.getBranchHeadSha(BRANCH)).thenReturn(Optional.of("head-2"));
        when(repositoryClient.getChangedPaths("head-1", "head-2")).thenReturn(Optional.of(Set.of(CHANGED_PATH)));
        final GitBranchStateCache.BranchState state = cache.getState(repositoryClient, BRANCH).orElseThrow();

        assertEquals("head-2", state.getHeadSha());
        assertTrue(state.getLatestVersion(CHANGED_PATH).isEmpty());
        assertTrue(state.getFlow(CHANGED_PATH).isEmpty());
        assertEquals(Optional.of("commit-1"), state.getLatestVersion(UNCHANGED_PATH).orElseThrow().toOptional());
        assertEquals("unchanged", state.getFlow(UNCHANGED_PATH).orElseThrow().getName());
    }

    @Test
    void testMovedHeadWithUnknownChangesDropsEverything() throws Exception {
        cache.getState(repositoryClient, BRANCH).orElseThrow().putLatestVersion(UNCHANGED_PATH, Optional.of("commit-1"));

        when(repositoryClient.getBranchHeadSha(BRANCH)).thenReturn(Optional.of("head-2"));
        when(repositoryClient.getChangedPaths("head-1", "head-2")).thenReturn(Optional.empty());

        assertTrue(cache.getState(repositoryClient, BRANCH).orElseThrow().getLatestVersion(UNCHANGED_PATH).isEmpty());
    }

    @Test
    void testFailedComparisonDropsEverythingAndMovesToNewHead() throws Exception {
        cache.getState(repositoryClient, BRANCH).orElseThrow().putLatestVersion(UNCHANGED_PATH, Optional.of("commit-1"));

        when(repositoryClient.getBranchHeadSha(BRANCH)).thenReturn(Optional.of("head-2"));
        when(repositoryClient.getChangedPaths("head-1", "head-2")).thenThrow(new FileNotFoundException("Not Found"));

        final GitBranchStateCache.BranchState state = cache.getState(repositoryClient, BRANCH).orElseThrow();
        assertEquals("head-2", state.getHeadSha());
        assertTrue(state.getLatestVersion(UNCHANGED_PATH).isEmpty());

        assertSame(state, cache.getState(repositoryClient, BRANCH).orElseThrow());
        verify(repositoryClient, times(1)).getChangedPaths(anyString(), anyString());
    }

    @Test
    void testRuntimeFailureOfComparisonDropsEverythingAndMovesToNewHead() throws Exception {
        cache.getState(repositoryClient, BRANCH).orElseThrow().putLatestVersion(UNCHANGED_PATH, Optional.of("commit-1"));

        when(repositoryClient.getBranchHeadSha(BRANCH)).thenReturn(Optional.of("head-2"));
        when(repositoryClient.getChangedPaths("head-1", "head-2")).thenThrow(new IllegalStateException("Comparison failed"));

        final GitBranchStateCache.BranchState state = cache.getState(repositoryClient, BRANCH).orElseThrow();
        assertEquals("head-2", state.getHeadSha());
        assertTrue(state.getLatestVersion(UNCHANGED_PATH).isEmpty());

        assertSame(state, cache.getState(repositoryClient, BRANCH).orElseThrow());
        verify(repositoryClient, times(1)).getChangedPaths(anyString(), anyString());
    }

    @Test
    void testRuntimeFailureOfHeadCheckIsRegistryFailure() throws Exception {
        when(repositoryClient.getBranchHeadSha(BRANCH)).thenThrow(new UnsupportedOperationException("Not supported"));

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class, () -> cache.getState(repositoryClient, BRANCH));
        assertInstanceOf(UnsupportedOperationException.class, exception.getCause());
    }

    @Test
    void testComparisonDoesNotHoldOtherCallsForTheBranch() throws Exception {
        cache.getState(repositoryClient, BRANCH).orElseThrow().putLatestVersion(UNCHANGED_PATH, Optional.of("commit-1"));

        when(repositoryClient.getBranchHeadSha(BRANCH)).thenReturn(Optional.of("head-2"));
        final CountDownLatch comparisonStarted = new CountDownLatch(1);
        final CountDownLatch comparisonReleased = new CountDownLatch(1);
        final AtomicInteger comparisons = new AtomicInteger();
        when(repositoryClient.getChangedPaths("head-1", "head-2")).thenAnswer(invocation -> {
            if (comparisons.incrementAndGet() == 1) {
                comparisonStarted.countDown();
                comparisonReleased.await(WAIT_SECONDS, TimeUnit.SECONDS);
            }
            return Optional.of(Set.of());
        });

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<GitBranchStateCache.BranchState> slowCall = executor.submit(() -> cache.getState(repositoryClient, BRANCH).orElseThrow());
            assertTrue(comparisonStarted.await(WAIT_SECONDS, TimeUnit.SECONDS));

            // Answered while the first comparison is still running
            final Future<GitBranchStateCache.BranchState> otherCall = executor.submit(() -> cache.getState(repositoryClient, BRANCH).orElseThrow());
            final GitBranchStateCache.BranchState state = otherCall.get(WAIT_SECONDS, TimeUnit.SECONDS);
            assertEquals("head-2", state.getHeadSha());
            assertEquals(Optional.of("commit-1"), state.getLatestVersion(UNCHANGED_PATH).orElseThrow().toOptional());

            comparisonReleased.countDown();
            assertSame(state, slowCall.get(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            comparisonReleased.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testEachCallAnswersForTheHeadItSaw() throws Exception {
        cache.getState(repositoryClient, BRANCH).orElseThrow().putLatestVersion(UNCHANGED_PATH, Optional.of("commit-1"));

        // While this call waits for its head check, another call sees the head moved to head-2 and moves the state there;
        // this call's check is answered last, still with the earlier head-1
        final AtomicInteger headChecks = new AtomicInteger();
        final AtomicReference<GitBranchStateCache.BranchState> laterState = new AtomicReference<>();
        when(repositoryClient.getBranchHeadSha(BRANCH)).thenAnswer(invocation -> {
            if (headChecks.incrementAndGet() > 1) {
                return Optional.of("head-2");
            }
            final GitBranchStateCache.BranchState state = cache.getState(repositoryClient, BRANCH).orElseThrow();
            state.putLatestVersion(UNCHANGED_PATH, Optional.of("commit-2"));
            laterState.set(state);
            return Optional.of("head-1");
        });
        when(repositoryClient.getChangedPaths("head-1", "head-2")).thenReturn(Optional.of(Set.of()));

        final GitBranchStateCache.BranchState state = cache.getState(repositoryClient, BRANCH).orElseThrow();

        assertEquals("head-2", laterState.get().getHeadSha());
        assertEquals("head-1", state.getHeadSha());
        assertTrue(state.getLatestVersion(UNCHANGED_PATH).isEmpty());
    }

    @Test
    void testStateIsNotUsedWithOtherRepositoryClient() throws Exception {
        cache.getState(repositoryClient, BRANCH).orElseThrow().putLatestVersion(UNCHANGED_PATH, Optional.of("commit-1"));

        final GitRepositoryClient reconfiguredClient = mock(GitRepositoryClient.class);
        when(reconfiguredClient.getBranchHeadSha(BRANCH)).thenReturn(Optional.of("head-1"));
        final GitBranchStateCache.BranchState state = cache.getState(reconfiguredClient, BRANCH).orElseThrow();

        assertTrue(state.getLatestVersion(UNCHANGED_PATH).isEmpty());
        verify(reconfiguredClient, never()).getChangedPaths(anyString(), anyString());

        // A call still running with the previous client does not get the new client's state either
        state.putLatestVersion(UNCHANGED_PATH, Optional.of("commit-2"));
        assertTrue(cache.getState(repositoryClient, BRANCH).orElseThrow().getLatestVersion(UNCHANGED_PATH).isEmpty());
    }

    @Test
    void testUnsupportedBranchHeadDisablesCache() throws Exception {
        when(repositoryClient.getBranchHeadSha(BRANCH)).thenReturn(Optional.empty());

        assertTrue(cache.getState(repositoryClient, BRANCH).isEmpty());
    }

    @Test
    void testFlowsAreCopied() throws Exception {
        final GitBranchStateCache.BranchState state = cache.getState(repositoryClient, BRANCH).orElseThrow();
        final RegisteredFlow flow = createFlow("original");
        state.putFlow(CHANGED_PATH, flow);
        flow.setName("changed after put");
        state.getFlow(CHANGED_PATH).orElseThrow().setName("changed after get");

        assertEquals("original", state.getFlow(CHANGED_PATH).orElseThrow().getName());
    }

    @Test
    void testFlowPermissionsAndVersionInfoAreCopied() throws Exception {
        final GitBranchStateCache.BranchState state = cache.getState(repositoryClient, BRANCH).orElseThrow();
        final RegisteredFlow flow = createFlow("original");
        final FlowRegistryPermissions permissions = new FlowRegistryPermissions();
        permissions.setCanRead(true);
        flow.setPermissions(permissions);
        final RegisteredFlowVersionInfo versionInfo = new RegisteredFlowVersionInfo();
        versionInfo.setVersion(1);
        flow.setVersionInfo(versionInfo);
        state.putFlow(CHANGED_PATH, flow);

        permissions.setCanRead(false);
        versionInfo.setVersion(2);
        final RegisteredFlow returned = state.getFlow(CHANGED_PATH).orElseThrow();
        returned.getPermissions().setCanWrite(true);
        returned.getVersionInfo().setVersion(3);

        final RegisteredFlow cached = state.getFlow(CHANGED_PATH).orElseThrow();
        assertTrue(cached.getPermissions().getCanRead());
        assertFalse(cached.getPermissions().getCanWrite());
        assertEquals(1, cached.getVersionInfo().getVersion());
    }

    private static RegisteredFlow createFlow(final String name) {
        final RegisteredFlow flow = new RegisteredFlow();
        flow.setIdentifier(name);
        flow.setName(name);
        return flow;
    }
}
