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

import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
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

    private final AtomicLong nanoTime = new AtomicLong();
    private GitRepositoryClient repositoryClient;
    private GitBranchStateCache cache;

    @BeforeEach
    void setUp() throws Exception {
        repositoryClient = mock(GitRepositoryClient.class);
        when(repositoryClient.getBranchHeadSha(BRANCH)).thenReturn(Optional.of("head-1"));
        cache = new GitBranchStateCache(nanoTime::get);
    }

    @Test
    void testHeadCheckedOncePerInterval() throws Exception {
        final GitBranchStateCache.BranchState firstState = cache.getState(repositoryClient, BRANCH).orElseThrow();
        nanoTime.addAndGet(GitBranchStateCache.HEAD_CHECK_INTERVAL_NANOS - 1);
        final GitBranchStateCache.BranchState secondState = cache.getState(repositoryClient, BRANCH).orElseThrow();

        assertSame(firstState, secondState);
        verify(repositoryClient, times(1)).getBranchHeadSha(BRANCH);
    }

    @Test
    void testUnchangedHeadKeepsValues() throws Exception {
        cache.getState(repositoryClient, BRANCH).orElseThrow().putLatestVersion(UNCHANGED_PATH, Optional.of("commit-1"));

        nanoTime.addAndGet(GitBranchStateCache.HEAD_CHECK_INTERVAL_NANOS);
        final GitBranchStateCache.BranchState state = cache.getState(repositoryClient, BRANCH).orElseThrow();

        assertEquals(Optional.of("commit-1"), state.getLatestVersion(UNCHANGED_PATH).orElseThrow().toOptional());
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
        nanoTime.addAndGet(GitBranchStateCache.HEAD_CHECK_INTERVAL_NANOS);
        final GitBranchStateCache.BranchState state = cache.getState(repositoryClient, BRANCH).orElseThrow();

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
        nanoTime.addAndGet(GitBranchStateCache.HEAD_CHECK_INTERVAL_NANOS);

        assertTrue(cache.getState(repositoryClient, BRANCH).orElseThrow().getLatestVersion(UNCHANGED_PATH).isEmpty());
    }

    @Test
    void testUnsupportedBranchHeadDisablesCache() throws Exception {
        when(repositoryClient.getBranchHeadSha(BRANCH)).thenReturn(Optional.empty());

        assertTrue(cache.getState(repositoryClient, BRANCH).isEmpty());
    }

    @Test
    void testInvalidateDropsPath() throws Exception {
        final GitBranchStateCache.BranchState state = cache.getState(repositoryClient, BRANCH).orElseThrow();
        state.putLatestVersion(CHANGED_PATH, Optional.empty());

        assertTrue(state.getLatestVersion(CHANGED_PATH).orElseThrow().toOptional().isEmpty());
        cache.invalidate(BRANCH, CHANGED_PATH);
        assertTrue(state.getLatestVersion(CHANGED_PATH).isEmpty());
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

    private static RegisteredFlow createFlow(final String name) {
        final RegisteredFlow flow = new RegisteredFlow();
        flow.setIdentifier(name);
        flow.setName(name);
        return flow;
    }
}
