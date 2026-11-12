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
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Remembers, per branch, what was read for each flow file while the head of the branch stays the same.
 * <p>
 * The framework asks for the flow and its latest version for every versioned Process Group on every
 * registry synchronization. Without this cache each of those questions costs requests to the Git provider,
 * so the cost of a synchronization grows with the number of versioned Process Groups. With it, a
 * synchronization costs one request for the branch head per check interval, plus requests for the files
 * that changed since the previous head.
 */
final class GitBranchStateCache {

    static final long HEAD_CHECK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final LongSupplier nanoTimeSupplier;
    private final Map<String, BranchState> branchStates = new ConcurrentHashMap<>();

    GitBranchStateCache() {
        this(System::nanoTime);
    }

    GitBranchStateCache(final LongSupplier nanoTimeSupplier) {
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    /**
     * Gets the state of the branch, checking its head when the last check is older than the check interval.
     * When the head moved, values cached for files changed between the two heads are dropped; when those files
     * cannot be determined, every value cached for the branch is dropped.
     *
     * @param repositoryClient the repository client
     * @param branch the branch
     * @return the state of the branch, or empty when the repository client cannot report the branch head
     */
    Optional<BranchState> getState(final GitRepositoryClient repositoryClient, final String branch) throws IOException, FlowRegistryException {
        final long now = nanoTimeSupplier.getAsLong();
        final BranchState state = branchStates.get(branch);
        if (state != null && now - state.checkedAt < HEAD_CHECK_INTERVAL_NANOS) {
            return Optional.of(state);
        }

        final Optional<String> headSha = repositoryClient.getBranchHeadSha(branch);
        if (headSha.isEmpty()) {
            branchStates.remove(branch);
            return Optional.empty();
        }

        if (state != null && state.headSha.equals(headSha.get())) {
            state.checkedAt = now;
            return Optional.of(state);
        }

        final BranchState currentState = new BranchState(headSha.get(), now);
        if (state != null) {
            final Optional<Set<String>> changedPaths = repositoryClient.getChangedPaths(state.headSha, currentState.headSha);
            changedPaths.ifPresent(paths -> currentState.retainUnchanged(state, paths));
        }
        // Values put into the replaced state by requests still running are dropped with it
        branchStates.put(branch, currentState);
        return Optional.of(currentState);
    }

    /**
     * Drops the values cached for the given file, after this client changed it.
     *
     * @param branch the branch
     * @param path the path of the file
     */
    void invalidate(final String branch, final String path) {
        final BranchState state = branchStates.get(branch);
        if (state != null) {
            state.remove(path);
        }
    }

    void clear() {
        branchStates.clear();
    }

    /**
     * The latest commit of a file as read from the repository; the SHA is null when no commit changed the file.
     */
    record LatestVersion(String commitSha) {
        Optional<String> toOptional() {
            return Optional.ofNullable(commitSha);
        }
    }

    static final class BranchState {
        private final String headSha;
        private volatile long checkedAt;
        private final Map<String, LatestVersion> latestVersions = new ConcurrentHashMap<>();
        private final Map<String, RegisteredFlow> flows = new ConcurrentHashMap<>();

        private BranchState(final String headSha, final long checkedAt) {
            this.headSha = headSha;
            this.checkedAt = checkedAt;
        }

        /**
         * @return the latest version read for the file, empty when it was not read since the head of the branch changed
         */
        Optional<LatestVersion> getLatestVersion(final String path) {
            return Optional.ofNullable(latestVersions.get(path));
        }

        void putLatestVersion(final String path, final Optional<String> commitSha) {
            latestVersions.put(path, new LatestVersion(commitSha.orElse(null)));
        }

        Optional<RegisteredFlow> getFlow(final String path) {
            return Optional.ofNullable(flows.get(path)).map(BranchState::copy);
        }

        void putFlow(final String path, final RegisteredFlow flow) {
            flows.put(path, copy(flow));
        }

        private void remove(final String path) {
            latestVersions.remove(path);
            flows.remove(path);
        }

        private void retainUnchanged(final BranchState previousState, final Set<String> changedPaths) {
            previousState.latestVersions.forEach((path, latestVersion) -> {
                if (!changedPaths.contains(path)) {
                    latestVersions.put(path, latestVersion);
                }
            });
            previousState.flows.forEach((path, flow) -> {
                if (!changedPaths.contains(path)) {
                    flows.put(path, flow);
                }
            });
        }

        // Callers change the returned flow, so the cache keeps and hands out copies
        private static RegisteredFlow copy(final RegisteredFlow flow) {
            final RegisteredFlow copy = new RegisteredFlow();
            copy.setIdentifier(flow.getIdentifier());
            copy.setName(flow.getName());
            copy.setDescription(flow.getDescription());
            copy.setBranch(flow.getBranch());
            copy.setBucketIdentifier(flow.getBucketIdentifier());
            copy.setBucketName(flow.getBucketName());
            copy.setCreatedTimestamp(flow.getCreatedTimestamp());
            copy.setLastModifiedTimestamp(flow.getLastModifiedTimestamp());
            copy.setPermissions(flow.getPermissions());
            copy.setVersionCount(flow.getVersionCount());
            copy.setVersionInfo(flow.getVersionInfo());
            return copy;
        }
    }
}
