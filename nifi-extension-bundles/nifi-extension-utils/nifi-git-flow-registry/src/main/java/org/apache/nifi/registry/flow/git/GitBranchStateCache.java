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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowRegistryPermissions;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowVersionInfo;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Remembers, per branch, what was read for each flow file at the head of the branch.
 * <p>
 * The framework asks for the flow and its latest version for every versioned Process Group on every
 * registry synchronization. Without this cache each of those questions costs requests to the Git provider,
 * so the cost of a synchronization grows with the number of versioned Process Groups. With it, each question
 * costs a check of the branch head, which the repository client makes cheap while the head is unchanged, plus
 * requests for the files touched since the previous head.
 * <p>
 * Each value is read at the head commit of the state that holds it, and a state is used only while the branch head
 * is still that commit, so a value is never answered for another head. A state is also used only with the repository
 * client instance it was read with, so values read before a change of configuration are never answered after it.
 */
final class GitBranchStateCache {

    private final Supplier<ComponentLog> loggerSupplier;
    private final Map<String, BranchState> branchStates = new ConcurrentHashMap<>();

    GitBranchStateCache(final Supplier<ComponentLog> loggerSupplier) {
        this.loggerSupplier = loggerSupplier;
    }

    /**
     * Gets the state of the branch at its current head. When the head moved since the state was read, the values for
     * files touched between the two heads are dropped; when those files cannot be determined, every value is dropped.
     *
     * @param repositoryClient the repository client
     * @param branch the branch
     * @return the state of the branch, or empty when the repository client cannot report the branch head
     */
    Optional<BranchState> getState(final GitRepositoryClient repositoryClient, final String branch) throws IOException, FlowRegistryException {
        final Optional<String> headSha = getBranchHeadSha(repositoryClient, branch);
        if (headSha.isEmpty()) {
            return Optional.empty();
        }

        final BranchState state = branchStates.get(branch);
        if (state != null && state.isReadWith(repositoryClient) && state.headSha.equals(headSha.get())) {
            return Optional.of(state);
        }
        return Optional.of(moveState(repositoryClient, branch, state, headSha.get()));
    }

    // A client that returns a branch head is expected to answer for it, so a runtime failure is reported as a registry failure
    private static Optional<String> getBranchHeadSha(final GitRepositoryClient repositoryClient, final String branch) throws IOException, FlowRegistryException {
        try {
            return repositoryClient.getBranchHeadSha(branch);
        } catch (final RuntimeException e) {
            throw new FlowRegistryException("Failed to get the head commit of branch [%s]".formatted(branch), e);
        }
    }

    // Each call answers for the head it saw. The heads are compared before the state is replaced, so no call waits for
    // another call's comparison: concurrent calls that saw the same new head may each compare the heads, and calls that
    // saw different heads may move the state back and forth, which costs requests but never answers a value for another head.
    private BranchState moveState(final GitRepositoryClient repositoryClient, final String branch, final BranchState previousState, final String headSha) {
        final boolean previousStateUsable = previousState != null && previousState.isReadWith(repositoryClient);
        final Optional<Set<String>> changedPaths = previousStateUsable
                ? getChangedPaths(repositoryClient, branch, previousState.headSha, headSha)
                : Optional.empty();

        return branchStates.compute(branch, (key, currentState) -> {
            if (currentState != null && currentState.isReadWith(repositoryClient) && currentState.headSha.equals(headSha)) {
                return currentState;
            }

            // Values of the previous state were read at its head, so those for files not touched since are valid at the new head
            final BranchState movedState = new BranchState(repositoryClient, headSha);
            changedPaths.ifPresent(paths -> movedState.retainUnchanged(previousState, paths));
            return movedState;
        });
    }

    // A failed comparison, such as Not Found when the previous head was removed by a force push, leaves the touched
    // files unknown, so the new state starts empty
    private Optional<Set<String>> getChangedPaths(final GitRepositoryClient repositoryClient, final String branch, final String fromCommitSha, final String toCommitSha) {
        try {
            return repositoryClient.getChangedPaths(fromCommitSha, toCommitSha);
        } catch (final FileNotFoundException e) {
            loggerSupplier.get().debug("Paths changed on branch [{}] from [{}] to [{}] not found, dropping every cached value", branch, fromCommitSha, toCommitSha, e);
            return Optional.empty();
        } catch (final IOException | FlowRegistryException | RuntimeException e) {
            loggerSupplier.get().warn("Paths changed on branch [{}] from [{}] to [{}] could not be determined, dropping every cached value", branch, fromCommitSha, toCommitSha, e);
            return Optional.empty();
        }
    }

    /**
     * Drops every state, releasing the repository client they were read with.
     */
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
        private final GitRepositoryClient repositoryClient;
        private final String headSha;
        private final Map<String, LatestVersion> latestVersions = new ConcurrentHashMap<>();
        private final Map<String, RegisteredFlow> flows = new ConcurrentHashMap<>();

        private BranchState(final GitRepositoryClient repositoryClient, final String headSha) {
            this.repositoryClient = repositoryClient;
            this.headSha = headSha;
        }

        /**
         * @return the SHA of the head commit the values of this state are read at
         */
        String getHeadSha() {
            return headSha;
        }

        private boolean isReadWith(final GitRepositoryClient client) {
            return repositoryClient == client;
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
            copy.setPermissions(copy(flow.getPermissions()));
            copy.setVersionCount(flow.getVersionCount());
            copy.setVersionInfo(copy(flow.getVersionInfo()));
            return copy;
        }

        private static FlowRegistryPermissions copy(final FlowRegistryPermissions permissions) {
            if (permissions == null) {
                return null;
            }
            final FlowRegistryPermissions copy = new FlowRegistryPermissions();
            copy.setCanRead(permissions.getCanRead());
            copy.setCanWrite(permissions.getCanWrite());
            copy.setCanDelete(permissions.getCanDelete());
            return copy;
        }

        private static RegisteredFlowVersionInfo copy(final RegisteredFlowVersionInfo versionInfo) {
            if (versionInfo == null) {
                return null;
            }
            final RegisteredFlowVersionInfo copy = new RegisteredFlowVersionInfo();
            copy.setVersion(versionInfo.getVersion());
            return copy;
        }
    }
}
