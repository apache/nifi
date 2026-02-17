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
package org.apache.nifi.registry.flow.git;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractGitFlowRegistryClientTest {

    @Test
    void verifySuccessful() throws Exception {
        final TestGitRepositoryClient repositoryClient = new TestGitRepositoryClient(true, true, Set.of("bucket-a", ".git"));
        final AtomicReference<TestGitRepositoryClient> suppliedClient = new AtomicReference<>(repositoryClient);
        final TestGitFlowRegistryClient flowRegistryClient = new TestGitFlowRegistryClient(() -> suppliedClient.getAndSet(null), "git@example.git");
        final FlowRegistryClientConfigurationContext context = createContext("main", "[.].*");
        final ComponentLog verificationLogger = new MockComponentLog("test-component", this);

        final List<ConfigVerificationResult> results = flowRegistryClient.verify(context, verificationLogger, Map.of());

        assertEquals(4, results.size());
        assertEquals("Authenticate with Repository", results.get(0).getVerificationStepName());
        assertEquals(Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals("Verify Read Access", results.get(1).getVerificationStepName());
        assertEquals(Outcome.SUCCESSFUL, results.get(1).getOutcome());
        assertEquals("List Buckets", results.get(2).getVerificationStepName());
        assertEquals(Outcome.SUCCESSFUL, results.get(2).getOutcome());
        assertEquals("Verify Write Access", results.get(3).getVerificationStepName());
        assertEquals(Outcome.SUCCESSFUL, results.get(3).getOutcome());
        assertTrue(repositoryClient.isClosed());
    }

    @Test
    void verifyAuthenticationFailure() {
        final TestGitFlowRegistryClient flowRegistryClient = new TestGitFlowRegistryClient(() -> {
            throw new FlowRegistryException("Authentication failed");
        }, "git@example.git");
        final FlowRegistryClientConfigurationContext context = createContext("main", "[.].*");
        final ComponentLog verificationLogger = new MockComponentLog("test-component", this);

        final List<ConfigVerificationResult> results = flowRegistryClient.verify(context, verificationLogger, Map.of());

        assertEquals(1, results.size());
        assertEquals("Authenticate with Repository", results.getFirst().getVerificationStepName());
        assertEquals(Outcome.FAILED, results.getFirst().getOutcome());
    }

    @Test
    void verifyReadFailureSkipsBucketListing() throws Exception {
        final TestGitRepositoryClient repositoryClient = new TestGitRepositoryClient(false, false, Set.of());
        final TestGitFlowRegistryClient flowRegistryClient = new TestGitFlowRegistryClient(() -> repositoryClient, "git@example.git");
        final FlowRegistryClientConfigurationContext context = createContext("main", "[.].*");
        final ComponentLog verificationLogger = new MockComponentLog("test-component", this);

        final List<ConfigVerificationResult> results = flowRegistryClient.verify(context, verificationLogger, Map.of());

        assertEquals(4, results.size());
        assertEquals(Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(Outcome.FAILED, results.get(1).getOutcome());
        assertEquals(Outcome.SKIPPED, results.get(2).getOutcome());
        assertEquals(Outcome.FAILED, results.get(3).getOutcome());
        assertTrue(repositoryClient.isClosed());
    }

    @Test
    void verifyBucketListingFailureReported() throws Exception {
        final TestGitRepositoryClient repositoryClient = new TestGitRepositoryClient(true, true, Set.of());
        repositoryClient.setGetTopLevelDirectoryNamesException(new FlowRegistryException("listing error"));

        final TestGitFlowRegistryClient flowRegistryClient = new TestGitFlowRegistryClient(() -> repositoryClient, "git@example.git");
        final FlowRegistryClientConfigurationContext context = createContext("main", "[.].*");
        final ComponentLog verificationLogger = new MockComponentLog("test-component", this);

        final List<ConfigVerificationResult> results = flowRegistryClient.verify(context, verificationLogger, Map.of());

        assertEquals(4, results.size());
        assertEquals(Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(Outcome.SUCCESSFUL, results.get(1).getOutcome());
        assertEquals(Outcome.FAILED, results.get(2).getOutcome());
        assertTrue(results.get(2).getExplanation().contains("listing error"));
        assertEquals(Outcome.SUCCESSFUL, results.get(3).getOutcome());
        assertTrue(repositoryClient.isClosed());
    }

    private FlowRegistryClientConfigurationContext createContext(final String branch, final String exclusionPattern) {
        final Map<PropertyDescriptor, PropertyValue> properties = Map.of(
                AbstractGitFlowRegistryClient.REPOSITORY_BRANCH, new MockPropertyValue(branch),
                AbstractGitFlowRegistryClient.DIRECTORY_FILTER_EXCLUDE, new MockPropertyValue(exclusionPattern)
        );

        return new FlowRegistryClientConfigurationContext() {
            @Override
            public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                return Objects.requireNonNull(properties.get(descriptor), "Unknown property descriptor: " + descriptor.getName());
            }

            @Override
            public Map<String, String> getAllProperties() {
                return Map.of(
                        AbstractGitFlowRegistryClient.REPOSITORY_BRANCH.getName(), branch,
                        AbstractGitFlowRegistryClient.DIRECTORY_FILTER_EXCLUDE.getName(), exclusionPattern
                );
            }

            @Override
            public Optional<String> getNiFiUserIdentity() {
                return Optional.empty();
            }
        };
    }

    private static class TestGitFlowRegistryClient extends AbstractGitFlowRegistryClient {
        private final RepositoryClientSupplier repositoryClientSupplier;
        private final String storageLocation;

        TestGitFlowRegistryClient(final RepositoryClientSupplier repositoryClientSupplier, final String storageLocation) {
            this.repositoryClientSupplier = repositoryClientSupplier;
            this.storageLocation = storageLocation;
        }

        @Override
        protected List<PropertyDescriptor> createPropertyDescriptors() {
            return List.of();
        }

        @Override
        protected String getStorageLocation(final GitRepositoryClient repositoryClient) {
            return storageLocation;
        }

        @Override
        protected GitRepositoryClient createRepositoryClient(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException {
            return repositoryClientSupplier.get();
        }

        @Override
        public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String location) {
            return storageLocation == null || storageLocation.equals(location);
        }
    }

    private interface RepositoryClientSupplier {
        GitRepositoryClient get() throws IOException, FlowRegistryException;
    }

    private static class TestGitRepositoryClient implements GitRepositoryClient {
        private final boolean canRead;
        private final boolean canWrite;
        private final Set<String> bucketNames;
        private FlowRegistryException topLevelDirectoryNamesException;
        private IOException topLevelDirectoryNamesIOException;
        private boolean closed;

        TestGitRepositoryClient(final boolean canRead, final boolean canWrite, final Set<String> bucketNames) {
            this.canRead = canRead;
            this.canWrite = canWrite;
            this.bucketNames = bucketNames;
        }

        void setGetTopLevelDirectoryNamesException(final FlowRegistryException exception) {
            this.topLevelDirectoryNamesException = exception;
            this.topLevelDirectoryNamesIOException = null;
        }

        void setGetTopLevelDirectoryNamesException(final IOException exception) {
            this.topLevelDirectoryNamesIOException = exception;
            this.topLevelDirectoryNamesException = null;
        }

        boolean isClosed() {
            return closed;
        }

        @Override
        public boolean hasReadPermission() {
            return canRead;
        }

        @Override
        public boolean hasWritePermission() {
            return canWrite;
        }

        @Override
        public Set<String> getTopLevelDirectoryNames(final String branch) throws IOException, FlowRegistryException {
            if (topLevelDirectoryNamesIOException != null) {
                throw topLevelDirectoryNamesIOException;
            }
            if (topLevelDirectoryNamesException != null) {
                throw topLevelDirectoryNamesException;
            }
            return bucketNames;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public Set<String> getBranches() {
            throw new UnsupportedOperationException("Not required for test");
        }

        @Override
        public Set<String> getFileNames(final String directory, final String branch) {
            throw new UnsupportedOperationException("Not required for test");
        }

        @Override
        public List<GitCommit> getCommits(final String path, final String branch) {
            throw new UnsupportedOperationException("Not required for test");
        }

        @Override
        public InputStream getContentFromBranch(final String path, final String branch) {
            throw new UnsupportedOperationException("Not required for test");
        }

        @Override
        public InputStream getContentFromCommit(final String path, final String commitSha) {
            throw new UnsupportedOperationException("Not required for test");
        }

        @Override
        public Optional<String> getContentSha(final String path, final String branch) {
            throw new UnsupportedOperationException("Not required for test");
        }

        @Override
        public Optional<String> getContentShaAtCommit(final String path, final String commitSha) {
            throw new UnsupportedOperationException("Not required for test");
        }

        @Override
        public String createContent(final GitCreateContentRequest request) {
            throw new UnsupportedOperationException("Not required for test");
        }

        @Override
        public InputStream deleteContent(final String filePath, final String commitMessage, final String branch) {
            throw new UnsupportedOperationException("Not required for test");
        }
    }
}
