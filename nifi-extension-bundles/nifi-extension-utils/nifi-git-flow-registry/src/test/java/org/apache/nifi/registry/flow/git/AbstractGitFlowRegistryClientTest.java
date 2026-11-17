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
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.flow.FlowLocation;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryClientInitializationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowVersionLocation;
import org.apache.nifi.registry.flow.RegisterAction;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractGitFlowRegistryClientTest {
    private static final String FLOW_PATH = "bucket-a/flow-1.json";
    private static final FlowLocation FLOW_LOCATION = new FlowLocation("main", "bucket-a", "flow-1");

    @Test
    void verifySuccessful() {
        final TestGitRepositoryClient repositoryClient = new TestGitRepositoryClient(true, true, Set.of("bucket-a", ".git"));
        final AtomicReference<TestGitRepositoryClient> suppliedClient = new AtomicReference<>(repositoryClient);
        final TestGitFlowRegistryClient flowRegistryClient = new TestGitFlowRegistryClient(() -> suppliedClient.getAndSet(null), "git@example.git");
        flowRegistryClient.initialize(createInitializationContext());
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
        flowRegistryClient.initialize(createInitializationContext());
        final FlowRegistryClientConfigurationContext context = createContext("main", "[.].*");
        final ComponentLog verificationLogger = new MockComponentLog("test-component", this);

        final List<ConfigVerificationResult> results = flowRegistryClient.verify(context, verificationLogger, Map.of());

        assertEquals(1, results.size());
        assertEquals("Authenticate with Repository", results.getFirst().getVerificationStepName());
        assertEquals(Outcome.FAILED, results.getFirst().getOutcome());
    }

    @Test
    void verifyReadFailureSkipsBucketListing() {
        final TestGitRepositoryClient repositoryClient = new TestGitRepositoryClient(false, false, Set.of());
        final TestGitFlowRegistryClient flowRegistryClient = new TestGitFlowRegistryClient(() -> repositoryClient, "git@example.git");
        flowRegistryClient.initialize(createInitializationContext());
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
    void verifyBucketListingFailureReported() {
        final TestGitRepositoryClient repositoryClient = new TestGitRepositoryClient(true, true, Set.of("bucket-a"));
        repositoryClient.setGetTopLevelDirectoryNamesException(new FlowRegistryException("listing error"));

        final TestGitFlowRegistryClient flowRegistryClient = new TestGitFlowRegistryClient(() -> repositoryClient, "git@example.git");
        flowRegistryClient.initialize(createInitializationContext());
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

    @Test
    void createBranchDelegatesToRepositoryClient() throws Exception {
        final TestGitRepositoryClient repositoryClient = new TestGitRepositoryClient(true, true, Set.of("bucket-a"));
        final TestGitFlowRegistryClient flowRegistryClient = new TestGitFlowRegistryClient(() -> repositoryClient, "git@example.git");
        flowRegistryClient.initialize(createInitializationContext());
        final FlowRegistryClientConfigurationContext context = createContext("main", "[.].*");

        final FlowVersionLocation sourceLocation = new FlowVersionLocation("source-branch", "bucket-a", "flow-x", "commit-1");

        flowRegistryClient.createBranch(context, sourceLocation, " new-branch ");

        assertEquals("new-branch", repositoryClient.getCreatedBranch());
        assertEquals("source-branch", repositoryClient.getCreatedBranchSource());
        assertEquals(Optional.of("commit-1"), repositoryClient.getCreatedBranchCommit());
    }

    @Test
    void createBranchUnsupportedThrowsFlowRegistryException() {
        final TestGitRepositoryClient repositoryClient = new TestGitRepositoryClient(true, true, Set.of("bucket-a"));
        repositoryClient.setBranchCreationUnsupported(true);

        final TestGitFlowRegistryClient flowRegistryClient = new TestGitFlowRegistryClient(() -> repositoryClient, "git@example.git");
        flowRegistryClient.initialize(createInitializationContext());
        final FlowRegistryClientConfigurationContext context = createContext("main", "[.].*");

        final FlowVersionLocation sourceLocation = new FlowVersionLocation();
        sourceLocation.setBranch("source");

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class,
                () -> flowRegistryClient.createBranch(context, sourceLocation, "new-branch"));

        assertEquals("Configured repository client does not support branch creation", exception.getMessage());
        assertTrue(repositoryClient.getCreatedBranchCommit().isEmpty());
    }

    @Test
    void repeatedQuestionsForUnchangedBranchAreAnsweredFromCache() throws Exception {
        final GitRepositoryClient repositoryClient = mockRepositoryClient(new AtomicReference<>("head-1"));
        when(repositoryClient.getLatestCommitShaAtCommit(FLOW_PATH, "head-1")).thenReturn(Optional.of("commit-1"));
        when(repositoryClient.getContentFromCommit(FLOW_PATH, "head-1")).thenAnswer(invocation -> getFlowContent("Flow 1"));

        final TestGitFlowRegistryClient flowRegistryClient = createFlowRegistryClient(repositoryClient);
        final FlowRegistryClientConfigurationContext context = createContext("main", "[.].*");

        for (int i = 0; i < 3; i++) {
            assertEquals(Optional.of("commit-1"), flowRegistryClient.getLatestVersion(context, FLOW_LOCATION));
            final RegisteredFlow flow = flowRegistryClient.getFlow(context, FLOW_LOCATION);
            assertEquals("Flow 1", flow.getName());
            assertEquals("bucket-a", flow.getBucketIdentifier());
            assertEquals("main", flow.getBranch());
        }

        verify(repositoryClient, times(6)).getBranchHeadSha("main");
        verify(repositoryClient, times(1)).getLatestCommitShaAtCommit(FLOW_PATH, "head-1");
        verify(repositoryClient, times(1)).getContentFromCommit(FLOW_PATH, "head-1");
        verify(repositoryClient, never()).getLatestCommitSha(anyString(), anyString());
        verify(repositoryClient, never()).getContentFromBranch(anyString(), anyString());
    }

    @Test
    void commitIsVisibleOnNextRead() throws Exception {
        final AtomicReference<String> head = new AtomicReference<>("head-1");
        final GitRepositoryClient repositoryClient = mockRepositoryClient(head);
        when(repositoryClient.getLatestCommitShaAtCommit(FLOW_PATH, "head-1")).thenReturn(Optional.of("head-1"));
        when(repositoryClient.getContentFromCommit(FLOW_PATH, "head-1")).thenAnswer(invocation -> getFlowContent("Flow 1"));

        final TestGitFlowRegistryClient flowRegistryClient = createFlowRegistryClient(repositoryClient);
        final FlowRegistryClientConfigurationContext context = createContext("main", "[.].*");
        assertEquals(Optional.of("head-1"), flowRegistryClient.getLatestVersion(context, FLOW_LOCATION));
        assertEquals("Flow 1", flowRegistryClient.getFlow(context, FLOW_LOCATION).getName());

        when(repositoryClient.hasWritePermission()).thenReturn(true);
        when(repositoryClient.getLatestCommitSha(FLOW_PATH, "main")).thenReturn(Optional.of("head-1"));
        when(repositoryClient.getContentShaAtCommit(FLOW_PATH, "head-1")).thenReturn(Optional.of("blob-1"));
        when(repositoryClient.getContentFromBranch(FLOW_PATH, "main")).thenAnswer(invocation -> getFlowContent("Flow 1"));
        when(repositoryClient.createContent(any())).thenAnswer(invocation -> {
            head.set("head-2");
            return "head-2";
        });
        flowRegistryClient.registerFlowSnapshot(context, createFlowSnapshot("head-1"), RegisterAction.COMMIT);

        when(repositoryClient.getChangedPaths("head-1", "head-2")).thenReturn(Optional.of(Set.of(FLOW_PATH)));
        when(repositoryClient.getLatestCommitShaAtCommit(FLOW_PATH, "head-2")).thenReturn(Optional.of("head-2"));
        when(repositoryClient.getContentFromCommit(FLOW_PATH, "head-2")).thenAnswer(invocation -> getFlowContent("Flow 2"));

        assertEquals(Optional.of("head-2"), flowRegistryClient.getLatestVersion(context, FLOW_LOCATION));
        assertEquals("Flow 2", flowRegistryClient.getFlow(context, FLOW_LOCATION).getName());
        verify(repositoryClient, times(1)).getContentFromCommit(FLOW_PATH, "head-1");
        verify(repositoryClient, times(1)).getContentFromCommit(FLOW_PATH, "head-2");
    }

    @Test
    void runtimeFailureOfLatestCommitAtCommitIsRegistryFailure() throws Exception {
        final GitRepositoryClient repositoryClient = mockRepositoryClient(new AtomicReference<>("head-1"));
        when(repositoryClient.getLatestCommitShaAtCommit(FLOW_PATH, "head-1")).thenThrow(new UnsupportedOperationException("Not supported"));

        final TestGitFlowRegistryClient flowRegistryClient = createFlowRegistryClient(repositoryClient);
        final FlowRegistryClientConfigurationContext context = createContext("main", "[.].*");

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class, () -> flowRegistryClient.getLatestVersion(context, FLOW_LOCATION));
        assertInstanceOf(UnsupportedOperationException.class, exception.getCause());
    }

    private GitRepositoryClient mockRepositoryClient(final AtomicReference<String> head) throws Exception {
        final GitRepositoryClient repositoryClient = mock(GitRepositoryClient.class);
        when(repositoryClient.hasReadPermission()).thenReturn(true);
        when(repositoryClient.getBranchHeadSha("main")).thenAnswer(invocation -> Optional.of(head.get()));
        when(repositoryClient.getTopLevelDirectoryNames("main")).thenReturn(Set.of("bucket-a"));
        return repositoryClient;
    }

    private TestGitFlowRegistryClient createFlowRegistryClient(final GitRepositoryClient repositoryClient) {
        final TestGitFlowRegistryClient flowRegistryClient = new TestGitFlowRegistryClient(() -> repositoryClient, "git@example.git");
        flowRegistryClient.initialize(createInitializationContext());
        return flowRegistryClient;
    }

    private static RegisteredFlowSnapshot createFlowSnapshot(final String version) {
        final RegisteredFlowSnapshotMetadata snapshotMetadata = new RegisteredFlowSnapshotMetadata();
        snapshotMetadata.setBranch("main");
        snapshotMetadata.setBucketIdentifier("bucket-a");
        snapshotMetadata.setFlowIdentifier("flow-1");
        snapshotMetadata.setVersion(version);

        final VersionedProcessGroup flowContents = new VersionedProcessGroup();
        flowContents.setIdentifier("contents");

        final RegisteredFlowSnapshot flowSnapshot = new RegisteredFlowSnapshot();
        flowSnapshot.setSnapshotMetadata(snapshotMetadata);
        flowSnapshot.setFlowContents(flowContents);
        return flowSnapshot;
    }

    private static InputStream getFlowContent(final String name) {
        return new ByteArrayInputStream("""
                {"flow": {"identifier": "flow-1", "name": "%s"}, "flowContents": {"identifier": "contents"}}
                """.formatted(name).getBytes(StandardCharsets.UTF_8));
    }

    private FlowRegistryClientConfigurationContext createContext(final String branch, final String exclusionPattern) {
        final Map<PropertyDescriptor, PropertyValue> properties = Map.of(
                AbstractGitFlowRegistryClient.REPOSITORY_BRANCH, new MockPropertyValue(branch),
                AbstractGitFlowRegistryClient.DIRECTORY_FILTER_EXCLUDE, new MockPropertyValue(exclusionPattern),
                AbstractGitFlowRegistryClient.COMMIT_AUTHOR_SOURCE, new MockPropertyValue(AbstractGitFlowRegistryClient.CommitAuthorSource.SERVICE_USER.getValue()),
                AbstractGitFlowRegistryClient.PARAMETER_CONTEXT_VALUES, new MockPropertyValue(AbstractGitFlowRegistryClient.ParameterContextValuesStrategy.RETAIN.getValue())
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

    private FlowRegistryClientInitializationContext createInitializationContext() {
        return new FlowRegistryClientInitializationContext() {
            @Override
            public String getIdentifier() {
                return "test-git-client";
            }

            @Override
            public ComponentLog getLogger() {
                return new MockComponentLog("test-git-client", AbstractGitFlowRegistryClientTest.this);
            }

            @Override
            public Optional<SSLContext> getSystemSslContext() {
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
        protected GitRepositoryClient createRepositoryClient(final FlowRegistryClientConfigurationContext context) throws FlowRegistryException {
            return repositoryClientSupplier.get();
        }

        @Override
        public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String location) {
            return storageLocation == null || storageLocation.equals(location);
        }
    }

    private interface RepositoryClientSupplier {
        GitRepositoryClient get() throws FlowRegistryException;
    }

    private static class TestGitRepositoryClient implements GitRepositoryClient {
        private final boolean canRead;
        private final boolean canWrite;
        private final Set<String> bucketNames;
        private FlowRegistryException topLevelDirectoryNamesException;
        private IOException topLevelDirectoryNamesIOException;
        private boolean closed;
        private boolean branchCreationUnsupported;
        private FlowRegistryException createBranchException;
        private String createdBranch;
        private String createdBranchSource;
        private Optional<String> createdBranchCommit = Optional.empty();

        TestGitRepositoryClient(final boolean canRead, final boolean canWrite, final Set<String> bucketNames) {
            this.canRead = canRead;
            this.canWrite = canWrite;
            this.bucketNames = bucketNames;
        }

        void setGetTopLevelDirectoryNamesException(final FlowRegistryException exception) {
            this.topLevelDirectoryNamesException = exception;
            this.topLevelDirectoryNamesIOException = null;
        }

        void setGetTopLevelDirectoryNamesIOException(final IOException exception) {
            this.topLevelDirectoryNamesIOException = exception;
            this.topLevelDirectoryNamesException = null;
        }

        void setBranchCreationUnsupported(final boolean unsupported) {
            this.branchCreationUnsupported = unsupported;
        }

        void setCreateBranchException(final FlowRegistryException exception) {
            this.createBranchException = exception;
        }

        String getCreatedBranch() {
            return createdBranch;
        }

        String getCreatedBranchSource() {
            return createdBranchSource;
        }

        Optional<String> getCreatedBranchCommit() {
            return createdBranchCommit;
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
        public void createBranch(final String newBranchName, final String sourceBranch, final Optional<String> sourceCommitSha)
                throws FlowRegistryException {
            if (branchCreationUnsupported) {
                throw new UnsupportedOperationException("Branch creation not supported");
            }
            if (createBranchException != null) {
                throw createBranchException;
            }

            createdBranch = newBranchName;
            createdBranchSource = sourceBranch;
            createdBranchCommit = sourceCommitSha;
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
            return "test-commit";
        }

        @Override
        public InputStream deleteContent(final String filePath, final String commitMessage, final String branch) {
            throw new UnsupportedOperationException("Not required for test");
        }
    }
}
