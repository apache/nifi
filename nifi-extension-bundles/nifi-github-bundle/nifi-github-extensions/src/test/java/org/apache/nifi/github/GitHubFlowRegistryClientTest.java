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

package org.apache.nifi.github;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryClientInitializationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.RegisterAction;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GitHubFlowRegistryClientTest {

    static final String DEFAULT_REPO_PATH = "some-path";
    static final String DEFAULT_REPO_BRANCH = "some-branch";

    private GitHubRepositoryClient repositoryClient;
    private FlowSnapshotSerializer flowSnapshotSerializer;
    private GitHubFlowRegistryClient flowRegistryClient;
    private FlowRegistryClientConfigurationContext clientConfigurationContext;
    private ComponentLog componentLog;

    @BeforeEach
    public void setup() throws IOException, FlowRegistryException {
        repositoryClient = mock(GitHubRepositoryClient.class);
        flowSnapshotSerializer = mock(FlowSnapshotSerializer.class);
        flowRegistryClient = new TestableGitHubRepositoryClient(repositoryClient, flowSnapshotSerializer);

        clientConfigurationContext = mock(FlowRegistryClientConfigurationContext.class);
        componentLog = mock(ComponentLog.class);

        final FlowRegistryClientInitializationContext initializationContext = mock(FlowRegistryClientInitializationContext.class);
        when(initializationContext.getLogger()).thenReturn(componentLog);
        flowRegistryClient.initialize(initializationContext);

        when(repositoryClient.getCanRead()).thenReturn(true);
        when(repositoryClient.getCanWrite()).thenReturn(true);
        when(repositoryClient.getTopLevelDirectoryNames(anyString())).thenReturn(Set.of("existing-bucket"));
    }

    @Test
    public void testRegisterFlow() throws IOException, FlowRegistryException {
        setupClientConfigurationContextWithDefaults();

        final String serializedSnapshotContent = "placeholder";
        when(flowSnapshotSerializer.serialize(any(RegisteredFlowSnapshot.class))).thenReturn(serializedSnapshotContent);

        final RegisteredFlow incomingFlow = createIncomingRegisteredFlow();
        final RegisteredFlow resultFlow = flowRegistryClient.registerFlow(clientConfigurationContext, incomingFlow);
        assertEquals(incomingFlow.getIdentifier(), resultFlow.getIdentifier());
        assertEquals(incomingFlow.getName(), resultFlow.getName());
        assertEquals(incomingFlow.getBucketIdentifier(), resultFlow.getBucketIdentifier());
        assertEquals(incomingFlow.getBucketName(), resultFlow.getBucketName());
        assertEquals(incomingFlow.getBranch(), resultFlow.getBranch());

        final ArgumentCaptor<GitHubCreateContentRequest> argumentCaptor = ArgumentCaptor.forClass(GitHubCreateContentRequest.class);
        verify(repositoryClient).createContent(argumentCaptor.capture());

        final GitHubCreateContentRequest capturedArgument = argumentCaptor.getValue();
        assertEquals(incomingFlow.getBranch(), capturedArgument.getBranch());
        assertEquals(GitHubFlowRegistryClient.SNAPSHOT_FILE_PATH_FORMAT.formatted(incomingFlow.getBucketIdentifier(), incomingFlow.getIdentifier()), capturedArgument.getPath());
        assertEquals(serializedSnapshotContent, capturedArgument.getContent());
        assertNull(capturedArgument.getExistingContentSha());
    }

    @Test
    public void testRegisterFlowSnapshot() throws IOException, FlowRegistryException {
        setupClientConfigurationContextWithDefaults();

        final RegisteredFlow incomingFlow = createIncomingRegisteredFlow();

        final RegisteredFlowSnapshotMetadata incomingMetadata = new RegisteredFlowSnapshotMetadata();
        incomingMetadata.setBranch(incomingFlow.getBranch());
        incomingMetadata.setBucketIdentifier(incomingFlow.getBucketIdentifier());
        incomingMetadata.setFlowIdentifier(incomingFlow.getIdentifier());
        incomingMetadata.setComments("Unit test");

        final RegisteredFlowSnapshot incomingSnapshot = new RegisteredFlowSnapshot();
        incomingSnapshot.setFlow(incomingFlow);
        incomingSnapshot.setSnapshotMetadata(incomingMetadata);
        incomingSnapshot.setFlowContents(new VersionedProcessGroup());

        final String snapshotFilePath = GitHubFlowRegistryClient.SNAPSHOT_FILE_PATH_FORMAT.formatted(incomingFlow.getBucketIdentifier(), incomingFlow.getIdentifier());
        when(repositoryClient.getContentFromBranch(snapshotFilePath, DEFAULT_REPO_BRANCH)).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(flowSnapshotSerializer.deserialize(any(InputStream.class))).thenReturn(incomingSnapshot);

        final String serializedSnapshotContent = "placeholder";
        when(flowSnapshotSerializer.serialize(any(RegisteredFlowSnapshot.class))).thenReturn(serializedSnapshotContent);

        final String commitSha = "commitSha";
        when(repositoryClient.createContent(any(GitHubCreateContentRequest.class))).thenReturn(commitSha);

        final RegisteredFlowSnapshot resultSnapshot = flowRegistryClient.registerFlowSnapshot(clientConfigurationContext, incomingSnapshot, RegisterAction.COMMIT);
        assertNotNull(resultSnapshot);

        final RegisteredFlow resultFlow = resultSnapshot.getFlow();
        assertNotNull(resultFlow);
        assertEquals(incomingFlow.getIdentifier(), resultFlow.getIdentifier());
        assertEquals(incomingFlow.getName(), resultFlow.getName());
        assertEquals(incomingFlow.getBucketIdentifier(), resultFlow.getBucketIdentifier());
        assertEquals(incomingFlow.getBucketName(), resultFlow.getBucketName());
        assertEquals(incomingFlow.getBranch(), resultFlow.getBranch());

        final RegisteredFlowSnapshotMetadata resultMetadata = resultSnapshot.getSnapshotMetadata();
        assertNotNull(resultMetadata);
        assertEquals(incomingMetadata.getBranch(), resultMetadata.getBranch());
        assertEquals(incomingMetadata.getBucketIdentifier(), resultMetadata.getBucketIdentifier());
        assertEquals(incomingMetadata.getFlowIdentifier(), resultMetadata.getFlowIdentifier());
        assertEquals(incomingMetadata.getComments(), resultMetadata.getComments());

        final FlowRegistryBucket resultBucket = resultSnapshot.getBucket();
        assertNotNull(resultBucket);
        assertEquals(incomingMetadata.getBucketIdentifier(), resultBucket.getIdentifier());
        assertEquals(incomingMetadata.getBucketIdentifier(), resultBucket.getName());
    }

    private RegisteredFlow createIncomingRegisteredFlow() {
        final RegisteredFlow incomingFlow = new RegisteredFlow();
        incomingFlow.setIdentifier("Flow1");
        incomingFlow.setName("Flow1");
        incomingFlow.setBucketIdentifier("Bucket1");
        incomingFlow.setBucketName("Bucket1");
        incomingFlow.setBranch(DEFAULT_REPO_BRANCH);
        return incomingFlow;
    }

    private void setupClientConfigurationContext(final String repoPath, final String branch) {
        final PropertyValue repoPathPropertyValue = createMockPropertyValue(repoPath);
        when(clientConfigurationContext.getProperty(GitHubFlowRegistryClient.REPOSITORY_PATH)).thenReturn(repoPathPropertyValue);

        final PropertyValue branchPropertyValue = createMockPropertyValue(branch);
        when(clientConfigurationContext.getProperty(GitHubFlowRegistryClient.REPOSITORY_BRANCH)).thenReturn(branchPropertyValue);
    }

    private void setupClientConfigurationContextWithDefaults() {
        setupClientConfigurationContext(DEFAULT_REPO_PATH, DEFAULT_REPO_BRANCH);
    }

    private PropertyValue createMockPropertyValue(final String value) {
        final PropertyValue propertyValue = mock(PropertyValue.class);
        when(propertyValue.getValue()).thenReturn(value);
        return propertyValue;
    }

    private static class TestableGitHubRepositoryClient extends GitHubFlowRegistryClient {
        private final GitHubRepositoryClient repositoryClient;
        private final FlowSnapshotSerializer flowSnapshotSerializer;

        public TestableGitHubRepositoryClient(final GitHubRepositoryClient repositoryClient, final FlowSnapshotSerializer flowSnapshotSerializer) {
            this.repositoryClient = repositoryClient;
            this.flowSnapshotSerializer = flowSnapshotSerializer;
        }

        @Override
        protected GitHubRepositoryClient createRepositoryClient(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException {
            return repositoryClient;
        }

        @Override
        protected FlowSnapshotSerializer createFlowSnapshotSerializer(final FlowRegistryClientInitializationContext initializationContext) {
            return flowSnapshotSerializer;
        }
    }

}
