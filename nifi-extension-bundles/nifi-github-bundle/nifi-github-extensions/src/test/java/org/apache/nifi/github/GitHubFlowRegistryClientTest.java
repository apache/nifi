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
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
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
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.registry.flow.git.serialize.FlowSnapshotSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kohsuke.github.GitHubBuilder;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
    static final String DEFAULT_FILTER = "[.].*";

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

        when(repositoryClient.hasReadPermission()).thenReturn(true);
        when(repositoryClient.hasWritePermission()).thenReturn(true);
        when(repositoryClient.getTopLevelDirectoryNames(anyString())).thenReturn(Set.of("existing-bucket", ".github"));
    }

    @Test
    public void testGitHubClientInitializationFailsWithIncompatibleJackson() {
        assertDoesNotThrow(() -> new GitHubBuilder()
                .withEndpoint("https://api.github.com")
                .build());
    }

    @Test
    public void testDirExclusion() throws IOException, FlowRegistryException {
        setupClientConfigurationContextWithDefaults();
        final Set<FlowRegistryBucket> buckets = flowRegistryClient.getBuckets(clientConfigurationContext, DEFAULT_REPO_BRANCH);
        assertEquals(buckets.stream().filter(b -> b.getName().equals(".github")).count(), 0);
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

        final ArgumentCaptor<GitCreateContentRequest> argumentCaptor = ArgumentCaptor.forClass(GitCreateContentRequest.class);
        verify(repositoryClient).createContent(argumentCaptor.capture());

        final GitCreateContentRequest capturedArgument = argumentCaptor.getValue();
        assertEquals(incomingFlow.getBranch(), capturedArgument.getBranch());
        assertEquals("%s/%s.json".formatted(incomingFlow.getBucketIdentifier(), incomingFlow.getIdentifier()), capturedArgument.getPath());
        assertEquals(serializedSnapshotContent, capturedArgument.getContent());
        assertNull(capturedArgument.getExistingContentSha());
    }

    @Test
    public void testRegisterFlowSnapshot() throws IOException, FlowRegistryException {
        setupClientConfigurationContextWithDefaults();

        final RegisteredFlow incomingFlow = createIncomingRegisteredFlow();

        final long timestamp = System.currentTimeMillis();
        final RegisteredFlowSnapshotMetadata incomingMetadata = new RegisteredFlowSnapshotMetadata();
        incomingMetadata.setBranch(incomingFlow.getBranch());
        incomingMetadata.setBucketIdentifier(incomingFlow.getBucketIdentifier());
        incomingMetadata.setFlowIdentifier(incomingFlow.getIdentifier());
        incomingMetadata.setComments("Unit test");
        incomingMetadata.setTimestamp(timestamp);

        final RegisteredFlowSnapshot incomingSnapshot = new RegisteredFlowSnapshot();
        incomingSnapshot.setFlow(incomingFlow);
        incomingSnapshot.setSnapshotMetadata(incomingMetadata);
        incomingSnapshot.setFlowContents(new VersionedProcessGroup());

        final String snapshotFilePath = "%s/%s.json".formatted(incomingFlow.getBucketIdentifier(), incomingFlow.getIdentifier());
        when(repositoryClient.getContentFromBranch(snapshotFilePath, DEFAULT_REPO_BRANCH)).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(flowSnapshotSerializer.deserialize(any(InputStream.class))).thenReturn(incomingSnapshot);

        final String serializedSnapshotContent = "placeholder";
        when(flowSnapshotSerializer.serialize(any(RegisteredFlowSnapshot.class))).thenReturn(serializedSnapshotContent);

        final String commitSha = "commitSha";
        when(repositoryClient.createContent(any(GitCreateContentRequest.class))).thenReturn(commitSha);

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
        assertEquals(timestamp, resultMetadata.getTimestamp());

        final FlowRegistryBucket resultBucket = resultSnapshot.getBucket();
        assertNotNull(resultBucket);
        assertEquals(incomingMetadata.getBucketIdentifier(), resultBucket.getIdentifier());
        assertEquals(incomingMetadata.getBucketIdentifier(), resultBucket.getName());
    }

    @Test
    public void testRegisterFlowSnapshotWithIgnoreUpdatesParameterStrategy() throws IOException, FlowRegistryException {
        setupClientConfigurationContextWithDefaults();

        final PropertyValue parametersPropertyValue = createMockPropertyValueWithEnum("IGNORE_CHANGES");
        when(clientConfigurationContext.getProperty(GitHubFlowRegistryClient.PARAMETER_CONTEXT_VALUES)).thenReturn(parametersPropertyValue);

        final RegisteredFlow incomingFlow = createIncomingRegisteredFlow();

        // Create existing snapshot with PC1 (Parameter1=A) and PC2 (Parameter2=B)
        final RegisteredFlowSnapshot existingSnapshot = createExistingSnapshotWithParameters();

        // Create incoming snapshot with PC1 (Parameter1=X, Parameter2=Y, Parameter3=Z)
        final RegisteredFlowSnapshot incomingSnapshot = createIncomingSnapshotWithNewParameters(incomingFlow);

        final String snapshotFilePath = "%s/%s.json".formatted(incomingFlow.getBucketIdentifier(), incomingFlow.getIdentifier());

        // Mock the repository client to return the existing snapshot
        when(repositoryClient.getContentFromBranch(snapshotFilePath, DEFAULT_REPO_BRANCH))
                .thenReturn(new ByteArrayInputStream("existing content".getBytes()));
        when(flowSnapshotSerializer.deserialize(any(InputStream.class)))
                .thenReturn(existingSnapshot);

        final String serializedSnapshotContent = "serialized content";
        when(flowSnapshotSerializer.serialize(any(RegisteredFlowSnapshot.class)))
                .thenReturn(serializedSnapshotContent);

        final String commitSha = "commitSha";
        when(repositoryClient.createContent(any(GitCreateContentRequest.class)))
                .thenReturn(commitSha);

        // Execute the method under test
        final RegisteredFlowSnapshot resultSnapshot = flowRegistryClient.registerFlowSnapshot(
                clientConfigurationContext, incomingSnapshot, RegisterAction.COMMIT);

        // Verify the result
        assertNotNull(resultSnapshot);

        // Verify Parameter Context PC1
        VersionedParameterContext pc1 = resultSnapshot.getParameterContexts().get("PC1");
        assertNotNull(pc1);

        Map<String, VersionedParameter> pc1Params = pc1.getParameters()
                .stream()
                .collect(Collectors.toMap(VersionedParameter::getName, Function.identity()));

        // Parameter1 should keep existing value A (not updated to X)
        assertEquals("A", pc1Params.get("Parameter1").getValue());
        // Parameter2 should have new value Y (new parameter)
        assertEquals("Y", pc1Params.get("Parameter2").getValue());
        // Parameter3 should have new value Z (new parameter)
        assertEquals("Z", pc1Params.get("Parameter3").getValue());

        // Verify Parameter Context PC2
        VersionedParameterContext pc2 = resultSnapshot.getParameterContexts().get("PC2");
        assertNull(pc2);
    }

    private RegisteredFlowSnapshot createExistingSnapshotWithParameters() {
        final RegisteredFlowSnapshot existingSnapshot = new RegisteredFlowSnapshot();

        // Create existing flow
        final RegisteredFlow existingFlow = new RegisteredFlow();
        existingFlow.setIdentifier("flow-id");
        existingFlow.setName("Test Flow");
        existingFlow.setBucketIdentifier("bucket-id");
        existingFlow.setBucketName("Test Bucket");
        existingFlow.setBranch(DEFAULT_REPO_BRANCH);
        existingSnapshot.setFlow(existingFlow);

        // Create existing metadata
        final RegisteredFlowSnapshotMetadata existingMetadata = new RegisteredFlowSnapshotMetadata();
        existingMetadata.setBranch(DEFAULT_REPO_BRANCH);
        existingMetadata.setBucketIdentifier("bucket-id");
        existingMetadata.setFlowIdentifier("flow-id");
        existingSnapshot.setSnapshotMetadata(existingMetadata);

        // Create Parameter Context PC1 with Parameter1=A
        final VersionedParameterContext pc1 = new VersionedParameterContext();
        pc1.setName("PC1");

        final VersionedParameter param1 = new VersionedParameter();
        param1.setName("Parameter1");
        param1.setValue("A");
        pc1.setParameters(Set.of(param1));

        // Create Parameter Context PC2 with Parameter2=B
        final VersionedParameterContext pc2 = new VersionedParameterContext();
        pc2.setName("PC2");

        final VersionedParameter param2 = new VersionedParameter();
        param2.setName("Parameter2");
        param2.setValue("B");
        pc2.setParameters(Set.of(param2));

        // Set parameter contexts on existing snapshot
        final Map<String, VersionedParameterContext> existingParameterContexts = new HashMap<>();
        existingParameterContexts.put("PC1", pc1);
        existingParameterContexts.put("PC2", pc2);
        existingSnapshot.setParameterContexts(existingParameterContexts);

        // Set flow contents
        existingSnapshot.setFlowContents(new VersionedProcessGroup());

        return existingSnapshot;
    }

    private RegisteredFlowSnapshot createIncomingSnapshotWithNewParameters(RegisteredFlow incomingFlow) {
        final RegisteredFlowSnapshot incomingSnapshot = new RegisteredFlowSnapshot();
        incomingSnapshot.setFlow(incomingFlow);

        final long timestamp = System.currentTimeMillis();
        final RegisteredFlowSnapshotMetadata incomingMetadata = new RegisteredFlowSnapshotMetadata();
        incomingMetadata.setBranch(incomingFlow.getBranch());
        incomingMetadata.setBucketIdentifier(incomingFlow.getBucketIdentifier());
        incomingMetadata.setFlowIdentifier(incomingFlow.getIdentifier());
        incomingMetadata.setComments("Unit test with parameter updates");
        incomingMetadata.setTimestamp(timestamp);
        incomingSnapshot.setSnapshotMetadata(incomingMetadata);

        // Create Parameter Context PC1 with Parameter1=X, Parameter2=Y, Parameter3=Z
        final VersionedParameterContext pc1 = new VersionedParameterContext();
        pc1.setName("PC1");

        final VersionedParameter param1 = new VersionedParameter();
        param1.setName("Parameter1");
        param1.setValue("X"); // This should be ignored and keep value A

        final VersionedParameter param2 = new VersionedParameter();
        param2.setName("Parameter2");
        param2.setValue("Y"); // This is new and should be kept

        final VersionedParameter param3 = new VersionedParameter();
        param3.setName("Parameter3");
        param3.setValue("Z"); // This is new and should be kept

        pc1.setParameters(Set.of(param1, param2, param3));

        // Set parameter contexts on incoming snapshot (only PC1 in this case)
        final Map<String, VersionedParameterContext> incomingParameterContexts = new HashMap<>();
        incomingParameterContexts.put("PC1", pc1);
        incomingSnapshot.setParameterContexts(incomingParameterContexts);

        // Set flow contents
        incomingSnapshot.setFlowContents(new VersionedProcessGroup());

        return incomingSnapshot;
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

        final PropertyValue filterPropertyValue = createMockPropertyValue(DEFAULT_FILTER);
        when(clientConfigurationContext.getProperty(GitHubFlowRegistryClient.DIRECTORY_FILTER_EXCLUDE)).thenReturn(filterPropertyValue);

        final PropertyValue parametersPropertyValue = createMockPropertyValue("Retain");
        when(clientConfigurationContext.getProperty(GitHubFlowRegistryClient.PARAMETER_CONTEXT_VALUES)).thenReturn(parametersPropertyValue);
    }

    private void setupClientConfigurationContextWithDefaults() {
        setupClientConfigurationContext(DEFAULT_REPO_PATH, DEFAULT_REPO_BRANCH);
    }

    private PropertyValue createMockPropertyValue(final String value) {
        final PropertyValue propertyValue = mock(PropertyValue.class);
        when(propertyValue.getValue()).thenReturn(value);
        return propertyValue;
    }

    private PropertyValue createMockPropertyValueWithEnum(final String value) {
        final PropertyValue propertyValue = mock(PropertyValue.class);
        when(propertyValue.getValue()).thenReturn(value);

        // Mock asAllowableValue to find the enum constant that matches the string value
        when(propertyValue.asAllowableValue(any())).thenAnswer(invocation -> {
            Class<?> clazz = invocation.getArgument(0);
            if (clazz.isEnum()) {
                Object[] enumConstants = clazz.getEnumConstants();
                for (Object enumConstant : enumConstants) {
                    if (value.equals(enumConstant.toString())) {
                        return enumConstant;
                    }
                }
            }
            return null;
        });

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
        protected FlowSnapshotSerializer createFlowSnapshotSerializer() {
            return flowSnapshotSerializer;
        }
    }

}
