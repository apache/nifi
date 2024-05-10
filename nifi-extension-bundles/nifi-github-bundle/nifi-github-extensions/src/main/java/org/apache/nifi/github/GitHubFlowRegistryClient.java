/*
 *
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
 *
 */
package org.apache.nifi.github;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.flow.AbstractFlowRegistryClient;
import org.apache.nifi.registry.flow.AuthorizationException;
import org.apache.nifi.registry.flow.BucketLocation;
import org.apache.nifi.registry.flow.FlowAlreadyExistsException;
import org.apache.nifi.registry.flow.FlowLocation;
import org.apache.nifi.registry.flow.FlowRegistryBranch;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryClientInitializationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowRegistryPermissions;
import org.apache.nifi.registry.flow.FlowVersionLocation;
import org.apache.nifi.registry.flow.RegisterAction;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHContent;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Implementation of {@link org.apache.nifi.registry.flow.FlowRegistryClient} that uses GitHub for version controlling flows.
 */
public class GitHubFlowRegistryClient extends AbstractFlowRegistryClient {

    static final PropertyDescriptor GITHUB_API_URL = new PropertyDescriptor.Builder()
            .name("GitHub API URL")
            .description("The URL of the GitHub API")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .defaultValue("https://api.github.com/")
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_NAME = new PropertyDescriptor.Builder()
            .name("Repository Name")
            .description("The name of the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_OWNER = new PropertyDescriptor.Builder()
            .name("Repository Owner")
            .description("The owner of the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_BRANCH = new PropertyDescriptor.Builder()
            .name("Default Branch")
            .description("The default branch to use for this client")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("main")
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_PATH = new PropertyDescriptor.Builder()
            .name("Repository Path")
            .description("The path with in the repository that this client will use to store all data. " +
                    "If left blank, then the root of the repository will be used.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
            .name("Authentication Type")
            .description("The type of authentication to use for accessing GitHub")
            .allowableValues(GitHubAuthenticationType.values())
            .defaultValue(GitHubAuthenticationType.NONE.name())
            .required(true)
            .build();

    static final PropertyDescriptor PERSONAL_ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Personal Access Token")
            .description("The personal access token to use for authentication")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .dependsOn(AUTHENTICATION_TYPE, GitHubAuthenticationType.PERSONAL_ACCESS_TOKEN.name())
            .build();

    static final PropertyDescriptor APP_INSTALLATION_TOKEN = new PropertyDescriptor.Builder()
            .name("App Installation Token")
            .description("The app installation token to use for authentication")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .dependsOn(AUTHENTICATION_TYPE, GitHubAuthenticationType.APP_INSTALLATION_TOKEN.name())
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GITHUB_API_URL,
            REPOSITORY_OWNER,
            REPOSITORY_NAME,
            REPOSITORY_BRANCH,
            REPOSITORY_PATH,
            AUTHENTICATION_TYPE,
            PERSONAL_ACCESS_TOKEN,
            APP_INSTALLATION_TOKEN
    );

    static final String DEFAULT_BUCKET_NAME = "default";
    static final String DEFAULT_BUCKET_KEEP_FILE_PATH = DEFAULT_BUCKET_NAME + "/.keep";
    static final String DEFAULT_BUCKET_KEEP_FILE_CONTENT = "Do Not Delete";
    static final String DEFAULT_BUCKET_KEEP_FILE_MESSAGE = "Creating default bucket";

    static final String REGISTER_FLOW_MESSAGE_PREFIX = "Registering Flow";
    static final String REGISTER_FLOW_MESSAGE_FORMAT = REGISTER_FLOW_MESSAGE_PREFIX + " [%s]";
    static final String DEREGISTER_FLOW_MESSAGE_FORMAT = "Deregistering Flow [%s]";
    static final String DEFAULT_FLOW_SNAPSHOT_MESSAGE_FORMAT = "Saving Flow Snapshot %s";
    static final String SNAPSHOT_FILE_EXTENSION = ".json";
    static final String SNAPSHOT_FILE_PATH_FORMAT = "%s/%s" + SNAPSHOT_FILE_EXTENSION;
    static final String FLOW_CONTENTS_GROUP_ID = "flow-contents-group";

    static final String STORAGE_LOCATION_PREFIX = "git@github.com:";
    static final String STORAGE_LOCATION_FORMAT = STORAGE_LOCATION_PREFIX + "%s/%s.git";

    private volatile FlowSnapshotSerializer flowSnapshotSerializer;
    private volatile GitHubRepositoryClient repositoryClient;
    private final AtomicBoolean clientInitialized = new AtomicBoolean(false);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final String repoPath = validationContext.getProperty(REPOSITORY_PATH).getValue();
        if (repoPath != null && (repoPath.startsWith("/") || repoPath.endsWith("/"))) {
            results.add(new ValidationResult.Builder()
                    .subject(REPOSITORY_PATH.getDisplayName())
                    .valid(false)
                    .explanation("Path can not start or end with /")
                    .build());
        }

        return results;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        synchronized (this) {
            clientInitialized.set(false);
        }
    }

    @Override
    public void initialize(final FlowRegistryClientInitializationContext context) {
        super.initialize(context);
        flowSnapshotSerializer = createFlowSnapshotSerializer(context);
    }

    // protected to allow for overriding from tests
    protected FlowSnapshotSerializer createFlowSnapshotSerializer(final FlowRegistryClientInitializationContext initializationContext) {
        return new JacksonFlowSnapshotSerializer();
    }

    @Override
    public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String location) {
        return location != null && location.startsWith(STORAGE_LOCATION_PREFIX);
    }

    @Override
    public boolean isBranchingSupported(final FlowRegistryClientConfigurationContext context) {
        return true;
    }

    @Override
    public Set<FlowRegistryBranch> getBranches(final FlowRegistryClientConfigurationContext context) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        return repositoryClient.getBranches().stream()
                .map(branchName -> {
                    final FlowRegistryBranch flowRegistryBranch = new FlowRegistryBranch();
                    flowRegistryBranch.setName(branchName);
                    return flowRegistryBranch;
                }).collect(Collectors.toSet());
    }

    @Override
    public FlowRegistryBranch getDefaultBranch(final FlowRegistryClientConfigurationContext context) {
        final FlowRegistryBranch defaultBranch = new FlowRegistryBranch();
        defaultBranch.setName(context.getProperty(REPOSITORY_BRANCH).getValue());
        return defaultBranch;
    }

    @Override
    public Set<FlowRegistryBucket> getBuckets(final FlowRegistryClientConfigurationContext context, final String branch) throws IOException, FlowRegistryException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final Set<FlowRegistryBucket> buckets = repositoryClient.getTopLevelDirectoryNames(branch).stream()
                .map(bucketName -> createFlowRegistryBucket(repositoryClient, bucketName))
                .collect(Collectors.toSet());

        // if the repository has no top-level directories, then return a default bucket entry, this won't exist in the repository until the first time a flow is saved to it
        return buckets.isEmpty() ? Set.of(createFlowRegistryBucket(repositoryClient, DEFAULT_BUCKET_NAME)) : buckets;
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientConfigurationContext context, final BucketLocation bucketLocation) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);
        return createFlowRegistryBucket(repositoryClient, bucketLocation.getBucketId());
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientConfigurationContext context, final RegisteredFlow flow) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyWritePermissions(repositoryClient);

        final String branch = flow.getBranch();
        final FlowLocation flowLocation = new FlowLocation(branch, flow.getBucketIdentifier(), flow.getIdentifier());
        final String filePath = getSnapshotFilePath(flowLocation);
        final String commitMessage = REGISTER_FLOW_MESSAGE_FORMAT.formatted(flow.getIdentifier());

        final Optional<String> existingFileSha = repositoryClient.getContentSha(filePath, branch);
        if (existingFileSha.isPresent()) {
            throw new FlowAlreadyExistsException("Another flow is already registered at [" + filePath + "] on branch [" + branch + "]");
        }

        // Clear values we don't want in the json stored in GitHub
        final String originalBucketId = flow.getBucketIdentifier();
        flow.setBucketIdentifier(null);
        flow.setBucketName(null);
        flow.setBranch(null);

        final RegisteredFlowSnapshot flowSnapshot = new RegisteredFlowSnapshot();
        flowSnapshot.setFlow(flow);

        final GitHubCreateContentRequest request = GitHubCreateContentRequest.builder()
                .branch(branch)
                .path(filePath)
                .content(flowSnapshotSerializer.serialize(flowSnapshot))
                .message(commitMessage)
                .build();

        repositoryClient.createContent(request);

        // Re-populate fields before returning
        flow.setBucketName(originalBucketId);
        flow.setBucketIdentifier(originalBucketId);
        flow.setBranch(branch);

        return flow;
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyWritePermissions(repositoryClient);

        final String branch = flowLocation.getBranch();
        final String filePath = getSnapshotFilePath(flowLocation);
        final String commitMessage = DEREGISTER_FLOW_MESSAGE_FORMAT.formatted(flowLocation.getFlowId());
        final GHContent deletedSnapshotContent = repositoryClient.deleteContent(filePath, commitMessage, branch);

        final RegisteredFlowSnapshot deletedSnapshot = getSnapshot(deletedSnapshotContent.read());
        updateBucketReferences(repositoryClient, deletedSnapshot, flowLocation.getBucketId());
        return deletedSnapshot.getFlow();
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final String branch = flowLocation.getBranch();
        final String filePath = getSnapshotFilePath(flowLocation);

        final RegisteredFlowSnapshot existingSnapshot = getSnapshot(filePath, branch);
        populateFlowAndSnapshotMetadata(existingSnapshot, flowLocation);
        updateBucketReferences(repositoryClient, existingSnapshot, flowLocation.getBucketId());

        final RegisteredFlow registeredFlow = existingSnapshot.getFlow();
        registeredFlow.setBranch(branch);
        return registeredFlow;
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientConfigurationContext context, final BucketLocation bucketLocation) throws IOException, FlowRegistryException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final String branch = bucketLocation.getBranch();
        final String bucketId = bucketLocation.getBucketId();

        return repositoryClient.getFileNames(bucketId, branch).stream()
                .filter(filename -> filename.endsWith(SNAPSHOT_FILE_EXTENSION))
                .map(filename -> mapToRegisteredFlow(bucketLocation, filename))
                .collect(Collectors.toSet());
    }

    @Override
    public RegisteredFlowSnapshot getFlowContents(final FlowRegistryClientConfigurationContext context, final FlowVersionLocation flowVersionLocation)
            throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final String version = flowVersionLocation.getVersion();
        final String filePath = getSnapshotFilePath(flowVersionLocation);

        final InputStream inputStream = repositoryClient.getContentFromCommit(filePath, version);
        final RegisteredFlowSnapshot flowSnapshot = getSnapshot(inputStream);
        populateFlowAndSnapshotMetadata(flowSnapshot, flowVersionLocation);

        // populate values that aren't store in GitHub
        flowSnapshot.getSnapshotMetadata().setVersion(version);
        flowSnapshot.getSnapshotMetadata().setBranch(flowVersionLocation.getBranch());
        flowSnapshot.getFlow().setBranch(flowVersionLocation.getBranch());

        // populate outgoing bucket references
        updateBucketReferences(repositoryClient, flowSnapshot, flowVersionLocation.getBucketId());

        // determine if the version is the "latest" version by comparing to the response of getLatestVersion
        final String latestVersion = getLatestVersion(context, flowVersionLocation).orElse(null);
        flowSnapshot.setLatest(version.equals(latestVersion));

        return flowSnapshot;
    }

    @Override
    public RegisteredFlowSnapshot registerFlowSnapshot(final FlowRegistryClientConfigurationContext context, final RegisteredFlowSnapshot flowSnapshot, final RegisterAction action)
            throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyWritePermissions(repositoryClient);

        final RegisteredFlowSnapshotMetadata snapshotMetadata = flowSnapshot.getSnapshotMetadata();
        final String branch = snapshotMetadata.getBranch();
        final FlowLocation flowLocation = new FlowLocation(snapshotMetadata.getBranch(), snapshotMetadata.getBucketIdentifier(), snapshotMetadata.getFlowIdentifier());
        final String filePath = getSnapshotFilePath(flowLocation);
        final String previousSha = repositoryClient.getContentSha(filePath, branch).orElse(null);

        final String snapshotComments = snapshotMetadata.getComments();
        final String commitMessage = StringUtils.isBlank(snapshotComments) ? DEFAULT_FLOW_SNAPSHOT_MESSAGE_FORMAT.formatted(flowLocation.getFlowId()) : snapshotComments;

        final RegisteredFlowSnapshot existingSnapshot = getSnapshot(filePath, branch);
        populateFlowAndSnapshotMetadata(existingSnapshot, flowLocation);

        final RegisteredFlow existingFlow = existingSnapshot.getFlow();
        existingFlow.setBranch(null);
        flowSnapshot.setFlow(existingFlow);

        // Clear values we don't want stored in the json in GitHub
        flowSnapshot.setBucket(null);
        flowSnapshot.getSnapshotMetadata().setBucketIdentifier(null);
        flowSnapshot.getSnapshotMetadata().setBranch(null);
        flowSnapshot.getSnapshotMetadata().setVersion(null);
        flowSnapshot.getSnapshotMetadata().setComments(null);
        flowSnapshot.getSnapshotMetadata().setTimestamp(0);

        // replace the id of the top level group and all of its references with a constant value prior to serializing to avoid
        // unnecessary diffs when different instances of the same flow are imported and have different top-level PG ids
        final String originalFlowContentsGroupId = replaceGroupId(flowSnapshot.getFlowContents(), FLOW_CONTENTS_GROUP_ID);
        final Position originalFlowContentsPosition = replacePosition(flowSnapshot.getFlowContents(), new Position(0, 0));

        final GitHubCreateContentRequest createContentRequest = GitHubCreateContentRequest.builder()
                .branch(branch)
                .path(filePath)
                .content(flowSnapshotSerializer.serialize(flowSnapshot))
                .message(commitMessage)
                .existingContentSha(previousSha)
                .build();

        final String createContentCommitSha = repositoryClient.createContent(createContentRequest);

        final VersionedFlowCoordinates versionedFlowCoordinates = new VersionedFlowCoordinates();
        versionedFlowCoordinates.setRegistryId(getIdentifier());
        versionedFlowCoordinates.setBranch(flowLocation.getBranch());
        versionedFlowCoordinates.setBucketId(flowLocation.getBucketId());
        versionedFlowCoordinates.setFlowId(flowLocation.getFlowId());
        versionedFlowCoordinates.setVersion(createContentCommitSha);
        versionedFlowCoordinates.setStorageLocation(getStorageLocation(repositoryClient));

        flowSnapshot.getFlowContents().setVersionedFlowCoordinates(versionedFlowCoordinates);
        flowSnapshot.getFlow().setBranch(branch);
        flowSnapshot.getSnapshotMetadata().setBranch(branch);
        flowSnapshot.getSnapshotMetadata().setVersion(createContentCommitSha);
        flowSnapshot.setLatest(true);

        // populate outgoing bucket references
        updateBucketReferences(repositoryClient, flowSnapshot, flowLocation.getBucketId());

        // set back to the original id so that the returned snapshot is has the correct values from what was passed in
        replaceGroupId(flowSnapshot.getFlowContents(), originalFlowContentsGroupId);
        replacePosition(flowSnapshot.getFlowContents(), originalFlowContentsPosition);

        return flowSnapshot;
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation)
            throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final String branch = flowLocation.getBranch();
        final String filePath = getSnapshotFilePath(flowLocation);

        final Set<RegisteredFlowSnapshotMetadata> snapshotMetadataSet = new LinkedHashSet<>();
        for (final GHCommit ghCommit : repositoryClient.getCommits(filePath, branch)) {
            final RegisteredFlowSnapshotMetadata snapshotMetadata = createSnapshotMetadata(ghCommit, flowLocation);
            if (snapshotMetadata.getComments() != null && snapshotMetadata.getComments().startsWith(REGISTER_FLOW_MESSAGE_PREFIX)) {
                continue;
            }
            snapshotMetadataSet.add(snapshotMetadata);
        }
        return snapshotMetadataSet;
    }

    @Override
    public Optional<String> getLatestVersion(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        final GitHubRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final String branch = flowLocation.getBranch();
        final String filePath = getSnapshotFilePath(flowLocation);

        final List<GHCommit> commits = repositoryClient.getCommits(filePath, branch);
        final String latestVersion = commits.isEmpty() ? null : commits.getFirst().getSHA1();
        return Optional.ofNullable(latestVersion);
    }

    @Override
    public String generateFlowId(final String flowName) {
        return flowName.trim()
                .replaceAll("\\s", "-") // replace whitespace with -
                .replaceAll("[^a-zA-Z0-9-]", "") // replace all other invalid chars with empty string
                .replaceAll("(-)\\1+", "$1"); // replace consecutive - with single -
    }

    private FlowRegistryBucket createFlowRegistryBucket(final GitHubRepositoryClient repositoryClient, final String name) {
        final FlowRegistryPermissions bucketPermissions = new FlowRegistryPermissions();
        bucketPermissions.setCanRead(repositoryClient.getCanRead());
        bucketPermissions.setCanWrite(repositoryClient.getCanWrite());
        bucketPermissions.setCanDelete(repositoryClient.getCanWrite());

        final FlowRegistryBucket bucket = new FlowRegistryBucket();
        bucket.setIdentifier(name);
        bucket.setName(name);
        bucket.setPermissions(bucketPermissions);
        return bucket;
    }

    private RegisteredFlowSnapshotMetadata createSnapshotMetadata(final GHCommit ghCommit, final FlowLocation flowLocation) throws IOException {
        final GHCommit.ShortInfo shortInfo = ghCommit.getCommitShortInfo();

        final RegisteredFlowSnapshotMetadata snapshotMetadata = new RegisteredFlowSnapshotMetadata();
        snapshotMetadata.setBranch(flowLocation.getBranch());
        snapshotMetadata.setBucketIdentifier(flowLocation.getBucketId());
        snapshotMetadata.setFlowIdentifier(flowLocation.getFlowId());
        snapshotMetadata.setVersion(ghCommit.getSHA1());
        snapshotMetadata.setAuthor(ghCommit.getAuthor().getLogin());
        snapshotMetadata.setComments(shortInfo.getMessage());
        snapshotMetadata.setTimestamp(shortInfo.getCommitDate().getTime());
        return snapshotMetadata;
    }

    private RegisteredFlow mapToRegisteredFlow(final BucketLocation bucketLocation, final String filename) {
        final String branch = bucketLocation.getBranch();
        final String bucketId = bucketLocation.getBucketId();
        final String flowId = filename.replace(SNAPSHOT_FILE_EXTENSION, "");

        final RegisteredFlow registeredFlow = new RegisteredFlow();
        registeredFlow.setIdentifier(flowId);
        registeredFlow.setName(flowId);
        registeredFlow.setBranch(branch);
        registeredFlow.setBucketIdentifier(bucketId);
        registeredFlow.setBucketName(bucketId);
        return registeredFlow;
    }

    private String getSnapshotFilePath(final FlowLocation flowLocation) {
        return SNAPSHOT_FILE_PATH_FORMAT.formatted(flowLocation.getBucketId(), flowLocation.getFlowId());
    }

    private RegisteredFlowSnapshot getSnapshot(final String filePath, final String branch) throws IOException, FlowRegistryException {
        try (final InputStream contentInputStream = repositoryClient.getContentFromBranch(filePath, branch)) {
            return flowSnapshotSerializer.deserialize(contentInputStream);
        }
    }

    private RegisteredFlowSnapshot getSnapshot(final InputStream inputStream) throws IOException {
        try {
            return flowSnapshotSerializer.deserialize(inputStream);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    private Position replacePosition(final VersionedProcessGroup group, final Position newPosition) {
        final Position originalPosition = group.getPosition();
        group.setPosition(newPosition);
        return originalPosition;
    }

    private String replaceGroupId(final VersionedProcessGroup group, final String newGroupId) {
        final String originalGroupId = group.getIdentifier();
        group.setIdentifier(newGroupId);

        replaceGroupId(group.getProcessGroups(), newGroupId);
        replaceGroupId(group.getRemoteProcessGroups(), newGroupId);
        replaceGroupId(group.getProcessors(), newGroupId);
        replaceGroupId(group.getFunnels(), newGroupId);
        replaceGroupId(group.getLabels(), newGroupId);
        replaceGroupId(group.getInputPorts(), newGroupId);
        replaceGroupId(group.getOutputPorts(), newGroupId);
        replaceGroupId(group.getControllerServices(), newGroupId);
        replaceGroupId(group.getConnections(), newGroupId);

        if (group.getConnections() != null) {
            for (final VersionedConnection connection : group.getConnections()) {
                replaceGroupId(connection.getSource(), originalGroupId, newGroupId);
                replaceGroupId(connection.getDestination(), originalGroupId, newGroupId);
            }
        }

        return originalGroupId;
    }

    private <T extends VersionedComponent> void replaceGroupId(final Collection<T> components, final String newGroupIdentifier) {
        if (components == null) {
            return;
        }
        components.forEach(c -> c.setGroupIdentifier(newGroupIdentifier));
    }

    private void replaceGroupId(final ConnectableComponent connectableComponent, final String originalGroupId, final String newGroupId) {
        if (connectableComponent == null) {
            return;
        }

        if (originalGroupId.equals(connectableComponent.getGroupId())) {
            connectableComponent.setGroupId(newGroupId);
        }
    }

    private void updateBucketReferences(final GitHubRepositoryClient repositoryClient, final RegisteredFlowSnapshot flowSnapshot, final String bucketId) {
        final FlowRegistryBucket bucket = createFlowRegistryBucket(repositoryClient, bucketId);
        flowSnapshot.setBucket(bucket);

        final RegisteredFlow flow = flowSnapshot.getFlow();
        flow.setBucketName(bucketId);
        flow.setBucketIdentifier(bucketId);

        final RegisteredFlowSnapshotMetadata snapshotMetadata = flowSnapshot.getSnapshotMetadata();
        snapshotMetadata.setBucketIdentifier(bucketId);
    }

    // Ensures the snapshot has non-null flow and metadata fields, which would only be null if taking a flow from "Download Flow Definition" and adding directly to GitHub
    private void populateFlowAndSnapshotMetadata(final RegisteredFlowSnapshot flowSnapshot, final FlowLocation flowLocation) {
        if (flowSnapshot.getFlow() == null) {
            final RegisteredFlow registeredFlow = new RegisteredFlow();
            registeredFlow.setName(flowLocation.getFlowId());
            registeredFlow.setIdentifier(flowLocation.getFlowId());
            flowSnapshot.setFlow(registeredFlow);
        }
        if (flowSnapshot.getSnapshotMetadata() == null) {
            final RegisteredFlowSnapshotMetadata snapshotMetadata = new RegisteredFlowSnapshotMetadata();
            snapshotMetadata.setFlowIdentifier(flowLocation.getFlowId());
            flowSnapshot.setSnapshotMetadata(snapshotMetadata);
        }
    }

    private String getStorageLocation(final GitHubRepositoryClient repositoryClient) {
        return STORAGE_LOCATION_FORMAT.formatted(repositoryClient.getRepoOwner(), repositoryClient.getRepoName());
    }

    private void verifyWritePermissions(final GitHubRepositoryClient repositoryClient) throws AuthorizationException {
        if (!repositoryClient.getCanWrite()) {
            throw new AuthorizationException("Client does not have write access to the GitHub repository");
        }
    }

    private void verifyReadPermissions(final GitHubRepositoryClient repositoryClient) throws AuthorizationException {
        if (!repositoryClient.getCanRead()) {
            throw new AuthorizationException("Client does not have read access to the GitHub repository");
        }
    }

    private synchronized GitHubRepositoryClient getRepositoryClient(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException {
        if (!clientInitialized.get()) {
            getLogger().info("Initializing GitHub repository client");
            repositoryClient = createRepositoryClient(context);
            clientInitialized.set(true);
            initializeDefaultBucket(context);
        }
        return repositoryClient;
    }

    // protected so can be overridden during tests
    protected GitHubRepositoryClient createRepositoryClient(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException {
        return GitHubRepositoryClient.builder()
                .apiUrl(context.getProperty(GITHUB_API_URL).getValue())
                .authenticationType(GitHubAuthenticationType.valueOf(context.getProperty(AUTHENTICATION_TYPE).getValue()))
                .personalAccessToken(context.getProperty(PERSONAL_ACCESS_TOKEN).getValue())
                .appInstallationToken(context.getProperty(APP_INSTALLATION_TOKEN).getValue())
                .repoOwner(context.getProperty(REPOSITORY_OWNER).getValue())
                .repoName(context.getProperty(REPOSITORY_NAME).getValue())
                .repoPath(context.getProperty(REPOSITORY_PATH).getValue())
                .build();
    }

    // If the client has write permissions to the repo, then ensure the directory for the default bucket is present and if not create it,
    // otherwise the client can only be used to import flows from the repo and won't be able to set up the default bucket
    private void initializeDefaultBucket(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException {
        if (!repositoryClient.getCanWrite()) {
            getLogger().info("GitHub repository client does not have write permissions to the repository, skipping setup of default bucket");
            return;
        }

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final Set<String> bucketDirectoryNames = repositoryClient.getTopLevelDirectoryNames(branch);
        if (!bucketDirectoryNames.isEmpty()) {
            getLogger().info("Found existing buckets, skipping setup of default bucket");
            return;
        }

        getLogger().info("Creating default bucket in repo [{}/{}] on branch [{}]", repositoryClient.getRepoOwner(), repositoryClient.getRepoName(), branch);

        repositoryClient.createContent(
                GitHubCreateContentRequest.builder()
                        .branch(branch)
                        .path(DEFAULT_BUCKET_KEEP_FILE_PATH)
                        .content(DEFAULT_BUCKET_KEEP_FILE_CONTENT)
                        .message(DEFAULT_BUCKET_KEEP_FILE_MESSAGE)
                        .build()
        );
    }

}
