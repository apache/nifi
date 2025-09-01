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

package org.apache.nifi.flow.registry;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.flow.AbstractFlowRegistryClient;
import org.apache.nifi.registry.flow.BucketLocation;
import org.apache.nifi.registry.flow.FlowLocation;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryPermissions;
import org.apache.nifi.registry.flow.FlowVersionLocation;
import org.apache.nifi.registry.flow.RegisterAction;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.RegisteredFlowVersionInfo;
import org.apache.nifi.util.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileSystemFlowRegistryClient extends AbstractFlowRegistryClient {
    private static final String TEST_FLOWS_BUCKET = "test-flows";

    private static final Set<String> FLOW_IDS = Set.of(
            "first-flow",
            "flow-with-invalid-connection",
            "port-moved-groups",
            "Parent",
            "Child"
    );

    private final ObjectMapper objectMapper = new ObjectMapper();

    {
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
        .name("Directory")
        .displayName("Directory")
        .description("The root directory to store flows in")
        .required(true)
        .addValidator(StandardValidators.createDirectoryExistsValidator(false, false))
        .defaultValue("target/flow-registry-storage")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(DIRECTORY);
    }

    @Override
    public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String storageLocation) {
        try {
            final Path rootPath = getRootDirectory(context).toPath().normalize();
            final URI location = URI.create(storageLocation);
            final Path storageLocationPath = Paths.get(location.getPath()).normalize();

            if (storageLocationPath.startsWith(rootPath)) {
                // If this doesn't throw an Exception, the given storageLocation is relative to the root path
                Objects.requireNonNull(rootPath.relativize(storageLocationPath));
            } else {
                return false;
            }
        } catch (final Exception e) {
            return false;
        }

        return true;
    }

    @Override
    public Set<FlowRegistryBucket> getBuckets(final FlowRegistryClientConfigurationContext context, final String branch) throws IOException {
        final File rootDir = getRootDirectory(context);
        final File[] children = rootDir.listFiles();
        if (children == null) {
            throw new IOException("Cannot get listing of directory " + rootDir.getAbsolutePath());
        }

        return Arrays.stream(children).map(this::toBucket).collect(Collectors.toSet());
    }

    private FlowRegistryBucket toBucket(final File file) {
        final FlowRegistryBucket bucket = new FlowRegistryBucket();
        bucket.setIdentifier(file.getName());
        bucket.setName(bucket.getName());

        final FlowRegistryPermissions permissions = new FlowRegistryPermissions();
        permissions.setCanDelete(true);
        permissions.setCanRead(true);
        permissions.setCanWrite(true);

        bucket.setPermissions(permissions);
        return bucket;
    }

    private File getRootDirectory(final FlowRegistryClientConfigurationContext context) {
        final String rootDirectory = context.getProperty(DIRECTORY).getValue();
        if (rootDirectory == null) {
            throw new IllegalStateException("Registry Client cannot be used, as Directory property has not been set");
        }

        return new File(rootDirectory);
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientConfigurationContext context, final BucketLocation bucketLocation) {
        final File rootDir = getRootDirectory(context);
        final File bucketDir = getChildLocation(rootDir, getValidatedBucketPath(bucketLocation.getBucketId()));
        return toBucket(bucketDir);
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientConfigurationContext context, final RegisteredFlow flow) throws IOException {
        final String bucketId = flow.getBucketIdentifier();
        final File flowDir = getFlowDirectory(context, bucketId, flow.getIdentifier());
        Files.createDirectories(flowDir.toPath());

        return flow;
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws IOException {
        final String bucketId = flowLocation.getBucketId();
        final String flowId = flowLocation.getFlowId();

        final File flowDir = getFlowDirectory(context, bucketId, flowId);
        final File[] versionDirs = flowDir.listFiles();

        final RegisteredFlow flow = new RegisteredFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setBucketName(bucketId);
        flow.setIdentifier(flowId);
        flow.setLastModifiedTimestamp(flowDir.lastModified());
        flow.setVersionCount(versionDirs == null ? 0 : versionDirs.length);

        FileUtils.deleteFile(flowDir, true);
        return flow;
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) {
        final String bucketId = flowLocation.getBucketId();
        final String flowId = flowLocation.getFlowId();

        final File flowDir = getFlowDirectory(context, bucketId, flowId);
        final File[] versionDirs = flowDir.listFiles();

        final RegisteredFlow flow = new RegisteredFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setBucketName(bucketId);
        flow.setIdentifier(flowId);
        flow.setLastModifiedTimestamp(flowDir.lastModified());
        flow.setVersionCount(versionDirs == null ? 0 : versionDirs.length);

        return flow;
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientConfigurationContext context, final BucketLocation bucketLocation) throws IOException {
        final String bucketId = bucketLocation.getBucketId();
        final File rootDir = getRootDirectory(context);
        final File bucketDir = getChildLocation(rootDir, getValidatedBucketPath(bucketId));
        final File[] flowDirs = bucketDir.listFiles();
        if (flowDirs == null) {
            throw new IOException("Could not get listing of directory " + bucketDir);
        }

        final Set<RegisteredFlow> registeredFlows = new HashSet<>();
        for (final File flowDir : flowDirs) {
            final FlowLocation flowLocation = new FlowLocation();
            flowLocation.setBucketId(bucketId);
            flowLocation.setFlowId(flowDir.getName());

            final RegisteredFlow flow = getFlow(context, flowLocation);
            registeredFlows.add(flow);
        }

        return registeredFlows;
    }

    @Override
    public RegisteredFlowSnapshot getFlowContents(final FlowRegistryClientConfigurationContext context, final FlowVersionLocation flowVersionLocation) throws IOException {
        final String bucketId = flowVersionLocation.getBucketId();
        final String flowId = flowVersionLocation.getFlowId();
        final String version = flowVersionLocation.getVersion();

        final File flowDir = getFlowDirectory(context, bucketId, flowId);
        final Pattern intPattern = Pattern.compile("\\d+");
        final File[] versionFiles = flowDir.listFiles(file -> intPattern.matcher(file.getName()).matches());

        final JsonFactory factory = new JsonFactory(objectMapper);
        final File snapshotFile = getSnapshotFile(context, bucketId, flowId, version);
        try (final JsonParser parser = factory.createParser(snapshotFile)) {
            final RegisteredFlowSnapshot snapshot = parser.readValueAs(RegisteredFlowSnapshot.class);
            populateBucket(snapshot, bucketId);
            populateFlow(snapshot, bucketId, flowId, versionFiles == null ? 0 : versionFiles.length);

            final String latestVersion = getLatestVersion(context, flowVersionLocation).orElse(null);
            snapshot.setLatest(version.equals(latestVersion));

            return snapshot;
        }
    }

    private void populateBucket(final RegisteredFlowSnapshot snapshot, final String bucketId) {
        final FlowRegistryBucket existingBucket = snapshot.getBucket();
        if (existingBucket != null) {
            return;
        }

        final FlowRegistryBucket bucket = new FlowRegistryBucket();
        bucket.setCreatedTimestamp(System.currentTimeMillis());
        bucket.setIdentifier(bucketId);
        bucket.setName(bucketId);
        bucket.setPermissions(createAllowAllPermissions());
        snapshot.setBucket(bucket);

        snapshot.getSnapshotMetadata().setBucketIdentifier(bucketId);
    }

    private void populateFlow(final RegisteredFlowSnapshot snapshot, final String bucketId, final String flowId, final int numVersions) {
        final RegisteredFlow existingFlow = snapshot.getFlow();
        if (existingFlow != null) {
            return;
        }

        final RegisteredFlow flow = new RegisteredFlow();
        flow.setCreatedTimestamp(System.currentTimeMillis());
        flow.setLastModifiedTimestamp(System.currentTimeMillis());
        flow.setBucketIdentifier(bucketId);
        flow.setBucketName(bucketId);
        flow.setIdentifier(flowId);
        flow.setName(flowId);
        flow.setPermissions(createAllowAllPermissions());
        flow.setVersionCount(numVersions);

        final RegisteredFlowVersionInfo versionInfo = new RegisteredFlowVersionInfo();
        versionInfo.setVersion(numVersions);
        flow.setVersionInfo(versionInfo);

        snapshot.setFlow(flow);
        snapshot.getSnapshotMetadata().setFlowIdentifier(flowId);
    }

    @Override
    public RegisteredFlowSnapshot registerFlowSnapshot(final FlowRegistryClientConfigurationContext context, final RegisteredFlowSnapshot flowSnapshot, final RegisterAction registerAction)
            throws IOException {
        final RegisteredFlowSnapshotMetadata metadata = flowSnapshot.getSnapshotMetadata();
        final String bucketId = metadata.getBucketIdentifier();
        final String flowId = metadata.getFlowIdentifier();
        final File flowDir = getFlowDirectory(context, bucketId, flowId);
        final String version = metadata.getVersion() == null ? "1" : String.valueOf(Integer.parseInt(metadata.getVersion()) + 1);
        flowSnapshot.getSnapshotMetadata().setVersion(version);

        // Create the directory for the version, if it doesn't exist.
        final File versionDir = getChildLocation(flowDir, Paths.get(version));
        if (!versionDir.exists()) {
            Files.createDirectories(versionDir.toPath());
        }

        final File snapshotFile = getSnapshotFile(context, bucketId, flowId, version);

        final RegisteredFlowSnapshot fullyPopulated = fullyPopulate(flowSnapshot, flowDir);
        final JsonFactory factory = new JsonFactory(objectMapper);
        try (final JsonGenerator generator = factory.createGenerator(snapshotFile, JsonEncoding.UTF8)) {
            generator.writeObject(fullyPopulated);
        }

        return fullyPopulated;
    }

    private RegisteredFlowSnapshot fullyPopulate(final RegisteredFlowSnapshot requested, final File flowDir) {
        final RegisteredFlowSnapshot full = new RegisteredFlowSnapshot();
        full.setExternalControllerServices(requested.getExternalControllerServices());
        full.setFlowContents(requested.getFlowContents());
        full.setFlowEncodingVersion(requested.getFlowEncodingVersion());
        full.setParameterContexts(requested.getParameterContexts());
        full.setParameterProviders(requested.getParameterProviders());
        full.setSnapshotMetadata(requested.getSnapshotMetadata());

        // Populated the bucket
        final FlowRegistryBucket bucket;
        if (requested.getBucket() == null) {
            bucket = new FlowRegistryBucket();
            bucket.setCreatedTimestamp(System.currentTimeMillis());
            bucket.setDescription("Description");
            bucket.setIdentifier(requested.getSnapshotMetadata().getBucketIdentifier());
            bucket.setName(requested.getSnapshotMetadata().getBucketIdentifier());

            final FlowRegistryPermissions bucketPermissions = createAllowAllPermissions();
            bucket.setPermissions(bucketPermissions);
        } else {
            bucket = requested.getBucket();
        }
        full.setBucket(bucket);

        // Populate the flow
        final RegisteredFlow flow;
        if (requested.getFlow() == null) {
            flow = new RegisteredFlow();
            flow.setBucketIdentifier(requested.getSnapshotMetadata().getBucketIdentifier());
            flow.setBucketName(requested.getSnapshotMetadata().getBucketIdentifier());
            flow.setCreatedTimestamp(System.currentTimeMillis());
            flow.setDescription("Description");
            flow.setIdentifier(requested.getSnapshotMetadata().getFlowIdentifier());
            flow.setName(requested.getSnapshotMetadata().getFlowIdentifier());
            flow.setLastModifiedTimestamp(System.currentTimeMillis());
            flow.setPermissions(createAllowAllPermissions());

            final File[] flowVersionDirs = flowDir.listFiles();
            final int versionCount = flowVersionDirs == null ? 0 : flowVersionDirs.length;
            flow.setVersionCount(versionCount);

            final RegisteredFlowVersionInfo versionInfo = new RegisteredFlowVersionInfo();
            versionInfo.setVersion(versionCount);
            flow.setVersionInfo(versionInfo);
        } else {
            flow = requested.getFlow();
        }
        full.setFlow(flow);

        return full;
    }

    private FlowRegistryPermissions createAllowAllPermissions() {
        final FlowRegistryPermissions permissions = new FlowRegistryPermissions();
        permissions.setCanWrite(true);
        permissions.setCanRead(true);
        permissions.setCanDelete(true);
        return permissions;
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws IOException {
        final String bucketId = flowLocation.getBucketId();
        final String flowId = flowLocation.getFlowId();

        final File flowDir = getFlowDirectory(context, bucketId, flowId);
        final File[] versionDirs = flowDir.listFiles();
        if (versionDirs == null) {
            throw new IOException("Could not list directories of " + flowDir);
        }

        final Set<RegisteredFlowSnapshotMetadata> metadatas = new HashSet<>();
        for (final File versionDir : versionDirs) {
            final String versionName = versionDir.getName();

            final RegisteredFlowSnapshotMetadata metadata = new RegisteredFlowSnapshotMetadata();
            metadata.setVersion(versionName);
            metadata.setTimestamp(versionDir.lastModified());
            metadata.setFlowIdentifier(flowId);
            metadata.setBucketIdentifier(bucketId);
            metadata.setAuthor("System Test Author");
            metadatas.add(metadata);
        }

        return metadatas;
    }

    @Override
    public Optional<String> getLatestVersion(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws IOException {
        final String bucketId = flowLocation.getBucketId();
        final String flowId = flowLocation.getFlowId();
        final int latestVersion = getLatestFlowVersionInt(context, bucketId, flowId);
        return latestVersion == -1 ? Optional.empty() : Optional.of(String.valueOf(latestVersion));
    }

    private int getLatestFlowVersionInt(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws IOException {
        final File flowDir = getFlowDirectory(context, bucketId, flowId);
        final File[] versionDirs = flowDir.listFiles();
        if (versionDirs == null) {
            throw new IOException("Cannot list directories of " + flowDir);
        }

        final OptionalInt greatestValue = Arrays.stream(versionDirs)
                .map(File::getName)
                .mapToInt(Integer::parseInt)
                .max();
        return greatestValue.orElse(-1);
    }

    private File getSnapshotFile(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId, final String version) {
        final File flowDirectory = getFlowDirectory(context, bucketId, flowId);
        final File versionDirectory = getChildLocation(flowDirectory, Paths.get(String.valueOf(version)));
        return new File(versionDirectory, "snapshot.json");
    }

    private File getFlowDirectory(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) {
        final File rootDir = getRootDirectory(context);
        final File bucketDir = getChildLocation(rootDir, getValidatedBucketPath(bucketId));
        return getChildLocation(bucketDir, getFlowPath(flowId));
    }

    private File getChildLocation(final File parentDir, final Path childLocation) {
        final Path parentPath = parentDir.toPath().normalize();
        final Path childPath = parentPath.resolve(childLocation.normalize());
        if (childPath.startsWith(parentPath)) {
            return childPath.toFile();
        }
        throw new IllegalArgumentException(String.format("Child location not valid [%s]", childLocation));
    }

    private Path getFlowPath(final String flowId) {
        final Optional<String> flowIdFound = FLOW_IDS.stream().filter(id -> id.equals(flowId)).findFirst();
        if (flowIdFound.isPresent()) {
            return Paths.get(flowIdFound.get());
        }

        try {
            final UUID flowIdentifier = UUID.fromString(flowId);
            return Paths.get(flowIdentifier.toString());
        } catch (final RuntimeException e) {
            throw new IllegalArgumentException(String.format("Flow ID [%s] not validated", flowId));
        }
    }

    private Path getValidatedBucketPath(final String id) {
        if (TEST_FLOWS_BUCKET.equals(id)) {
            return Paths.get(TEST_FLOWS_BUCKET);
        }
        throw new IllegalArgumentException(String.format("Bucket [%s] not validated", id));
    }
}
