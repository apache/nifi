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
package org.apache.nifi.registry.provider.flow.git;

import org.apache.nifi.registry.flow.FlowPersistenceException;
import org.apache.nifi.registry.flow.FlowSnapshotContext;
import org.apache.nifi.registry.flow.MetadataAwareFlowPersistenceProvider;
import org.apache.nifi.registry.metadata.BucketMetadata;
import org.apache.nifi.registry.metadata.FlowMetadata;
import org.apache.nifi.registry.metadata.FlowSnapshotMetadata;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.apache.nifi.registry.provider.ProviderCreationException;
import org.apache.nifi.registry.util.FileUtils;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.registry.util.FileUtils.sanitizeFilename;

public class GitFlowPersistenceProvider implements MetadataAwareFlowPersistenceProvider {

    private static final Logger logger = LoggerFactory.getLogger(GitFlowMetaData.class);
    static final String FLOW_STORAGE_DIR_PROP = "Flow Storage Directory";
    private static final String REMOTE_TO_PUSH = "Remote To Push";
    private static final String REMOTE_ACCESS_USER = "Remote Access User";
    private static final String REMOTE_ACCESS_PASSWORD = "Remote Access Password";
    private static final String REMOTE_CLONE_REPOSITORY = "Remote Clone Repository";
    static final String SNAPSHOT_EXTENSION = ".snapshot";

    private File flowStorageDir;
    private GitFlowMetaData flowMetaData;

    @Override
    public void onConfigured(ProviderConfigurationContext configurationContext) throws ProviderCreationException {
        flowMetaData = new GitFlowMetaData();

        final Map<String,String> props = configurationContext.getProperties();
        if (!props.containsKey(FLOW_STORAGE_DIR_PROP)) {
            throw new ProviderCreationException("The property " + FLOW_STORAGE_DIR_PROP + " must be provided");
        }

        final String flowStorageDirValue = props.get(FLOW_STORAGE_DIR_PROP);
        if (isEmpty(flowStorageDirValue)) {
            throw new ProviderCreationException("The property " + FLOW_STORAGE_DIR_PROP + " cannot be null or blank");
        }

        flowMetaData.setRemoteToPush(props.get(REMOTE_TO_PUSH));

        final String remoteUser = props.get(REMOTE_ACCESS_USER);
        final String remotePassword = props.get(REMOTE_ACCESS_PASSWORD);
        final String remoteRepo = props.get(REMOTE_CLONE_REPOSITORY);
        if (!isEmpty(remoteRepo)) {
            if (isEmpty(remoteUser) || isEmpty(remotePassword)) {
                throw new ProviderCreationException(format("The property %s needs remote username and remote password",
                        REMOTE_CLONE_REPOSITORY));
            }
        }
        if (!isEmpty(remoteUser) && isEmpty(remotePassword)) {
            throw new ProviderCreationException(format("The property %s is specified but %s is not." +
                    " %s is required for username password authentication.",
                    REMOTE_ACCESS_USER, REMOTE_ACCESS_PASSWORD, REMOTE_ACCESS_PASSWORD));
        }
        if (!isEmpty(remotePassword)) {
            flowMetaData.setRemoteCredential(remoteUser, remotePassword);
        }

        try {
            flowStorageDir = new File(flowStorageDirValue);
            final boolean localRepoExists = flowMetaData.localRepoExists(flowStorageDir);
            if (remoteRepo != null && !remoteRepo.isEmpty() && !localRepoExists){
                flowMetaData.remoteRepoExists(remoteRepo);
                flowMetaData.cloneRepository(flowStorageDir, remoteRepo);
            }
            flowMetaData.loadGitRepository(flowStorageDir);
            flowMetaData.startPushThread();
            logger.info("Configured GitFlowPersistenceProvider with Flow Storage Directory {}",
                    new Object[] {flowStorageDir.getAbsolutePath()});
        } catch (IOException|GitAPIException e) {
            throw new ProviderCreationException("Failed to load a git repository " + flowStorageDir, e);
        }
    }

    @Override
    public void saveFlowContent(FlowSnapshotContext context, byte[] content) throws FlowPersistenceException {

        try {
            // Check if working dir is clean, any uncommitted file?
            if (!flowMetaData.isGitDirectoryClean()) {
                throw new FlowPersistenceException(format("Git directory %s is not clean" +
                                " or has uncommitted changes, resolve those changes first to save flow contents.",
                        flowStorageDir));
            }
        } catch (GitAPIException e) {
            throw new FlowPersistenceException(format("Failed to get Git status for directory %s due to %s",
                    flowStorageDir, e));
        }

        final String bucketId = context.getBucketId();
        final Bucket bucket = flowMetaData.getBucketOrCreate(bucketId);
        final String currentBucketDirName = bucket.getBucketDirName();
        final String bucketDirName = sanitizeFilename(context.getBucketName());
        final boolean isBucketNameChanged = !bucketDirName.equals(currentBucketDirName);
        bucket.setBucketDirName(bucketDirName);

        final Flow flow = bucket.getFlowOrCreate(context.getFlowId());
        final String flowSnapshotFilename = sanitizeFilename(context.getFlowName()) + SNAPSHOT_EXTENSION;

        final Optional<String> currentFlowSnapshotFilename = flow
                .getLatestVersion().map(flow::getFlowVersion).map(Flow.FlowPointer::getFileName);

        // Add new version.
        final Flow.FlowPointer flowPointer = new Flow.FlowPointer(flowSnapshotFilename);
        flowPointer.setFlowName(context.getFlowName());
        flowPointer.setFlowDescription(context.getFlowDescription());
        flowPointer.setAuthor(context.getAuthor());
        flowPointer.setComment(context.getComments());
        flowPointer.setCreated(context.getSnapshotTimestamp());

        flow.putVersion(context.getVersion(), flowPointer);

        final File bucketDir = new File(flowStorageDir, bucketDirName);
        final File flowSnippetFile = new File(bucketDir, flowSnapshotFilename);

        final File currentBucketDir = isEmpty(currentBucketDirName) ? null : new File(flowStorageDir, currentBucketDirName);
        if (currentBucketDir != null && currentBucketDir.isDirectory()) {
            if (isBucketNameChanged) {
                logger.debug("Detected bucket name change from {} to {}, moving it.", currentBucketDirName, bucketDirName);
                if (!currentBucketDir.renameTo(bucketDir)) {
                    throw new FlowPersistenceException(format("Failed to move existing bucket %s to %s.", currentBucketDir, bucketDir));
                }
            }
        } else {
            if (!bucketDir.mkdirs()) {
                throw new FlowPersistenceException(format("Failed to create new bucket dir %s.", bucketDir));
            }
        }


        try {
            if (currentFlowSnapshotFilename.isPresent() && !flowSnapshotFilename.equals(currentFlowSnapshotFilename.get())) {
                // Delete old file if flow name has been changed.
                final File latestFlowSnapshotFile = new File(bucketDir, currentFlowSnapshotFilename.get());
                logger.debug("Detected flow name change from {} to {}, deleting the old snapshot file.",
                        currentFlowSnapshotFilename.get(), flowSnapshotFilename);
                latestFlowSnapshotFile.delete();
            }

            // Save the content.
            try (final OutputStream os = new FileOutputStream(flowSnippetFile)) {
                os.write(content);
                os.flush();
            }

            // Write a bucket file.
            flowMetaData.saveBucket(bucket, bucketDir);

            // Create a Git Commit.
            flowMetaData.commit(context.getAuthor(), context.getComments(), bucket, flowPointer);

        } catch (IOException|GitAPIException e) {
            throw new FlowPersistenceException("Failed to persist flow.", e);
        }

        // TODO: What if user rebased commits? Version number to Commit ID mapping will be broken.
    }

    @Override
    public byte[] getFlowContent(String bucketId, String flowId, int version) throws FlowPersistenceException {

        final Bucket bucket = getBucketOrFail(bucketId);
        final Flow flow = getFlowOrFail(bucket, flowId);
        if (!flow.hasVersion(version)) {
            throw new FlowPersistenceException(format("Flow ID %s version %d was not found in bucket %s:%s.",
                    flowId, version, bucket.getBucketDirName(), bucketId));
        }

        final Flow.FlowPointer flowPointer = flow.getFlowVersion(version);
        try {
            return flowMetaData.getContent(flowPointer.getObjectId());
        } catch (IOException e) {
            throw new FlowPersistenceException(format("Failed to get content of Flow ID %s version %d in bucket %s:%s due to %s.",
                    flowId, version, bucket.getBucketDirName(), bucketId, e), e);
        }
    }

    // TODO: Need to add userId argument?
    @Override
    public void deleteAllFlowContent(String bucketId, String flowId) throws FlowPersistenceException {
        final Bucket bucket = getBucketOrFail(bucketId);
        final Optional<Flow> flowOpt = bucket.getFlow(flowId);
        if (!flowOpt.isPresent()) {
            logger.debug(format("Tried deleting all versions, but the Flow ID %s was not found in bucket %s:%s.",
                    flowId, bucket.getBucketDirName(), bucket.getBucketId()));
            return;
        }

        final Flow flow = flowOpt.get();
        final Optional<Integer> latestVersionOpt = flow.getLatestVersion();
        if (!latestVersionOpt.isPresent()) {
            throw new IllegalStateException("Flow version is not added yet, can not be deleted.");
        }

        final Integer latestVersion = latestVersionOpt.get();
        final Flow.FlowPointer flowPointer = flow.getFlowVersion(latestVersion);

        // Delete the flow snapshot.
        final File bucketDir = new File(flowStorageDir, bucket.getBucketDirName());
        final File flowSnapshotFile = new File(bucketDir, flowPointer.getFileName());
        if (flowSnapshotFile.exists()) {
            if (!flowSnapshotFile.delete()) {
                throw new FlowPersistenceException(format("Failed to delete flow content for %s:%s in bucket %s:%s",
                        flowPointer.getFileName(), flowId, bucket.getBucketDirName(), bucketId));
            }
        }

        bucket.removeFlow(flowId);

        try {

            if (bucket.isEmpty()) {
                // delete bucket dir if this is the last flow.
                FileUtils.deleteFile(bucketDir, true);
            } else {
                // Write a bucket file.
                flowMetaData.saveBucket(bucket, bucketDir);
            }

            // Create a Git Commit.
            final String commitMessage = format("Deleted flow %s:%s in bucket %s:%s.",
                    flowPointer.getFileName(), flowId, bucket.getBucketDirName(), bucketId);
            flowMetaData.commit(null, commitMessage, bucket, null);

        } catch (IOException|GitAPIException e) {
            throw new FlowPersistenceException(format("Failed to delete flow %s:%s in bucket %s:%s due to %s",
                    flowPointer.getFileName(), flowId, bucket.getBucketDirName(), bucketId, e), e);
        }

    }

    private Bucket getBucketOrFail(String bucketId) throws FlowPersistenceException {
        final Optional<Bucket> bucketOpt = flowMetaData.getBucket(bucketId);
        if (!bucketOpt.isPresent()) {
            throw new FlowPersistenceException(format("Bucket ID %s was not found.", bucketId));
        }

        return bucketOpt.get();
    }

    private Flow getFlowOrFail(Bucket bucket, String flowId) throws FlowPersistenceException {
        final Optional<Flow> flowOpt = bucket.getFlow(flowId);
        if (!flowOpt.isPresent()) {
            throw new FlowPersistenceException(format("Flow ID %s was not found in bucket %s:%s.",
                    flowId, bucket.getBucketDirName(), bucket.getBucketId()));
        }

        return flowOpt.get();
    }

    @Override
    public void deleteFlowContent(String bucketId, String flowId, int version) throws FlowPersistenceException {
        // TODO: Do nothing? This signature is not used. Actually there's nothing to do to the old versions as those exist in old commits even if this method is called.
    }

    @Override
    public List<BucketMetadata> getMetadata() {
        final Map<String, Bucket> gitBuckets = flowMetaData.getBuckets();
        if (gitBuckets == null || gitBuckets.isEmpty()) {
            return Collections.emptyList();
        }

        final List<BucketMetadata> bucketMetadataList = new ArrayList<>();
        for (Map.Entry<String,Bucket> bucketEntry : gitBuckets.entrySet()) {
            final String bucketId = bucketEntry.getKey();
            final Bucket gitBucket = bucketEntry.getValue();

            final BucketMetadata bucketMetadata = new BucketMetadata();
            bucketMetadata.setIdentifier(bucketId);
            bucketMetadata.setName(gitBucket.getBucketDirName());
            bucketMetadata.setFlowMetadata(createFlowMetadata(gitBucket));
            bucketMetadataList.add(bucketMetadata);
        }
        return bucketMetadataList;
    }

    private List<FlowMetadata> createFlowMetadata(final Bucket bucket) {
        if (bucket.isEmpty()) {
            return Collections.emptyList();
        }

        final List<FlowMetadata> flowMetadataList = new ArrayList<>();
        for (Map.Entry<String, Flow> flowEntry : bucket.getFlows().entrySet()) {
            final String flowId = flowEntry.getKey();
            final Flow flow = flowEntry.getValue();

            final Optional<Integer> latestVersion = flow.getLatestVersion();
            if (latestVersion.isPresent()) {
                final Flow.FlowPointer latestFlowPointer = flow.getFlowVersion(latestVersion.get());

                String flowName = latestFlowPointer.getFlowName();
                if (flowName == null) {
                    flowName = latestFlowPointer.getFileName();
                    if (flowName.endsWith(".snapshot")) {
                        flowName = flowName.substring(0, flowName.lastIndexOf("."));
                    }
                }


                final FlowMetadata flowMetadata = new FlowMetadata();
                flowMetadata.setIdentifier(flowId);
                flowMetadata.setName(flowName);
                flowMetadata.setDescription(latestFlowPointer.getFlowDescription());
                flowMetadata.setFlowSnapshotMetadata(createFlowSnapshotMetdata(flow));
                flowMetadataList.add(flowMetadata);
            }
        }
        return flowMetadataList;
    }

    private List<FlowSnapshotMetadata> createFlowSnapshotMetdata(final Flow flow) {
        final List<FlowSnapshotMetadata> flowSnapshotMetadataList = new ArrayList<>();

        final Map<Integer, Flow.FlowPointer> versions = flow.getVersions();
        for (Map.Entry<Integer, Flow.FlowPointer> entry : versions.entrySet()) {
            final Integer version = entry.getKey();
            final Flow.FlowPointer flowPointer = entry.getValue();

            final FlowSnapshotMetadata snapshotMetadata = new FlowSnapshotMetadata();
            snapshotMetadata.setVersion(version);
            snapshotMetadata.setAuthor(flowPointer.getAuthor());
            snapshotMetadata.setComments(flowPointer.getComment());
            snapshotMetadata.setCreated(flowPointer.getCreated());
            flowSnapshotMetadataList.add(snapshotMetadata);
        }

        return flowSnapshotMetadataList;
    }
}
