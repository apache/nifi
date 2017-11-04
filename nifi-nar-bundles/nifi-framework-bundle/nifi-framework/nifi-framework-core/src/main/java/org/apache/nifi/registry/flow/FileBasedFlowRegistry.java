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

package org.apache.nifi.registry.flow;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A simple file-based implementation of a Flow Registry Client. Rather than interacting
 * with an actual Flow Registry, this implementation simply reads flows from disk and writes
 * them to disk. It is not meant for any production use but is available for testing purposes.
 */
public class FileBasedFlowRegistry implements FlowRegistry {
    private final File directory;
    private final Map<String, Set<String>> flowNamesByBucket = new HashMap<>();
    private final JsonFactory jsonFactory = new JsonFactory();
    private final String id;
    private volatile String name = "Local Registry";
    private volatile String url = "file:" + (new File("..").getAbsolutePath());
    private volatile String description = "Default file-based Flow Registry";

    public FileBasedFlowRegistry(final String id, final String url) throws IOException {
        final URI uri = URI.create(url);
        if (!uri.getScheme().equalsIgnoreCase("file")) {
            throw new IllegalArgumentException("Cannot create a File Based Flow Registry with a URL of " + url + "; URL scheme must be 'file'");
        }

        this.directory = new File(URI.create(url).getPath());

        if (!directory.exists() && !directory.mkdirs()) {
            throw new IOException("Could not access or create directory " + directory.getAbsolutePath() + " for Flow Registry");
        }

        this.id = id;
        this.url = url;
        recoverBuckets();
    }

    private void recoverBuckets() throws IOException {
        final File[] bucketDirs = directory.listFiles();
        if (bucketDirs == null) {
            throw new IOException("Could not get listing of directory " + directory);
        }

        for (final File bucketDir : bucketDirs) {
            final File[] flowDirs = bucketDir.listFiles();
            if (flowDirs == null) {
                throw new IOException("Could not get listing of directory " + bucketDir);
            }

            final Set<String> flowNames = new HashSet<>();
            for (final File flowDir : flowDirs) {
                final File propsFile = new File(flowDir, "flow.properties");
                if (!propsFile.exists()) {
                    continue;
                }

                final Properties properties = new Properties();
                try (final InputStream in = new FileInputStream(propsFile)) {
                    properties.load(in);
                }

                final String flowName = properties.getProperty("name");
                if (flowName == null) {
                    continue;
                }

                flowNames.add(flowName);
            }

            if (!flowNames.isEmpty()) {
                flowNamesByBucket.put(bucketDir.getName(), flowNames);
            }
        }
    }

    @Override
    public String getURL() {
        return url;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<Bucket> getBuckets(NiFiUser user) throws IOException {
        final Set<Bucket> buckets = new HashSet<>();

        final File[] bucketDirs = directory.listFiles();
        if (bucketDirs == null) {
            throw new IOException("Could not get listing of directory " + directory);
        }

        for (final File bucketDirectory : bucketDirs) {
            final String bucketIdentifier = bucketDirectory.getName();
            final long creation = bucketDirectory.lastModified();

            final Bucket bucket = new Bucket();
            bucket.setIdentifier(bucketIdentifier);
            bucket.setName("Bucket '" + bucketIdentifier + "'");
            bucket.setCreatedTimestamp(creation);

            final Set<VersionedFlow> versionedFlows = new HashSet<>();
            final File[] flowDirs = bucketDirectory.listFiles();
            if (flowDirs != null) {
                for (final File flowDir : flowDirs) {
                    final String flowIdentifier = flowDir.getName();
                    try {
                        final VersionedFlow versionedFlow = getVersionedFlow(bucketIdentifier, flowIdentifier);
                        versionedFlows.add(versionedFlow);
                    } catch (NiFiRegistryException e) {
                        continue;
                    }
                }
            }

            bucket.setVersionedFlows(versionedFlows);

            buckets.add(bucket);
        }

        return buckets;
    }

    @Override
    public Bucket getBucket(String bucketId) throws IOException, NiFiRegistryException {
        return getBucket(bucketId, null);
    }

    @Override
    public Bucket getBucket(String bucketId, NiFiUser user) throws IOException, NiFiRegistryException {
        return getBuckets(user).stream().filter(b -> b.getIdentifier().equals(bucketId)).findFirst().orElse(null);
    }

    @Override
    public Set<VersionedFlow> getFlows(final String bucketId, final NiFiUser user) throws IOException, NiFiRegistryException {
        final Bucket bucket = getBuckets(user).stream().filter(b -> bucketId.equals(b.getIdentifier())).findFirst().orElse(null);
        if (bucket == null) {
            return Collections.emptySet();
        }

        return bucket.getVersionedFlows();
    }

    @Override
    public Set<VersionedFlowSnapshotMetadata> getFlowVersions(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
        final VersionedFlow flow = getFlows(bucketId, user).stream().filter(f -> flowId.equals(f.getIdentifier())).findFirst().orElse(null);
        if (flow == null) {
            return Collections.emptySet();
        }

        return flow.getSnapshotMetadata();
    }

    @Override
    public synchronized VersionedFlow registerVersionedFlow(final VersionedFlow flow) throws IOException, NiFiRegistryException {
        Objects.requireNonNull(flow);
        Objects.requireNonNull(flow.getBucketIdentifier());
        Objects.requireNonNull(flow.getName());

        // Verify that bucket exists
        final File bucketDir = new File(directory, flow.getBucketIdentifier());
        if (!bucketDir.exists()) {
            throw new NiFiRegistryException("No bucket exists with ID " + flow.getBucketIdentifier());
        }

        // Verify that there is no flow with the same name in that bucket
        final Set<String> flowNames = flowNamesByBucket.get(flow.getBucketIdentifier());
        if (flowNames != null && flowNames.contains(flow.getName())) {
            throw new IllegalArgumentException("Flow with name '" + flow.getName() + "' already exists for Bucket with ID " + flow.getBucketIdentifier());
        }

        final String flowIdentifier = UUID.randomUUID().toString();
        final File flowDir = new File(bucketDir, flowIdentifier);
        if (!flowDir.mkdirs()) {
            throw new IOException("Failed to create directory " + flowDir + " for new Flow");
        }

        final File propertiesFile = new File(flowDir, "flow.properties");

        final Properties flowProperties = new Properties();
        flowProperties.setProperty("name", flow.getName());
        flowProperties.setProperty("created", String.valueOf(flow.getCreatedTimestamp()));
        flowProperties.setProperty("description", flow.getDescription());
        flowProperties.setProperty("lastModified", String.valueOf(flow.getModifiedTimestamp()));

        try (final OutputStream out = new FileOutputStream(propertiesFile)) {
            flowProperties.store(out, null);
        }

        final VersionedFlow response = new VersionedFlow();
        response.setBucketIdentifier(flow.getBucketIdentifier());
        response.setCreatedTimestamp(flow.getCreatedTimestamp());
        response.setDescription(flow.getDescription());
        response.setIdentifier(flowIdentifier);
        response.setModifiedTimestamp(flow.getModifiedTimestamp());
        response.setName(flow.getName());

        return response;
    }

    @Override
    public synchronized VersionedFlowSnapshot registerVersionedFlowSnapshot(final VersionedFlow flow, final VersionedProcessGroup snapshot, final String comments, final int expectedVersion)
        throws IOException, NiFiRegistryException {
        Objects.requireNonNull(flow);
        Objects.requireNonNull(flow.getBucketIdentifier());
        Objects.requireNonNull(flow.getName());
        Objects.requireNonNull(snapshot);

        // Verify that the bucket exists
        final File bucketDir = new File(directory, flow.getBucketIdentifier());
        if (!bucketDir.exists()) {
            throw new NiFiRegistryException("No bucket exists with ID " + flow.getBucketIdentifier());
        }

        // Verify that the flow exists
        final File flowDir = new File(bucketDir, flow.getIdentifier());
        if (!flowDir.exists()) {
            throw new NiFiRegistryException("No Flow with ID " + flow.getIdentifier() + " exists for Bucket with ID " + flow.getBucketIdentifier());
        }

        final File[] versionDirs = flowDir.listFiles();
        if (versionDirs == null) {
            throw new IOException("Unable to perform listing of directory " + flowDir);
        }

        int maxVersion = 0;
        for (final File versionDir : versionDirs) {
            final String versionName = versionDir.getName();

            final int version;
            try {
                version = Integer.parseInt(versionName);
            } catch (final NumberFormatException nfe) {
                continue;
            }

            if (version > maxVersion) {
                maxVersion = version;
            }
        }

        final int snapshotVersion = maxVersion + 1;
        final File snapshotDir = new File(flowDir, String.valueOf(snapshotVersion));
        if (!snapshotDir.mkdir()) {
            throw new IOException("Could not create directory " + snapshotDir);
        }

        final File contentsFile = new File(snapshotDir, "flow.xml");

        try (final OutputStream out = new FileOutputStream(contentsFile);
            final JsonGenerator generator = jsonFactory.createGenerator(out)) {
            generator.setCodec(new ObjectMapper());
            generator.setPrettyPrinter(new DefaultPrettyPrinter());
            generator.writeObject(snapshot);
        }

        final Properties snapshotProperties = new Properties();
        snapshotProperties.setProperty("comments", comments);
        snapshotProperties.setProperty("name", flow.getName());
        final File snapshotPropsFile = new File(snapshotDir, "snapshot.properties");
        try (final OutputStream out = new FileOutputStream(snapshotPropsFile)) {
            snapshotProperties.store(out, null);
        }

        final VersionedFlowSnapshotMetadata snapshotMetadata = new VersionedFlowSnapshotMetadata();
        snapshotMetadata.setBucketIdentifier(flow.getBucketIdentifier());
        snapshotMetadata.setComments(comments);
        snapshotMetadata.setFlowIdentifier(flow.getIdentifier());
        snapshotMetadata.setFlowName(flow.getName());
        snapshotMetadata.setTimestamp(System.currentTimeMillis());
        snapshotMetadata.setVersion(snapshotVersion);

        final VersionedFlowSnapshot response = new VersionedFlowSnapshot();
        response.setSnapshotMetadata(snapshotMetadata);
        response.setFlowContents(snapshot);
        return response;
    }

    @Override
    public int getLatestVersion(final String bucketId, final String flowId) throws IOException, NiFiRegistryException {
        // Verify that the bucket exists
        final File bucketDir = new File(directory, bucketId);
        if (!bucketDir.exists()) {
            throw new NiFiRegistryException("No bucket exists with ID " + bucketId);
        }

        // Verify that the flow exists
        final File flowDir = new File(bucketDir, flowId);
        if (!flowDir.exists()) {
            throw new NiFiRegistryException("No Flow with ID " + flowId + " exists for Bucket with ID " + bucketId);
        }

        final File[] versionDirs = flowDir.listFiles();
        if (versionDirs == null) {
            throw new IOException("Unable to perform listing of directory " + flowDir);
        }

        int maxVersion = 0;
        for (final File versionDir : versionDirs) {
            final String versionName = versionDir.getName();

            final int version;
            try {
                version = Integer.parseInt(versionName);
            } catch (final NumberFormatException nfe) {
                continue;
            }

            if (version > maxVersion) {
                maxVersion = version;
            }
        }

        return maxVersion;
    }

    @Override
    public VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, int version) throws IOException, NiFiRegistryException {
        // Verify that the bucket exists
        final File bucketDir = new File(directory, bucketId);
        if (!bucketDir.exists()) {
            throw new NiFiRegistryException("No bucket exists with ID " + bucketId);
        }

        // Verify that the flow exists
        final File flowDir = new File(bucketDir, flowId);
        if (!flowDir.exists()) {
            throw new NiFiRegistryException("No Flow with ID " + flowId + " exists for Bucket with ID " + flowId);
        }

        final File versionDir = new File(flowDir, String.valueOf(version));
        if (!versionDir.exists()) {
            throw new NiFiRegistryException("Flow with ID " + flowId + " in Bucket with ID " + bucketId + " does not contain a snapshot with version " + version);
        }

        final File contentsFile = new File(versionDir, "flow.xml");

        final VersionedProcessGroup processGroup;
        try (final JsonParser parser = jsonFactory.createParser(contentsFile)) {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            parser.setCodec(mapper);
            processGroup = parser.readValueAs(VersionedProcessGroup.class);
        }

        final Properties properties = new Properties();
        final File snapshotPropsFile = new File(versionDir, "snapshot.properties");
        try (final InputStream in = new FileInputStream(snapshotPropsFile)) {
            properties.load(in);
        }

        final String comments = properties.getProperty("comments");
        final String flowName = properties.getProperty("name");

        final VersionedFlowSnapshotMetadata snapshotMetadata = new VersionedFlowSnapshotMetadata();
        snapshotMetadata.setBucketIdentifier(bucketId);
        snapshotMetadata.setComments(comments);
        snapshotMetadata.setFlowIdentifier(flowId);
        snapshotMetadata.setFlowName(flowName);
        snapshotMetadata.setTimestamp(System.currentTimeMillis());
        snapshotMetadata.setVersion(version);

        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
        snapshot.setFlowContents(processGroup);
        snapshot.setSnapshotMetadata(snapshotMetadata);

        return snapshot;
    }

    @Override
    public VersionedFlow getVersionedFlow(final String bucketId, final String flowId) throws IOException, NiFiRegistryException {
        // Verify that the bucket exists
        final File bucketDir = new File(directory, bucketId);
        if (!bucketDir.exists()) {
            throw new NiFiRegistryException("No bucket exists with ID " + bucketId);
        }

        // Verify that the flow exists
        final File flowDir = new File(bucketDir, flowId);
        if (!flowDir.exists()) {
            throw new NiFiRegistryException("No Flow with ID " + flowId + " exists for Bucket with ID " + flowId);
        }

        final File flowPropsFile = new File(flowDir, "flow.properties");
        final Properties flowProperties = new Properties();
        try (final InputStream in = new FileInputStream(flowPropsFile)) {
            flowProperties.load(in);
        }

        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setCreatedTimestamp(Long.parseLong(flowProperties.getProperty("created")));
        flow.setDescription(flowProperties.getProperty("description"));
        flow.setIdentifier(flowId);
        flow.setModifiedTimestamp(flowDir.lastModified());
        flow.setName(flowProperties.getProperty("name"));

        final Comparator<VersionedFlowSnapshotMetadata> versionComparator = (a, b) -> Integer.compare(a.getVersion(), b.getVersion());

        final SortedSet<VersionedFlowSnapshotMetadata> snapshotMetadataSet = new TreeSet<>(versionComparator);
        flow.setSnapshotMetadata(snapshotMetadataSet);

        final File[] versionDirs = flowDir.listFiles();
        flow.setVersionCount(versionDirs.length);

        for (final File file : versionDirs) {
            if (!file.isDirectory()) {
                continue;
            }

            int version;
            try {
                version = Integer.parseInt(file.getName());
            } catch (final NumberFormatException nfe) {
                // not a version. skip.
                continue;
            }

            final File snapshotPropsFile = new File(file, "snapshot.properties");
            final Properties snapshotProperties = new Properties();
            try (final InputStream in = new FileInputStream(snapshotPropsFile)) {
                snapshotProperties.load(in);
            }

            final VersionedFlowSnapshotMetadata metadata = new VersionedFlowSnapshotMetadata();
            metadata.setBucketIdentifier(bucketId);
            metadata.setComments(snapshotProperties.getProperty("comments"));
            metadata.setFlowIdentifier(flowId);
            metadata.setFlowName(snapshotProperties.getProperty("name"));
            metadata.setTimestamp(file.lastModified());
            metadata.setVersion(version);

            snapshotMetadataSet.add(metadata);
        }

        return flow;
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public void setURL(String url) {
        this.url = url;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }
}
