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
package org.apache.nifi.processors.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.util.HDFSListing;
import org.apache.nifi.processors.hadoop.util.StringSerDe;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"hadoop", "HDFS", "get", "list", "ingest", "source", "filesystem"})
@CapabilityDescription("Retrieves a listing of files from HDFS. For each file that is listed in HDFS, creates a FlowFile that represents "
        + "the HDFS file so that it can be fetched in conjunction with ListHDFS. This Processor is designed to run on Primary Node only "
        + "in a cluster. If the primary node changes, the new Primary Node will pick up where the previous node left off without duplicating "
        + "all of the data. Unlike GetHDFS, this Processor does not delete any data from HDFS.")
@WritesAttributes({
    @WritesAttribute(attribute="filename", description="The name of the file that was read from HDFS."),
    @WritesAttribute(attribute="path", description="The path is set to the absolute path of the file's directory on HDFS. For example, if the Directory property is set to /tmp, "
            + "then files picked up from /tmp will have the path attribute set to \"./\". If the Recurse Subdirectories property is set to true and a file is picked up "
            + "from /tmp/abc/1/2/3, then the path attribute will be set to \"/tmp/abc/1/2/3\"."),
    @WritesAttribute(attribute="hdfs.owner", description="The user that owns the file in HDFS"),
    @WritesAttribute(attribute="hdfs.group", description="The group that owns the file in HDFS"),
    @WritesAttribute(attribute="hdfs.lastModified", description="The timestamp of when the file in HDFS was last modified, as milliseconds since midnight Jan 1, 1970 UTC"),
    @WritesAttribute(attribute="hdfs.length", description="The number of bytes in the file in HDFS"),
    @WritesAttribute(attribute="hdfs.replication", description="The number of HDFS replicas for hte file"),
    @WritesAttribute(attribute="hdfs.permissions", description="The permissions for the file in HDFS. This is formatted as 3 characters for the owner, "
            + "3 for the group, and 3 for other users. For example rw-rw-r--")
})
@SeeAlso({GetHDFS.class, FetchHDFS.class, PutHDFS.class})
public class ListHDFS extends AbstractHadoopProcessor {
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
        .name("Distributed Cache Service")
        .description("Specifies the Controller Service that should be used to maintain state about what has been pulled from HDFS so that if a new node "
                + "begins pulling data, it won't duplicate all of the work that has been done.")
        .required(true)
        .identifiesControllerService(DistributedMapCacheClient.class)
        .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
        .name(DIRECTORY_PROP_NAME)
        .description("The HDFS directory from which files should be read")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor RECURSE_SUBDIRS = new PropertyDescriptor.Builder()
        .name("Recurse Subdirectories")
        .description("Indicates whether to list files from subdirectories of the HDFS directory")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles are transferred to this relationship")
        .build();

    private volatile Long lastListingTime = null;
    private volatile Set<Path> latestPathsListed = new HashSet<>();
    private volatile boolean electedPrimaryNode = false;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);
    }

    protected File getPersistenceFile() {
        return new File("conf/state/" + getIdentifier());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HADOOP_CONFIGURATION_RESOURCES);
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        properties.add(DIRECTORY);
        properties.add(RECURSE_SUBDIRS);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    protected String getKey(final String directory) {
        return getIdentifier() + ".lastListingTime." + directory;
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange(final PrimaryNodeState newState) {
        if ( newState == PrimaryNodeState.ELECTED_PRIMARY_NODE ) {
            electedPrimaryNode = true;
        }
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if ( descriptor.equals(DIRECTORY) ) {
            lastListingTime = null; // clear lastListingTime so that we have to fetch new time
            latestPathsListed = new HashSet<>();
        }
    }

    private HDFSListing deserialize(final String serializedState) throws JsonParseException, JsonMappingException, IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode jsonNode = mapper.readTree(serializedState);
        return mapper.readValue(jsonNode, HDFSListing.class);
    }


    private Long getMinTimestamp(final String directory, final DistributedMapCacheClient client) throws IOException {
        // Determine the timestamp for the last file that we've listed.
        Long minTimestamp = lastListingTime;
        if ( minTimestamp == null || electedPrimaryNode ) {
            // We haven't yet restored any state from local or distributed state - or it's been at least a minute since
            // we have performed a listing. In this case,
            // First, attempt to get timestamp from distributed cache service.
            try {
                final StringSerDe serde = new StringSerDe();
                final String serializedState = client.get(getKey(directory), serde, serde);
                if ( serializedState == null || serializedState.isEmpty() ) {
                    minTimestamp = null;
                    this.latestPathsListed = Collections.emptySet();
                } else {
                    final HDFSListing listing = deserialize(serializedState);
                    this.lastListingTime = listing.getLatestTimestamp().getTime();
                    minTimestamp = listing.getLatestTimestamp().getTime();
                    this.latestPathsListed = listing.toPaths();
                }

                this.lastListingTime = minTimestamp;
                electedPrimaryNode = false; // no requirement to pull an update from the distributed cache anymore.
            } catch (final IOException ioe) {
                throw ioe;
            }

            // Check the persistence file. We want to use the latest timestamp that we have so that
            // we don't duplicate data.
            try {
                final File persistenceFile = getPersistenceFile();
                if ( persistenceFile.exists() ) {
                    try (final FileInputStream fis = new FileInputStream(persistenceFile)) {
                        final Properties props = new Properties();
                        props.load(fis);

                        // get the local timestamp for this directory, if it exists.
                        final String locallyPersistedValue = props.getProperty(directory);
                        if ( locallyPersistedValue != null ) {
                            final HDFSListing listing = deserialize(locallyPersistedValue);
                            final long localTimestamp = listing.getLatestTimestamp().getTime();

                            // If distributed state doesn't have an entry or the local entry is later than the distributed state,
                            // update the distributed state so that we are in sync.
                            if (minTimestamp == null || localTimestamp > minTimestamp) {
                                minTimestamp = localTimestamp;

                                // Our local persistence file shows a later time than the Distributed service.
                                // Update the distributed service to match our local state.
                                try {
                                    final StringSerDe serde = new StringSerDe();
                                    client.put(getKey(directory), locallyPersistedValue, serde, serde);
                                } catch (final IOException ioe) {
                                    getLogger().warn("Local timestamp for {} is {}, which is later than Distributed state but failed to update Distributed "
                                            + "state due to {}. If a new node performs HDFS Listing, data duplication may occur",
                                            new Object[] {directory, locallyPersistedValue, ioe});
                                }
                            }
                        }
                    }
                }
            } catch (final IOException ioe) {
                getLogger().warn("Failed to recover local state due to {}. Assuming that the state from the distributed cache is correct.", ioe);
            }
        }

        return minTimestamp;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String directory = context.getProperty(DIRECTORY).getValue();
        final DistributedMapCacheClient client = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        final Long minTimestamp;
        try {
            minTimestamp = getMinTimestamp(directory, client);
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve timestamp of last listing from Distributed Cache Service. Will not perform listing until this is accomplished.");
            context.yield();
            return;
        }

        // Pull in any file that is newer than the timestamp that we have.
        final FileSystem hdfs = getFileSystem();
        final boolean recursive = context.getProperty(RECURSE_SUBDIRS).asBoolean();
        final Path rootPath = new Path(directory);

        int listCount = 0;
        Long latestListingModTime = null;
        final Set<FileStatus> statuses;
        try {
            statuses = getStatuses(rootPath, recursive, hdfs);
            for ( final FileStatus status : statuses ) {
                // don't get anything where the last modified timestamp is equal to our current timestamp.
                // if we do, then we run the risk of multiple files having the same last mod date but us only
                // seeing a portion of them.
                // I.e., there could be 5 files with last mod date = (now). But if we do the listing now, maybe
                // only 2 exist and 3 more will exist later in this millisecond. So we ignore anything with a
                // modified date not before the current time.
                final long fileModTime = status.getModificationTime();

                // we only want the file if its timestamp is later than the minTimestamp or equal to and we didn't pull it last time.
                // Also, HDFS creates files with the suffix _COPYING_ when they are being written - we want to ignore those.
                boolean fetch = !status.getPath().getName().endsWith("_COPYING_")
                        && (minTimestamp == null || fileModTime > minTimestamp || (fileModTime == minTimestamp && !latestPathsListed.contains(status.getPath())));

                // Create the FlowFile for this path.
                if ( fetch ) {
                    final Map<String, String> attributes = createAttributes(status);
                    FlowFile flowFile = session.create();
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    session.transfer(flowFile, REL_SUCCESS);
                    listCount++;

                    if ( latestListingModTime == null || fileModTime > latestListingModTime ) {
                        latestListingModTime = fileModTime;
                    }
                }
            }
        } catch (final IOException ioe) {
            getLogger().error("Failed to perform listing of HDFS due to {}", new Object[] {ioe});
            return;
        }

        if ( listCount > 0 ) {
            getLogger().info("Successfully created listing with {} new files from HDFS", new Object[] {listCount});
            session.commit();

            // We have performed a listing and pushed the FlowFiles out.
            // Now, we need to persist state about the Last Modified timestamp of the newest file
            // that we pulled in. We do this in order to avoid pulling in the same file twice.
            // However, we want to save the state both locally and remotely.
            // We store the state remotely so that if a new Primary Node is chosen, it can pick up where the
            // previously Primary Node left off.
            // We also store the state locally so that if the node is restarted, and the node cannot contact
            // the distributed state cache, the node can continue to run (if it is primary node).
            String serializedState = null;
            try {
                serializedState = serializeState(latestListingModTime, statuses);
            } catch (final Exception e) {
                getLogger().error("Failed to serialize state due to {}", new Object[] {e});
            }

            if ( serializedState != null ) {
                // Save our state locally.
                try {
                    persistLocalState(directory, serializedState);
                } catch (final IOException ioe) {
                    getLogger().warn("Unable to save state locally. If the node is restarted now, data may be duplicated. Failure is due to {}", ioe);
                }

                // Attempt to save state to remote server.
                try {
                    client.put(getKey(directory), serializedState, new StringSerDe(), new StringSerDe());
                } catch (final IOException ioe) {
                    getLogger().warn("Unable to communicate with distributed cache server due to {}. Persisting state locally instead.", ioe);
                }
            }

            lastListingTime = latestListingModTime;
        } else {
            getLogger().debug("There is no data to list. Yielding.");
            context.yield();

            // lastListingTime = 0 so that we don't continually poll the distributed cache / local file system
            if ( lastListingTime == null ) {
                lastListingTime = 0L;
            }

            return;
        }
    }

    private Set<FileStatus> getStatuses(final Path path, final boolean recursive, final FileSystem hdfs) throws IOException {
        final Set<FileStatus> statusSet = new HashSet<>();

        final FileStatus[] statuses = hdfs.listStatus(path);

        for ( final FileStatus status : statuses ) {
            if ( status.isDirectory() ) {
                if ( recursive ) {
                    try {
                        statusSet.addAll(getStatuses(status.getPath(), recursive, hdfs));
                    } catch (final IOException ioe) {
                        getLogger().error("Failed to retrieve HDFS listing for subdirectory {} due to {}; will continue listing others", new Object[] {status.getPath(), ioe});
                    }
                }
            } else {
                statusSet.add(status);
            }
        }

        return statusSet;
    }


    private String serializeState(final long latestListingTime, final Set<FileStatus> statuses) throws JsonGenerationException, JsonMappingException, IOException {
        // we need to keep track of all files that we pulled in that had a modification time equal to
        // lastListingTime so that we can avoid pulling those files in again. We can't just ignore any files
        // that have a mod time equal to that timestamp because more files may come in with the same timestamp
        // later in the same millisecond.
        if ( statuses.isEmpty() ) {
            return null;
        } else {
            final List<FileStatus> sortedStatuses = new ArrayList<>(statuses);
            Collections.sort(sortedStatuses, new Comparator<FileStatus>() {
                @Override
                public int compare(final FileStatus o1, final FileStatus o2) {
                    return Long.compare(o1.getModificationTime(), o2.getModificationTime());
                }
            });

            final long latestListingModTime = sortedStatuses.get(sortedStatuses.size() - 1).getModificationTime();
            final Set<Path> pathsWithModTimeEqualToListingModTime = new HashSet<>();
            for (int i=sortedStatuses.size() - 1; i >= 0; i--) {
                final FileStatus status = sortedStatuses.get(i);
                if (status.getModificationTime() == latestListingModTime) {
                    pathsWithModTimeEqualToListingModTime.add(status.getPath());
                }
            }

            this.latestPathsListed = pathsWithModTimeEqualToListingModTime;

            final HDFSListing listing = new HDFSListing();
            listing.setLatestTimestamp(new Date(latestListingModTime));
            final Set<String> paths = new HashSet<>();
            for ( final Path path : pathsWithModTimeEqualToListingModTime ) {
                paths.add(path.toUri().toString());
            }
            listing.setMatchingPaths(paths);

            final ObjectMapper mapper = new ObjectMapper();
            final String serializedState = mapper.writerWithType(HDFSListing.class).writeValueAsString(listing);
            return serializedState;
        }
    }

    protected void persistLocalState(final String directory, final String serializedState) throws IOException {
        // we need to keep track of all files that we pulled in that had a modification time equal to
        // lastListingTime so that we can avoid pulling those files in again. We can't just ignore any files
        // that have a mod time equal to that timestamp because more files may come in with the same timestamp
        // later in the same millisecond.
        final File persistenceFile = getPersistenceFile();
        final File dir = persistenceFile.getParentFile();
        if ( !dir.exists() && !dir.mkdirs() ) {
            throw new IOException("Could not create directory " + dir.getAbsolutePath() + " in order to save local state");
        }

        final Properties props = new Properties();
        if ( persistenceFile.exists() ) {
            try (final FileInputStream fis = new FileInputStream(persistenceFile)) {
                props.load(fis);
            }
        }

        props.setProperty(directory, serializedState);

        try (final FileOutputStream fos = new FileOutputStream(persistenceFile)) {
            props.store(fos, null);
        }
    }

    private String getAbsolutePath(final Path path) {
        final Path parent = path.getParent();
        final String prefix = (parent == null || parent.getName().equals("")) ? "" : getAbsolutePath(parent);
        return prefix + "/" + path.getName();
    }

    private Map<String, String> createAttributes(final FileStatus status) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), status.getPath().getName());
        attributes.put(CoreAttributes.PATH.key(), getAbsolutePath(status.getPath().getParent()));

        attributes.put("hdfs.owner", status.getOwner());
        attributes.put("hdfs.group", status.getGroup());
        attributes.put("hdfs.lastModified", String.valueOf(status.getModificationTime()));
        attributes.put("hdfs.length", String.valueOf(status.getLen()));
        attributes.put("hdfs.replication", String.valueOf(status.getReplication()));

        final FsPermission permission = status.getPermission();
        final String perms = getPerms(permission.getUserAction()) + getPerms(permission.getGroupAction()) + getPerms(permission.getOtherAction());
        attributes.put("hdfs.permissions", perms);
        return attributes;
    }

    private String getPerms(final FsAction action) {
        final StringBuilder sb = new StringBuilder();
        if ( action.implies(FsAction.READ) ) {
            sb.append("r");
        } else {
            sb.append("-");
        }

        if ( action.implies(FsAction.WRITE) ) {
            sb.append("w");
        } else {
            sb.append("-");
        }

        if ( action.implies(FsAction.EXECUTE) ) {
            sb.append("x");
        } else {
            sb.append("-");
        }

        return sb.toString();
    }
}
