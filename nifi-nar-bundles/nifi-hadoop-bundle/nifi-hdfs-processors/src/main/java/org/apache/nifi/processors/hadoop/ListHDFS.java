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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
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
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of HDFS files, the timestamp of the newest file is stored, "
    + "along with the filenames of all files that share that same timestamp. This allows the Processor to list only files that have been added or modified after "
    + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
    + "Node is selected, the new node can pick up where the previous node left off, without duplicating the data.")
@SeeAlso({GetHDFS.class, FetchHDFS.class, PutHDFS.class})
public class ListHDFS extends AbstractHadoopProcessor {
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
        .name("Distributed Cache Service")
        .description("Specifies the Controller Service that should be used to maintain state about what has been pulled from HDFS so that if a new node "
                + "begins pulling data, it won't duplicate all of the work that has been done.")
        .required(false)
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
        properties.add(KERBEROS_PRINCIPAL);
        properties.add(KERBEROS_KEYTAB);
        properties.add(KERBEROS_RELOGIN_PERIOD);
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

    @OnScheduled
    public void moveStateToStateManager(final ProcessContext context) throws IOException {
        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap = stateManager.getState(Scope.CLUSTER);

        // Check if we have already stored state in the cluster state manager.
        if (stateMap.getVersion() == -1L) {
            final HDFSListing serviceListing = getListingFromService(context);
            if (serviceListing != null) {
                context.getStateManager().setState(serviceListing.toMap(), Scope.CLUSTER);
            }
        }
    }

    private HDFSListing getListingFromService(final ProcessContext context) throws IOException {
        final DistributedMapCacheClient client = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
        if (client == null) {
            return null;
        }

        final String directory = context.getProperty(DIRECTORY).getValue();
        final String remoteValue = client.get(getKey(directory), new StringSerDe(), new StringSerDe());
        if (remoteValue == null) {
            return null;
        }

        try {
            return deserialize(remoteValue);
        } catch (final Exception e) {
            getLogger().error("Failed to retrieve state from Distributed Map Cache because the content that was retrieved could not be understood", e);
            return null;
        }
    }


    private Long getMinTimestamp(final String directory, final HDFSListing remoteListing) throws IOException {
        // No cluster-wide state has been recovered. Just use whatever values we already have.
        if (remoteListing == null) {
            return lastListingTime;
        }

        // If our local timestamp is already later than the remote listing's timestamp, use our local info.
        Long minTimestamp = lastListingTime;
        if (minTimestamp != null && minTimestamp > remoteListing.getLatestTimestamp().getTime()) {
            return minTimestamp;
        }

        // Use the remote listing's information.
        if (minTimestamp == null || electedPrimaryNode) {
            this.latestPathsListed = remoteListing.toPaths();
            this.lastListingTime = remoteListing.getLatestTimestamp().getTime();
        }

        return minTimestamp;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String directory = context.getProperty(DIRECTORY).getValue();

        // Ensure that we are using the latest listing information before we try to perform a listing of HDFS files.
        final Long minTimestamp;
        try {
            final HDFSListing stateListing;
            final StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
            if (stateMap.getVersion() == -1L) {
                stateListing = null;
            } else {
                final Map<String, String> stateValues = stateMap.toMap();
                stateListing = HDFSListing.fromMap(stateValues);
            }

            minTimestamp = getMinTimestamp(directory, stateListing);
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
            final HDFSListing latestListing = createListing(latestListingModTime, statuses);

            try {
                context.getStateManager().setState(latestListing.toMap(), Scope.CLUSTER);
            } catch (final IOException ioe) {
                getLogger().warn("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", ioe);
            }

            lastListingTime = latestListingModTime;
            latestPathsListed.clear();
            for (final FileStatus status : statuses) {
                latestPathsListed.add(status.getPath());
            }
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

    private HDFSListing createListing(final long latestListingModTime, final Set<FileStatus> statuses) {
        final Set<String> paths = new HashSet<>();
        for (final FileStatus status : statuses) {
            final String path = status.getPath().toUri().toString();
            paths.add(path);
        }

        final HDFSListing listing = new HDFSListing();
        listing.setLatestTimestamp(new Date(latestListingModTime));
        listing.setMatchingPaths(paths);

        return listing;
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
