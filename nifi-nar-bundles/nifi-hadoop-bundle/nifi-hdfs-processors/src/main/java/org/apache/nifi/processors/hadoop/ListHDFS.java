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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.hadoop.util.HDFSListing;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;


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

    private volatile long latestTimestampListed = -1L;
    private volatile long latestTimestampEmitted = -1L;
    private volatile long lastRunTimestamp = -1L;

    static final String LISTING_TIMESTAMP_KEY = "listing.timestamp";
    static final String EMITTED_TIMESTAMP_KEY = "emitted.timestamp";

    static final long LISTING_LAG_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);
    }

    protected File getPersistenceFile() {
        return new File("conf/state/" + getIdentifier());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = new ArrayList<>(properties);
        props.add(DISTRIBUTED_CACHE_SERVICE);
        props.add(DIRECTORY);
        props.add(RECURSE_SUBDIRS);
        return props;
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

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        if (isConfigurationRestored() && descriptor.equals(DIRECTORY)) {
            latestTimestampEmitted = -1L;
            latestTimestampListed = -1L;
        }
    }

    private HDFSListing deserialize(final String serializedState) throws JsonParseException, JsonMappingException, IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode jsonNode = mapper.readTree(serializedState);
        return mapper.readValue(jsonNode, HDFSListing.class);
    }

    /**
     * Determines which of the given FileStatus's describes a File that should be listed.
     *
     * @param statuses the eligible FileStatus objects that we could potentially list
     * @return a Set containing only those FileStatus objects that we want to list
     */
    Set<FileStatus> determineListable(final Set<FileStatus> statuses) {
        final long minTimestamp = this.latestTimestampListed;
        final TreeMap<Long, List<FileStatus>> orderedEntries = new TreeMap<>();

        // Build a sorted map to determine the latest possible entries
        for (final FileStatus status : statuses) {
            if (status.getPath().getName().endsWith("_COPYING_")) {
                continue;
            }

            final long entityTimestamp = status.getModificationTime();

            if (entityTimestamp > latestTimestampListed) {
                latestTimestampListed = entityTimestamp;
            }

            // New entries are all those that occur at or after the associated timestamp
            final boolean newEntry = entityTimestamp >= minTimestamp && entityTimestamp > latestTimestampEmitted;

            if (newEntry) {
                List<FileStatus> entitiesForTimestamp = orderedEntries.get(status.getModificationTime());
                if (entitiesForTimestamp == null) {
                    entitiesForTimestamp = new ArrayList<FileStatus>();
                    orderedEntries.put(status.getModificationTime(), entitiesForTimestamp);
                }
                entitiesForTimestamp.add(status);
            }
        }

        final Set<FileStatus> toList = new HashSet<>();

        if (orderedEntries.size() > 0) {
            long latestListingTimestamp = orderedEntries.lastKey();

            // If the last listing time is equal to the newest entries previously seen,
            // another iteration has occurred without new files and special handling is needed to avoid starvation
            if (latestListingTimestamp == minTimestamp) {
                // We are done if the latest listing timestamp is equal to the last processed time,
                // meaning we handled those items originally passed over
                if (latestListingTimestamp == latestTimestampEmitted) {
                    return Collections.emptySet();
                }
            } else {
                // Otherwise, newest entries are held back one cycle to avoid issues in writes occurring exactly when the listing is being performed to avoid missing data
                orderedEntries.remove(latestListingTimestamp);
            }

            for (List<FileStatus> timestampEntities : orderedEntries.values()) {
                for (FileStatus status : timestampEntities) {
                    toList.add(status);
                }
            }
        }

        return toList;
    }



    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // We have to ensure that we don't continually perform listings, because if we perform two listings within
        // the same millisecond, our algorithm for comparing timestamps will not work. So we ensure here that we do
        // not let that happen.
        final long now = System.nanoTime();
        if (now - lastRunTimestamp < LISTING_LAG_NANOS) {
            lastRunTimestamp = now;
            context.yield();
            return;
        }
        lastRunTimestamp = now;

        final String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();

        // Ensure that we are using the latest listing information before we try to perform a listing of HDFS files.
        try {
            final StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
            if (stateMap.getVersion() == -1L) {
                latestTimestampEmitted = -1L;
                latestTimestampListed = -1L;
                getLogger().debug("Found no state stored");
            } else {
                // Determine if state is stored in the 'new' format or the 'old' format
                final String emittedString = stateMap.get(EMITTED_TIMESTAMP_KEY);
                if (emittedString == null) {
                    latestTimestampEmitted = -1L;
                    latestTimestampListed = -1L;
                    getLogger().debug("Found no recognized state keys; assuming no relevant state and resetting listing/emitted time to -1");
                } else {
                    // state is stored in the new format, using just two timestamps
                    latestTimestampEmitted = Long.parseLong(emittedString);
                    final String listingTimestmapString = stateMap.get(LISTING_TIMESTAMP_KEY);
                    if (listingTimestmapString != null) {
                        latestTimestampListed = Long.parseLong(listingTimestmapString);
                    }

                    getLogger().debug("Found new-style state stored, latesting timestamp emitted = {}, latest listed = {}",
                        new Object[] {latestTimestampEmitted, latestTimestampListed});
                }
            }
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve timestamp of last listing from Distributed Cache Service. Will not perform listing until this is accomplished.");
            context.yield();
            return;
        }

        // Pull in any file that is newer than the timestamp that we have.
        final FileSystem hdfs = getFileSystem();
        final boolean recursive = context.getProperty(RECURSE_SUBDIRS).asBoolean();

        final Set<FileStatus> statuses;
        try {
            final Path rootPath = new Path(directory);
            statuses = getStatuses(rootPath, recursive, hdfs);
            getLogger().debug("Found a total of {} files in HDFS", new Object[] {statuses.size()});
        } catch (final IOException | IllegalArgumentException e) {
            getLogger().error("Failed to perform listing of HDFS due to {}", new Object[] {e});
            return;
        }

        final Set<FileStatus> listable = determineListable(statuses);
        getLogger().debug("Of the {} files found in HDFS, {} are listable", new Object[] {statuses.size(), listable.size()});

        for (final FileStatus status : listable) {
            final Map<String, String> attributes = createAttributes(status);
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);

            final long fileModTime = status.getModificationTime();
            if (fileModTime > latestTimestampEmitted) {
                latestTimestampEmitted = fileModTime;
            }
        }

        final int listCount = listable.size();
        if ( listCount > 0 ) {
            getLogger().info("Successfully created listing with {} new files from HDFS", new Object[] {listCount});
            session.commit();
        } else {
            getLogger().debug("There is no data to list. Yielding.");
            context.yield();
        }

        final Map<String, String> updatedState = new HashMap<>(1);
        updatedState.put(LISTING_TIMESTAMP_KEY, String.valueOf(latestTimestampListed));
        updatedState.put(EMITTED_TIMESTAMP_KEY, String.valueOf(latestTimestampEmitted));
        getLogger().debug("New state map: {}", new Object[] {updatedState});

        try {
            context.getStateManager().setState(updatedState, Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().warn("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", ioe);
        }
    }

    private Set<FileStatus> getStatuses(final Path path, final boolean recursive, final FileSystem hdfs) throws IOException {
        final Set<FileStatus> statusSet = new HashSet<>();

        getLogger().debug("Fetching listing for {}", new Object[] {path});
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
        if (action.implies(FsAction.READ)) {
            sb.append("r");
        } else {
            sb.append("-");
        }

        if (action.implies(FsAction.WRITE)) {
            sb.append("w");
        } else {
            sb.append("-");
        }

        if (action.implies(FsAction.EXECUTE)) {
            sb.append("x");
        } else {
            sb.append("-");
        }

        return sb.toString();
    }
}
