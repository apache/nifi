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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"hadoop", "HCFS", "HDFS", "get", "list", "ingest", "source", "filesystem"})
@CapabilityDescription("Retrieves a listing of files from HDFS. Each time a listing is performed, the files with the latest timestamp will be excluded "
        + "and picked up during the next execution of the processor. This is done to ensure that we do not miss any files, or produce duplicates, in the "
        + "cases where files with the same timestamp are written immediately before and after a single execution of the processor. For each file that is "
        + "listed in HDFS, this processor creates a FlowFile that represents the HDFS file to be fetched in conjunction with FetchHDFS. This Processor is "
        +  "designed to run on Primary Node only in a cluster. If the primary node changes, the new Primary Node will pick up where the previous node left "
        +  "off without duplicating all of the data. Unlike GetHDFS, this Processor does not delete any data from HDFS.")
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
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of HDFS files, the latest timestamp of all the files listed and the latest "
        + "timestamp of all the files transferred are both stored. This allows the Processor to list only files that have been added or modified after "
        + "this date the next time that the Processor is run, without having to store all of the actual filenames/paths which could lead to performance "
        + "problems. State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
        + "Node is selected, the new node can pick up where the previous node left off, without duplicating the data.")
@SeeAlso({GetHDFS.class, FetchHDFS.class, PutHDFS.class})
public class ListHDFS extends AbstractHadoopProcessor {

    private static final RecordSchema RECORD_SCHEMA;
    private static final String FILENAME = "filename";
    private static final String PATH = "path";
    private static final String IS_DIRECTORY = "directory";
    private static final String SIZE = "size";
    private static final String LAST_MODIFIED = "lastModified";
    private static final String PERMISSIONS = "permissions";
    private static final String OWNER = "owner";
    private static final String GROUP = "group";
    private static final String REPLICATION = "replication";
    private static final String IS_SYM_LINK = "symLink";
    private static final String IS_ENCRYPTED = "encrypted";
    private static final String IS_ERASURE_CODED = "erasureCoded";

    static {
        final List<RecordField> recordFields = new ArrayList<>();
        recordFields.add(new RecordField(FILENAME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(PATH, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(IS_DIRECTORY, RecordFieldType.BOOLEAN.getDataType(), false));
        recordFields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(LAST_MODIFIED, RecordFieldType.TIMESTAMP.getDataType(), false));
        recordFields.add(new RecordField(PERMISSIONS, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(OWNER, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(GROUP, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(REPLICATION, RecordFieldType.INT.getDataType()));
        recordFields.add(new RecordField(IS_SYM_LINK, RecordFieldType.BOOLEAN.getDataType()));
        recordFields.add(new RecordField(IS_ENCRYPTED, RecordFieldType.BOOLEAN.getDataType()));
        recordFields.add(new RecordField(IS_ERASURE_CODED, RecordFieldType.BOOLEAN.getDataType()));
        RECORD_SCHEMA = new SimpleRecordSchema(recordFields);
    }

    @Deprecated
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
        .name("Distributed Cache Service")
        .description("This property is ignored.  State will be stored in the " + Scope.LOCAL + " or " + Scope.CLUSTER + " scope by the State Manager based on NiFi's configuration.")
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

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Record Writer to use for creating the listing. If not specified, one FlowFile will be created for each entity that is listed. If the Record Writer is specified, " +
            "all entities will be written to a single FlowFile.")
        .required(false)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
        .name("File Filter")
        .description("Only files whose names match the given regular expression will be picked up")
        .required(true)
        .defaultValue("[^\\.].*")
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .build();

    private static final String FILTER_MODE_DIRECTORIES_AND_FILES = "filter-mode-directories-and-files";
    private static final String FILTER_MODE_FILES_ONLY = "filter-mode-files-only";
    private static final String FILTER_MODE_FULL_PATH = "filter-mode-full-path";
    static final AllowableValue FILTER_DIRECTORIES_AND_FILES_VALUE = new AllowableValue(FILTER_MODE_DIRECTORIES_AND_FILES,
        "Directories and Files",
        "Filtering will be applied to the names of directories and files.  If " + RECURSE_SUBDIRS.getDisplayName()
                + " is set to true, only subdirectories with a matching name will be searched for files that match "
                + "the regular expression defined in " + FILE_FILTER.getDisplayName() + ".");
    static final AllowableValue FILTER_FILES_ONLY_VALUE = new AllowableValue(FILTER_MODE_FILES_ONLY,
        "Files Only",
        "Filtering will only be applied to the names of files.  If " + RECURSE_SUBDIRS.getDisplayName()
                + " is set to true, the entire subdirectory tree will be searched for files that match "
                + "the regular expression defined in " + FILE_FILTER.getDisplayName() + ".");
    static final AllowableValue FILTER_FULL_PATH_VALUE = new AllowableValue(FILTER_MODE_FULL_PATH,
        "Full Path",
        "Filtering will be applied by evaluating the regular expression defined in " + FILE_FILTER.getDisplayName()
                + " against the full path of files with and without the scheme and authority.  If "
                + RECURSE_SUBDIRS.getDisplayName() + " is set to true, the entire subdirectory tree will be searched for files in which the full path of "
                + "the file matches the regular expression defined in " + FILE_FILTER.getDisplayName() + ".  See 'Additional Details' for more information.");

    public static final PropertyDescriptor FILE_FILTER_MODE = new PropertyDescriptor.Builder()
        .name("file-filter-mode")
        .displayName("File Filter Mode")
        .description("Determines how the regular expression in  " + FILE_FILTER.getDisplayName() + " will be used when retrieving listings.")
        .required(true)
        .allowableValues(FILTER_DIRECTORIES_AND_FILES_VALUE, FILTER_FILES_ONLY_VALUE, FILTER_FULL_PATH_VALUE)
        .defaultValue(FILTER_DIRECTORIES_AND_FILES_VALUE.getValue())
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
        .name("minimum-file-age")
        .displayName("Minimum File Age")
        .description("The minimum age that a file must be in order to be pulled; any file younger than this "
                + "amount of time (based on last modification date) will be ignored")
        .required(false)
        .addValidator(StandardValidators.createTimePeriodValidator(0, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
        .build();

    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
        .name("maximum-file-age")
        .displayName("Maximum File Age")
        .description("The maximum age that a file must be in order to be pulled; any file older than this "
                + "amount of time (based on last modification date) will be ignored. Minimum value is 100ms.")
        .required(false)
        .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles are transferred to this relationship")
        .build();

    private volatile long latestTimestampListed = -1L;
    private volatile long latestTimestampEmitted = -1L;
    private volatile long lastRunTimestamp = -1L;
    private volatile boolean resetState = false;
    static final String LISTING_TIMESTAMP_KEY = "listing.timestamp";
    static final String EMITTED_TIMESTAMP_KEY = "emitted.timestamp";

    static final long LISTING_LAG_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);
    private Pattern fileFilterRegexPattern;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);
    }

    @Override
    protected void preProcessConfiguration(Configuration config, ProcessContext context) {
        super.preProcessConfiguration(config, context);
        // Since this processor is marked as INPUT_FORBIDDEN, the FILE_FILTER regex can be compiled here rather than during onTrigger processing
        fileFilterRegexPattern = Pattern.compile(context.getProperty(FILE_FILTER).getValue());

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
        props.add(RECORD_WRITER);
        props.add(FILE_FILTER);
        props.add(FILE_FILTER_MODE);
        props.add(MIN_AGE);
        props.add(MAX_AGE);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(context));

        final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long minimumAge = (minAgeProp == null) ? 0L : minAgeProp;
        final long maximumAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;

        if (minimumAge > maximumAge) {
            problems.add(new ValidationResult.Builder().valid(false).subject("GetHDFS Configuration")
                    .explanation(MIN_AGE.getDisplayName() + " cannot be greater than " + MAX_AGE.getDisplayName()).build());
        }

        return problems;
    }

    protected String getKey(final String directory) {
        return getIdentifier() + ".lastListingTime." + directory;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        if (isConfigurationRestored() && (descriptor.equals(DIRECTORY) || descriptor.equals(FILE_FILTER))) {
            this.resetState = true;
        }
    }

    /**
     * Determines which of the given FileStatus's describes a File that should be listed.
     *
     * @param statuses the eligible FileStatus objects that we could potentially list
     * @param context processor context with properties values
     * @return a Set containing only those FileStatus objects that we want to list
     */
    Set<FileStatus> determineListable(final Set<FileStatus> statuses, ProcessContext context) {
        final long minTimestamp = this.latestTimestampListed;
        final TreeMap<Long, List<FileStatus>> orderedEntries = new TreeMap<>();

        final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        // NIFI-4144 - setting to MIN_VALUE so that in case the file modification time is in
        // the future relative to the nifi instance, files are not skipped.
        final long minimumAge = (minAgeProp == null) ? Long.MIN_VALUE : minAgeProp;
        final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maximumAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;

        // Build a sorted map to determine the latest possible entries
        for (final FileStatus status : statuses) {
            if (status.getPath().getName().endsWith("_COPYING_")) {
                continue;
            }

            final long fileAge = System.currentTimeMillis() - status.getModificationTime();
            if (minimumAge > fileAge || fileAge > maximumAge) {
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

    @OnScheduled
    public void resetStateIfNecessary(final ProcessContext context) throws IOException {
        if (resetState) {
            getLogger().debug("Property has been modified. Resetting the state values - listing.timestamp and emitted.timestamp to -1L");
            context.getStateManager().clear(Scope.CLUSTER);
            this.resetState = false;
        }
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

        // Ensure that we are using the latest listing information before we try to perform a listing of HDFS files.
        try {
            final StateMap stateMap = session.getState(Scope.CLUSTER);
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
            getLogger().error("Failed to retrieve timestamp of last listing from the State Manager. Will not perform listing until this is accomplished.");
            context.yield();
            return;
        }

        // Pull in any file that is newer than the timestamp that we have.
        final FileSystem hdfs = getFileSystem();
        final boolean recursive = context.getProperty(RECURSE_SUBDIRS).asBoolean();
        String fileFilterMode = context.getProperty(FILE_FILTER_MODE).getValue();

        final Set<FileStatus> statuses;
        try {
            final Path rootPath = getNormalizedPath(context, DIRECTORY);
            statuses = getStatuses(rootPath, recursive, hdfs, createPathFilter(context), fileFilterMode);
            getLogger().debug("Found a total of {} files in HDFS", new Object[] {statuses.size()});
        } catch (final IOException | IllegalArgumentException e) {
            getLogger().error("Failed to perform listing of HDFS", e);
            return;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            getLogger().error("Interrupted while performing listing of HDFS", e);
            return;
        }

        final Set<FileStatus> listable = determineListable(statuses, context);
        getLogger().debug("Of the {} files found in HDFS, {} are listable", new Object[] {statuses.size(), listable.size()});

        // Create FlowFile(s) for the listing, if there are any
        if (!listable.isEmpty()) {
            if (context.getProperty(RECORD_WRITER).isSet()) {
                try {
                    createRecords(listable, context, session);
                } catch (final IOException | SchemaNotFoundException e) {
                    getLogger().error("Failed to write listing of HDFS", e);
                    return;
                }
            } else {
                createFlowFiles(listable, session);
            }
        }

        for (final FileStatus status : listable) {
            final long fileModTime = status.getModificationTime();
            if (fileModTime > latestTimestampEmitted) {
                latestTimestampEmitted = fileModTime;
            }
        }

        final Map<String, String> updatedState = new HashMap<>(1);
        updatedState.put(LISTING_TIMESTAMP_KEY, String.valueOf(latestTimestampListed));
        updatedState.put(EMITTED_TIMESTAMP_KEY, String.valueOf(latestTimestampEmitted));
        getLogger().debug("New state map: {}", new Object[] {updatedState});

        try {
            session.setState(updatedState, Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().warn("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", ioe);
        }

        final int listCount = listable.size();
        if ( listCount > 0 ) {
            getLogger().info("Successfully created listing with {} new files from HDFS", new Object[] {listCount});
            session.commitAsync();
        } else {
            getLogger().debug("There is no data to list. Yielding.");
            context.yield();
        }
    }

    private void createFlowFiles(final Set<FileStatus> fileStatuses, final ProcessSession session) {
        for (final FileStatus status : fileStatuses) {
            final Map<String, String> attributes = createAttributes(status);
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private void createRecords(final Set<FileStatus> fileStatuses, final ProcessContext context, final ProcessSession session) throws IOException, SchemaNotFoundException {
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        FlowFile flowFile = session.create();
        final WriteResult writeResult;
        try (final OutputStream out = session.write(flowFile);
             final RecordSetWriter recordSetWriter = writerFactory.createWriter(getLogger(), getRecordSchema(), out, Collections.emptyMap())) {

            recordSetWriter.beginRecordSet();
            for (final FileStatus fileStatus : fileStatuses) {
                final Record record = createRecord(fileStatus);
                recordSetWriter.write(record);
            }

            writeResult = recordSetWriter.finishRecordSet();
        }

        final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.transfer(flowFile, REL_SUCCESS);
    }

    private Record createRecord(final FileStatus fileStatus) {
        final Map<String, Object> values = new HashMap<>();
        values.put(FILENAME, fileStatus.getPath().getName());
        values.put(PATH, getAbsolutePath(fileStatus.getPath().getParent()));
        values.put(OWNER, fileStatus.getOwner());
        values.put(GROUP, fileStatus.getGroup());
        values.put(LAST_MODIFIED, new Timestamp(fileStatus.getModificationTime()));
        values.put(SIZE, fileStatus.getLen());
        values.put(REPLICATION, fileStatus.getReplication());

        final FsPermission permission = fileStatus.getPermission();
        final String perms = getPerms(permission.getUserAction()) + getPerms(permission.getGroupAction()) + getPerms(permission.getOtherAction());
        values.put(PERMISSIONS, perms);

        values.put(IS_DIRECTORY, fileStatus.isDirectory());
        values.put(IS_SYM_LINK, fileStatus.isSymlink());
        values.put(IS_ENCRYPTED, fileStatus.isEncrypted());
        values.put(IS_ERASURE_CODED, fileStatus.isErasureCoded());

        return new MapRecord(getRecordSchema(), values);
    }

    private RecordSchema getRecordSchema() {
        return RECORD_SCHEMA;
    }

    private Set<FileStatus> getStatuses(final Path path, final boolean recursive, final FileSystem hdfs, final PathFilter filter, String filterMode) throws IOException, InterruptedException {
        final Set<FileStatus> statusSet = new HashSet<>();

        getLogger().debug("Fetching listing for {}", new Object[] {path});
        final FileStatus[] statuses;
        if (isPostListingFilterNeeded(filterMode)) {
            // For this filter mode, the filter is not passed to listStatus, so that directory names will not be
            // filtered out when the listing is recursive.
            statuses = getUserGroupInformation().doAs((PrivilegedExceptionAction<FileStatus[]>) () -> hdfs.listStatus(path));
        } else {
            statuses = getUserGroupInformation().doAs((PrivilegedExceptionAction<FileStatus[]>) () -> hdfs.listStatus(path, filter));
        }

        for ( final FileStatus status : statuses ) {
            if ( status.isDirectory() ) {
                if ( recursive ) {
                    try {
                        statusSet.addAll(getStatuses(status.getPath(), recursive, hdfs, filter, filterMode));
                    } catch (final IOException ioe) {
                        getLogger().error("Failed to retrieve HDFS listing for subdirectory {} due to {}; will continue listing others", new Object[] {status.getPath(), ioe});
                    }
                }
            } else {
                if (isPostListingFilterNeeded(filterMode)) {
                    // Filtering explicitly performed here, since it was not able to be done when calling listStatus.
                    if (filter.accept(status.getPath())) {
                        statusSet.add(status);
                    }
                } else {
                    statusSet.add(status);
                }
            }
        }

        return statusSet;
    }

    /**
     * Determines if filtering needs to be applied, after calling {@link FileSystem#listStatus(Path)}, based on the
     * given filter mode.
     * Filter modes that need to be able to search directories regardless of the given filter should return true.
     * FILTER_MODE_FILES_ONLY and FILTER_MODE_FULL_PATH require that {@link FileSystem#listStatus(Path)} be invoked
     * without a filter so that all directories can be traversed when filtering with these modes.
     * FILTER_MODE_DIRECTORIES_AND_FILES should return false, since filtering can be applied directly with
     * {@link FileSystem#listStatus(Path, PathFilter)} regardless of a recursive listing.
     * @param filterMode the value of one of the defined AllowableValues representing filter modes
     * @return true if results need to be filtered, false otherwise
     */
    private boolean isPostListingFilterNeeded(String filterMode) {
        return filterMode.equals(FILTER_MODE_FILES_ONLY) || filterMode.equals(FILTER_MODE_FULL_PATH);
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

    private PathFilter createPathFilter(final ProcessContext context) {
        final String filterMode = context.getProperty(FILE_FILTER_MODE).getValue();
        return path -> {
            final boolean accepted;
            if (FILTER_FULL_PATH_VALUE.getValue().equals(filterMode)) {
                accepted = fileFilterRegexPattern.matcher(path.toString()).matches()
                        || fileFilterRegexPattern.matcher(Path.getPathWithoutSchemeAndAuthority(path).toString()).matches();
            } else {
                accepted =  fileFilterRegexPattern.matcher(path.getName()).matches();
            }
            return accepted;
        };
    }

}
