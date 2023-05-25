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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.deprecation.log.DeprecationLogger;
import org.apache.nifi.deprecation.log.DeprecationLoggerFactory;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.util.FileStatusIterable;
import org.apache.nifi.processors.hadoop.util.FileStatusManager;
import org.apache.nifi.processors.hadoop.util.FilterMode;
import org.apache.nifi.processors.hadoop.util.writer.FlowFileObjectWriter;
import org.apache.nifi.processors.hadoop.util.writer.HdfsObjectWriter;
import org.apache.nifi.processors.hadoop.util.writer.RecordObjectWriter;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.nifi.processors.hadoop.util.FilterMode.FILTER_DIRECTORIES_AND_FILES;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"hadoop", "HCFS", "HDFS", "get", "list", "ingest", "source", "filesystem"})
@SeeAlso({GetHDFS.class, FetchHDFS.class, PutHDFS.class})
@CapabilityDescription("Retrieves a listing of files from HDFS. For each file that is listed in HDFS, this processor creates a FlowFile that represents "
        + "the HDFS file to be fetched in conjunction with FetchHDFS. This Processor is designed to run on Primary Node only in a cluster. If the primary "
        + "node changes, the new Primary Node will pick up where the previous node left off without duplicating all of the data. Unlike GetHDFS, this "
        + "Processor does not delete any data from HDFS.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file that was read from HDFS."),
        @WritesAttribute(attribute = "path", description = "The path is set to the absolute path of the file's directory on HDFS. For example, if the Directory property is set to /tmp, "
                + "then files picked up from /tmp will have the path attribute set to \"./\". If the Recurse Subdirectories property is set to true and a file is picked up "
                + "from /tmp/abc/1/2/3, then the path attribute will be set to \"/tmp/abc/1/2/3\"."),
        @WritesAttribute(attribute = "hdfs.owner", description = "The user that owns the file in HDFS"),
        @WritesAttribute(attribute = "hdfs.group", description = "The group that owns the file in HDFS"),
        @WritesAttribute(attribute = "hdfs.lastModified", description = "The timestamp of when the file in HDFS was last modified, as milliseconds since midnight Jan 1, 1970 UTC"),
        @WritesAttribute(attribute = "hdfs.length", description = "The number of bytes in the file in HDFS"),
        @WritesAttribute(attribute = "hdfs.replication", description = "The number of HDFS replicas for hte file"),
        @WritesAttribute(attribute = "hdfs.permissions", description = "The permissions for the file in HDFS. This is formatted as 3 characters for the owner, "
                + "3 for the group, and 3 for other users. For example rw-rw-r--")
})
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of HDFS files, the latest timestamp of all the files listed is stored. "
        + "This allows the Processor to list only files that have been added or modified after this date the next time that the Processor is run, "
        + "without having to store all of the actual filenames/paths which could lead to performance problems. State is stored across the cluster "
        + "so that this Processor can be run on Primary Node only and if a new Primary Node is selected, the new node can pick up where the previous "
        + "node left off, without duplicating the data.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListHDFS extends AbstractHadoopProcessor {

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
            .description("Specifies the Record Writer to use for creating the listing. If not specified, one FlowFile will be created for each "
                    + "entity that is listed. If the Record Writer is specified, all entities will be written to a single FlowFile.")
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

    public static final PropertyDescriptor FILE_FILTER_MODE = new PropertyDescriptor.Builder()
            .name("file-filter-mode")
            .displayName("File Filter Mode")
            .description("Determines how the regular expression in  " + FILE_FILTER.getDisplayName() + " will be used when retrieving listings.")
            .required(true)
            .allowableValues(FilterMode.class)
            .defaultValue(FILTER_DIRECTORIES_AND_FILES.getValue())
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

    private static final DeprecationLogger deprecationLogger = DeprecationLoggerFactory.getLogger(ListHDFS.class);

    public static final String LEGACY_EMITTED_TIMESTAMP_KEY = "emitted.timestamp";
    public static final String LEGACY_LISTING_TIMESTAMP_KEY = "listing.timestamp";
    public static final String LATEST_TIMESTAMP_KEY = "latest.timestamp";
    public static final String LATEST_FILES_KEY = "latest.files";

    private volatile boolean resetState = false;

    private Pattern fileFilterRegexPattern;
    private long latestModificationTime;
    private List<String> latestModifiedStatuses;

    @Override
    protected void preProcessConfiguration(Configuration config, ProcessContext context) {
        super.preProcessConfiguration(config, context);
        // Since this processor is marked as INPUT_FORBIDDEN, the FILE_FILTER regex can be compiled here rather than during onTrigger processing
        fileFilterRegexPattern = Pattern.compile(context.getProperty(FILE_FILTER).getValue());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = new ArrayList<>(properties);
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

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        if (isConfigurationRestored() && (descriptor.equals(DIRECTORY) || descriptor.equals(FILE_FILTER))) {
            this.resetState = true;
        }
    }

    @OnScheduled
    public void resetStateIfNecessary(final ProcessContext context) throws IOException {
        if (resetState) {
            getLogger().debug("Property has been modified. Resetting the state values.");
            context.getStateManager().clear(Scope.CLUSTER);
            this.resetState = false;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Ensure that we are using the latest listing information before we try to perform a listing of HDFS files.
        try {
            latestModifiedStatuses = new ArrayList<>();
            StateMap stateMap = session.getState(Scope.CLUSTER);
            String latestListedTimestampString = stateMap.get(LATEST_TIMESTAMP_KEY);
            String latestFiles = stateMap.get(LATEST_FILES_KEY);

            final String legacyLatestListingTimestampString = stateMap.get(LEGACY_LISTING_TIMESTAMP_KEY);
            final String legacyLatestEmittedTimestampString = stateMap.get(LEGACY_EMITTED_TIMESTAMP_KEY);

            if (legacyLatestListingTimestampString != null) {
                final long legacyLatestListingTimestamp = Long.parseLong(legacyLatestListingTimestampString);
                final long legacyLatestEmittedTimestamp = Long.parseLong(legacyLatestEmittedTimestampString);
                latestModificationTime = legacyLatestListingTimestamp == legacyLatestEmittedTimestamp ? legacyLatestListingTimestamp + 1 : legacyLatestListingTimestamp;
            } else if (latestListedTimestampString != null) {
                latestModificationTime = Long.parseLong(latestListedTimestampString);
                latestModifiedStatuses = new ArrayList<>(Arrays.asList(latestFiles.split("\\s")));
            }
        } catch (IOException e) {
            getLogger().error("Failed to retrieve timestamp of last listing from the State Manager. Will not perform listing until this is accomplished.");
            context.yield();
            return;
        }

        // Pull in any file that is newer than the timestamp that we have.
        final FileSystem hdfs = getFileSystem();
        final boolean recursive = context.getProperty(RECURSE_SUBDIRS).asBoolean();
        final PathFilter pathFilter = createPathFilter(context);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final FileStatusManager fileStatusManager = new FileStatusManager();
        final Path rootPath = getNormalizedPath(context, DIRECTORY);
        final FileStatusIterable fileStatuses = new FileStatusIterable(rootPath, recursive, hdfs, getUserGroupInformation());

        final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long minimumAge = (minAgeProp == null) ? Long.MIN_VALUE : minAgeProp;
        final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maximumAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;

        final HdfsObjectWriter writer = getHdfsObjectWriter(session, writerFactory, fileStatuses, minimumAge, maximumAge, pathFilter, fileStatusManager);

        writer.write();

        getLogger().debug("Found a total of {} files in HDFS, {} are listed", fileStatuses.getTotalFileCount(), writer.getListedFileCount());


        if (writer.getListedFileCount() > 0) {
            final Map<String, String> updatedState = new HashMap<>(1);
            updatedState.put(LATEST_TIMESTAMP_KEY, String.valueOf(fileStatusManager.getLastModificationTime()));
            updatedState.put(LATEST_FILES_KEY, String.valueOf(fileStatusManager.getLastModifiedStatusesAsString()));
            getLogger().debug("New state map: {}", updatedState);
            updateState(session, updatedState);

            getLogger().info("Successfully created listing with {} new files from HDFS", writer.getListedFileCount());
        } else {
            getLogger().debug("There is no data to list. Yielding.");
            context.yield();
        }
    }

    private HdfsObjectWriter getHdfsObjectWriter(final ProcessSession session, final RecordSetWriterFactory writerFactory,
                                                 FileStatusIterable fileStatuses, long minimumAge, long maximumAge,
                                                 PathFilter pathFilter, FileStatusManager fileStatusManager) {
        final HdfsObjectWriter writer;
        if (writerFactory == null) {
            writer = new FlowFileObjectWriter(session, fileStatuses, minimumAge, maximumAge,
                    pathFilter, fileStatusManager, latestModificationTime, latestModifiedStatuses);
        } else {
            writer = new RecordObjectWriter(session, writerFactory, getLogger(), fileStatuses, minimumAge, maximumAge,
                    pathFilter, fileStatusManager, latestModificationTime, latestModifiedStatuses);
        }
        return writer;
    }

    private PathFilter createPathFilter(final ProcessContext context) {
        final FilterMode filterMode = FilterMode.forName(context.getProperty(FILE_FILTER_MODE).getValue());
        final boolean recursive = context.getProperty(RECURSE_SUBDIRS).asBoolean();

        switch (filterMode) {
            case FILTER_MODE_FILES_ONLY:
                return path -> fileFilterRegexPattern.matcher(path.getName()).matches();
            case FILTER_MODE_FULL_PATH:
                return path -> fileFilterRegexPattern.matcher(path.toString()).matches()
                        || fileFilterRegexPattern.matcher(Path.getPathWithoutSchemeAndAuthority(path).toString()).matches();
            // FILTER_DIRECTORIES_AND_FILES
            default:
                return path -> Stream.of(path.toString().split("/"))
                        .skip(getPathSegmentsToSkip(recursive))
                        .allMatch(v -> fileFilterRegexPattern.matcher(v).matches());
        }
    }

    private int getPathSegmentsToSkip(final boolean recursive) {
        // We need to skip the first leading '/' of the path and if the traverse is recursive
        // the filter will be applied only to the subdirectories.
        return recursive ? 2 : 1;
    }

    void updateState(final ProcessSession session, final Map<String, String> newState) {
        // In case of legacy state we update the state even if there are no listable files.
        try {
            session.setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            getLogger().warn("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", e);
        }
    }
}
