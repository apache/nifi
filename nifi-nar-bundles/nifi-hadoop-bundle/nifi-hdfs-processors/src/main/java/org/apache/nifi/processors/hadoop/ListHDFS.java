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
import org.apache.nifi.processors.hadoop.util.FileCountRemoteIterator;
import org.apache.nifi.processors.hadoop.util.FileStatusManager;
import org.apache.nifi.processors.hadoop.util.FilterMode;
import org.apache.nifi.processors.hadoop.util.FlowFileObjectWriter;
import org.apache.nifi.processors.hadoop.util.HdfsObjectWriter;
import org.apache.nifi.processors.hadoop.util.RecordObjectWriter;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
    public static final String LISTING_TIMESTAMP_KEY = "listing.timestamp";
    static final long LISTING_LAG_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);

    private volatile long lastRunTimestamp = -1L;
    private volatile boolean resetState = false;

    private FileStatusManager fileStatusManager = FileStatusManager.createFileStatusManager();
    private Pattern fileFilterRegexPattern;

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
            fileStatusManager = FileStatusManager.createFileStatusManager();
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
        if (notEnoughTimeElapsedToRun(context)) {
            return;
        }

        // Ensure that we are using the latest listing information before we try to perform a listing of HDFS files.
        boolean isTransitioningFromLegacyState = false;
        boolean isLegacyLastStatusListed = false;
        try {
            StateMap stateMap = session.getState(Scope.CLUSTER);
            if (stateMap.getStateVersion().isPresent()) {
                final String emittedString = stateMap.get(LEGACY_EMITTED_TIMESTAMP_KEY);
                // Legacy states stored two different timestamps since it always held back the latest modified file(s).
                // The timestamp for the most recently modified file(s) was stored in the listing timestamp, while the
                // timestamp for the most recently listed file (second latest timestamp) was stored in the emitting timestamp.
                // The two were equal when the processor ran with just one file remaining to list.
                if (emittedString != null) {
                    isTransitioningFromLegacyState = true;
                    final String listingString = stateMap.get(LISTING_TIMESTAMP_KEY);
                    if (listingString != null) {
                        long lastModificationTimeFromLegacyState = Long.parseLong(listingString);
                        fileStatusManager.setLastModificationTime(lastModificationTimeFromLegacyState);
                        isLegacyLastStatusListed = emittedString.equals(listingString);
                    }
                    getLogger().debug("State restored from legacy format, latesting timestamp emitted = {}, latest listed = {}", emittedString, listingString);
                    // Restoring state from new format
                } else {
                    final String listingString = stateMap.get(LISTING_TIMESTAMP_KEY);
                    if (listingString != null) {
                        long lastModificationTimeFromNewState = Long.parseLong(listingString);
                        fileStatusManager.setLastModificationTime(lastModificationTimeFromNewState);
                    } else {
                        fileStatusManager = FileStatusManager.createFileStatusManager();
                    }
                }
            } else {
                fileStatusManager = FileStatusManager.createFileStatusManager();
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

        final HdfsObjectWriter writer = getHdfsObjectWriter(session, writerFactory);

        long listedFileCount = 0;
        try {
            final Path rootPath = getNormalizedPath(context, DIRECTORY);
            final FileCountRemoteIterator<FileStatus> fileStatusIterator = getFileStatusIterator(rootPath, recursive, hdfs, pathFilter);

            final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
            final long minimumAge = (minAgeProp == null) ? Long.MIN_VALUE : minAgeProp;
            final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
            final long maximumAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;

            writer.beginListing();

            FileStatus status;
            while (fileStatusIterator.hasNext()) {
                status = fileStatusIterator.next();
                if (status != null && determineListable(status, minimumAge, maximumAge, isTransitioningFromLegacyState, isLegacyLastStatusListed)) {
                    writer.addToListing(status);
                    fileStatusManager.update(status);
                    listedFileCount++;
                }
            }
            writer.finishListing();

            long totalFileCount = fileStatusIterator.getFileCount();
            getLogger().debug("Found a total of {} files in HDFS, {} are listed", totalFileCount, listedFileCount);
        } catch (final IOException | IllegalArgumentException | SchemaNotFoundException e) {
            getLogger().error("Failed to perform listing of HDFS", e);
            writer.finishListingExceptionally(e);
            return;
        }

        if (listedFileCount > 0) {
            fileStatusManager.finishIteration();
            final Map<String, String> updatedState = new HashMap<>(1);
            updatedState.put(LISTING_TIMESTAMP_KEY, String.valueOf(fileStatusManager.getLastModificationTime()));
            getLogger().debug("New state map: {}", updatedState);
            updateState(session, updatedState);

            getLogger().info("Successfully created listing with {} new files from HDFS", listedFileCount);
            session.commitAsync();
        } else {
            getLogger().debug("There is no data to list. Yielding.");
            context.yield();
        }
    }

    private HdfsObjectWriter getHdfsObjectWriter(final ProcessSession session, final RecordSetWriterFactory writerFactory) {
        final HdfsObjectWriter writer;
        if (writerFactory == null) {
            writer = new FlowFileObjectWriter(session);
        } else {
            writer = new RecordObjectWriter(session, writerFactory, getLogger());
        }
        return writer;
    }

    private boolean notEnoughTimeElapsedToRun(final ProcessContext context) {
        final long now = System.nanoTime();
        if (now - lastRunTimestamp < LISTING_LAG_NANOS) {
            context.yield();
            return true;
        }
        lastRunTimestamp = now;
        return false;
    }

    private boolean determineListable(final FileStatus status, final long minimumAge, final long maximumAge, final boolean isTransitioningFromLegacyState, final boolean isLegacyLastStatusListed) {
        // If the file was created during the processor's last iteration we have to check if it was already listed
        // If legacy state was used and the file was already listed once, we don't want to list it once again.
        if (status.getModificationTime() == fileStatusManager.getLastModificationTime()) {
            if (isTransitioningFromLegacyState) {
                return !isLegacyLastStatusListed;
            }
            return !fileStatusManager.getLastModifiedStatuses().contains(status);
        }

        final long fileAge = System.currentTimeMillis() - status.getModificationTime();
        if (minimumAge > fileAge || fileAge > maximumAge) {
            return false;
        }

        return status.getModificationTime() > fileStatusManager.getLastModificationTime();
    }

    private FileCountRemoteIterator<FileStatus> getFileStatusIterator(final Path path, final boolean recursive, final FileSystem hdfs, final PathFilter filter) {
        final Deque<Path> pathStack = new ArrayDeque<>();
        pathStack.push(path);

        return new FileCountRemoteIterator<>() {
            private FileStatus[] currentBatch = null;
            private int currentIndex = 0;
            private long fileCount = 0L;

            @Override
            public boolean hasNext() throws IOException {
                if (currentBatch == null || currentIndex >= currentBatch.length) {
                    fetchNextBatch();
                }
                return currentBatch != null && currentIndex < currentBatch.length;
            }

            @Override
            public FileStatus next() throws IOException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                FileStatus status = getNextStatus();

                while (status != null && shouldSkip(status)) {
                    status = getNextStatus();
                }
                return status;
            }

            private void fetchNextBatch() throws IOException {
                if (pathStack.isEmpty()) {
                    currentBatch = null;
                } else {
                    Path currentPath = pathStack.pop();
                    getLogger().debug("Fetching listing for {}", currentPath);

                    try {
                        currentBatch = getUserGroupInformation().doAs((PrivilegedExceptionAction<FileStatus[]>) () -> hdfs.listStatus(currentPath));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        getLogger().error("Interrupted while performing listing of HDFS", e);
                    }
                    currentIndex = 0;

                    if (recursive) {
                        for (final FileStatus status : currentBatch) {
                            if (status.isDirectory()) {
                                pathStack.push(status.getPath());
                            }
                        }
                    }
                }
            }

            public long getFileCount() {
                return fileCount;
            }

            private boolean shouldSkip(FileStatus status) {
                final boolean isDirectory = status.isDirectory();
                final boolean isFilteredOut = !filter.accept(status.getPath());
                final boolean isCopyInProgress = status.getPath().getName().endsWith("_COPYING_");

                return isDirectory || isFilteredOut || isCopyInProgress;
            }

            private FileStatus getNextStatus() throws IOException {
                if (!hasNext()) {
                    return null;
                }
                fileCount++;
                return currentBatch[currentIndex++];
            }
        };
    }

    private PathFilter createPathFilter(final ProcessContext context) {
        final FilterMode filterMode = FilterMode.forName(context.getProperty(FILE_FILTER_MODE).getValue());
        final boolean recursive = context.getProperty(RECURSE_SUBDIRS).asBoolean();

        switch (filterMode) {
            case FILTER_DIRECTORIES_AND_FILES:
                return path -> Stream.of(path.toString().split("/"))
                        .skip(getPathSegmentsToSkip(recursive))
                        .allMatch(v -> fileFilterRegexPattern.matcher(v).matches());
            case FILTER_MODE_FULL_PATH:
                return path -> fileFilterRegexPattern.matcher(path.toString()).matches()
                        || fileFilterRegexPattern.matcher(Path.getPathWithoutSchemeAndAuthority(path).toString()).matches();
            default:
                return path -> fileFilterRegexPattern.matcher(path.getName()).matches();
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
