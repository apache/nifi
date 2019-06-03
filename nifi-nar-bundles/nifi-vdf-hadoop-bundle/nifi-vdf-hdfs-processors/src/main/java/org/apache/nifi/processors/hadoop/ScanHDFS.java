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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"hadoop", "HDFS", "scan", "get", "list", "ingest", "source", "filesystem"})
@CapabilityDescription("Retrieves a listing of files from HDFS. The output will be 1 flowfile that holds a summary and all detected files in a JSON. This processor can optionally take an incoming "
        + "flowfile to set the HDFS query parameters dynamically. For each run, 2 HDFS listings are retrieved, a configurable (Listing delay) timelapse apart. If the latest timestamp has changed in the second listings the latest timestamp will be excluded from the final listing. This is done to "
        + "ensure that no files in transit or currently still being modified are listed. This Processor is designed to run on 1 thread per cluster node only. Contrary to ListHDFS, this processor doesn't keep state of previous runs of the same configuration. To prevent redundant file "
        + "retrievals downstream of this processor, state can also be managed with the help of Fetch- and GetDistributedMapCache in the flow. To support this, summary information is written to the outbound flowfile attributes")
@WritesAttributes({
    @WritesAttribute(attribute="hdfs.last.modified.ts.", description="The latest modification timestamp of any file in the listing. Will be -1 when no files are listed at all"),
    @WritesAttribute(attribute="hdfs.number.of.files", description="The number of files that were listed in the JSON summary as a results of the HDFS query")
})
public class ScanHDFS extends AbstractHadoopProcessor {

    public static final PropertyDescriptor RECURSE_SUBDIRS = new PropertyDescriptor.Builder()
        .name("Recurse Subdirectories")
        .description("Indicates whether to list files from subdirectories of the HDFS root directory")
        .required(true)
        // Currently allowable values can't be married with EL
        //.allowableValues("true", "false")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .defaultValue("false")
        .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
        .name("File Filter")
        .description("Only files whose names match the given regular expression will be picked up")
        .required(true)
        .defaultValue("[^\\.].*")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .build();

    public static final PropertyDescriptor LISTING_DELAY = new PropertyDescriptor.Builder()
            .name("Listing Delay")
            .description("Time in milliseconds to wait before a second HDFS listing is performed. To prevent listing of files that are currently still being written, "
                    +"the processor will take 2 listings, the amount of milliseconds apart, and not include any files that have the latest modification timestamp of any "
                    +"file in the query. If the latest timestamp hasn't changed in between both listings all files will be listed. At 0 millis the listing will contain "
                    +"all files detected during the first run and skip the second. At this setting there is a risk of retrieving files in modification, but the processor runs at higher speed")
            .required(true)
            .defaultValue("100 millis")
            .addValidator(StandardValidators.createTimePeriodValidator(0L, TimeUnit.MILLISECONDS, 10000L,TimeUnit.MILLISECONDS))
            .build();

    public static final PropertyDescriptor MIN_MOD = new PropertyDescriptor.Builder()
            .name("minimum-modification-ts")
            .displayName("Minimum Hdfs Modification Timestamp")
            .description("The modification timestamp of a file must be greater than this in order to be listed. Any file with a smaller or equal "
                    + "last modification timestamp will be ignored")
            .required(false)
            .defaultValue("Long.MIN_VALUE")
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_MOD = new PropertyDescriptor.Builder()
            .name("maximum-modification-ts")
            .displayName("Maximum Hdfs Modification Timestamp")
            .description("The modification timestamp of a file must be smaller than this in order to be listed. Any file with a greater or equal "
                    + "last modification timestamp will be ignored")
            .required(false)
            .defaultValue("Long.MAX_VALUE")
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
        .name("minimum-file-age")
        .displayName("Minimum File Age")
        .description("The minimum age that a file must be in order to be listed; any file younger than this "
                + "amount of time (based on last modification timestamp) will be ignored")
        .required(false)
        .addValidator(StandardValidators.createTimePeriodValidator(0, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
        .build();

    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
        .name("maximum-file-age")
        .displayName("Maximum File Age")
        .description("The maximum age that a file must be in order to be listed; any file older than this "
                + "amount of time (based on last modification timestamp) will be ignored. Minimum value is 100ms.")
        .required(false)
        .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
        .build();

     public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles will be routed to this relationship if the content of the HDFS file cannot be retrieved and trying again will likely not be helpful. "
                    + "This would occur, for instance, if the file is not found or if there is a permissions issue")
            .build();
    static final Relationship REL_COMMS_FAILURE = new Relationship.Builder()
            .name("comms.failure")
            .description("FlowFiles will be routed to this relationship if the content of the HDFS file cannot be retrieve due to a communications failure. "
                    + "This generally indicates that the Fetch should be tried again.")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original incoming FlowFiles if any, are transferred to this relationship")
            .build();

    private volatile long latestModTimestampListed = -1L;
    private volatile long latestModTimestampEmitted = -1L;
    private volatile String directory = null;
    private volatile Boolean recursive = null;
    private volatile Pattern fileFilterPattern = null;

    static long listingDelay;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = new ArrayList<>(properties);
        props.add(DIRECTORY);
        props.add(RECURSE_SUBDIRS);
        props.add(FILE_FILTER);
        props.add(LISTING_DELAY);
        props.add(MIN_MOD);
        props.add(MAX_MOD);
        props.add(MIN_AGE);
        props.add(MAX_AGE);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_COMMS_FAILURE);
        relationships.add(REL_ORIGINAL);
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
            problems.add(new ValidationResult.Builder().valid(false).subject("ScanHDFS Configuration")
                    .explanation(MIN_AGE.getName() + " cannot be greater than " + MAX_AGE.getName()).build());
        }
        return problems;
    }

    /**
     * Determines which of the given FileStatus's describes a File that should be listed.
     *
     * @param statuses the eligible FileStatus objects that we could potentially list
     * @param context processor context with properties values
     * @return a Set containing only those FileStatus objects that we want to list
     */
    private Set<FileStatus> determineListable(final Set<FileStatus> statuses, ProcessContext context, FlowFile inputFlowFile, boolean newQuery) {
        final long previousLatestModTimestamp = this.latestModTimestampListed;
        final TreeMap<Long, List<FileStatus>> orderedEntries = new TreeMap<>();

        final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        // NIFI-4144 - setting to MIN_VALUE so that in case the file modification time is in
        // the future relative to the nifi instance, files are not skipped.
        final long minimumAge = (minAgeProp == null) ? Long.MIN_VALUE : minAgeProp;
        final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maximumAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;

        final String minimumModProp = context.getProperty(MIN_MOD).evaluateAttributeExpressions(inputFlowFile).getValue();
        final Long minimumMod = minimumModProp.equals("Long.MIN_VALUE") ? Long.MIN_VALUE : Long.valueOf(minimumModProp);
        final String maximumModProp = context.getProperty(MAX_MOD).evaluateAttributeExpressions(inputFlowFile).getValue();
        final Long maximumMod = maximumModProp.equals("Long.MAX_VALUE") ? Long.MAX_VALUE : Long.valueOf(maximumModProp);

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
            getLogger().debug("file: "+status.getPath()+ " mod ts: "+ status.getModificationTime()+" maximumMod: "+ maximumMod+" minimumMod: "+minimumMod);

            if ((entityTimestamp >= maximumMod) || (entityTimestamp <= minimumMod)) {
                continue;
            }

            if (entityTimestamp > latestModTimestampListed) {
                latestModTimestampListed = entityTimestamp;
            }

            List<FileStatus> entitiesForTimestamp = orderedEntries.get(status.getModificationTime());
            if (entitiesForTimestamp == null) {
                entitiesForTimestamp = new ArrayList<>();
                orderedEntries.put(status.getModificationTime(), entitiesForTimestamp);
            }
            entitiesForTimestamp.add(status);
        }

        final Set<FileStatus> toList = new HashSet<>();
        latestModTimestampEmitted = latestModTimestampListed;

        if (orderedEntries.size() > 0) {
            long latestListingTimestamp = orderedEntries.lastKey();

            // If the last listing time is greater than the one from the previous pass, it means that there are newer files detected since the first snapshot.
            // Moreover, with listingDelay at 0L, the check for modification is skipped, favoring speed at the risk of listing files in motion
            if (latestListingTimestamp > previousLatestModTimestamp && listingDelay > 0L) {
                // The newest entries are held back to avoid issues in writes occurring exactly when the listing is being performed to avoid missing data
                orderedEntries.remove(latestListingTimestamp);
                if (orderedEntries.size() > 0) {
                    latestModTimestampEmitted = orderedEntries.lastKey();
                }
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
        FlowFile inputFlowFile = null;

        if (context.hasIncomingConnection()) {
            inputFlowFile = session.get();

            if (inputFlowFile == null && context.hasNonLoopConnection()) {
                return;
            }
        }
        directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions(inputFlowFile).getValue();
        recursive = context.getProperty(RECURSE_SUBDIRS).evaluateAttributeExpressions(inputFlowFile).asBoolean();
        fileFilterPattern = Pattern.compile(context.getProperty(FILE_FILTER).evaluateAttributeExpressions(inputFlowFile).getValue());
        // This happens when the incoming ff attribute that is mapped to the FILE_FILTER prop does has no or null value
        if (fileFilterPattern.pattern().isEmpty()) {
            fileFilterPattern = Pattern.compile(FILE_FILTER.getDefaultValue());
        }
        getLogger().debug("regex:" + fileFilterPattern.pattern() + "|" + fileFilterPattern.toString());
        listingDelay = context.getProperty(LISTING_DELAY).asTimePeriod(TimeUnit.MILLISECONDS);

        final FileSystem hdfs = getFileSystem();
        Set<FileStatus> listable = null;

        try {
            listable = performHdfsListing(hdfs, context, inputFlowFile, true);
        } catch (Exception e) {
            return;
        }

        // For each query, 2 hdfs snapshots are taken, LISTING_DELAY apart. This is to determine whether the latest modification
        // time found is changed in between the snapshots, meaning that some files are being written to and should be skipped
        // only when the listing delay property is set the 2nd cycle is performed
        if (listingDelay > 0L) {
            try {
                Thread.sleep(listingDelay);
            } catch (InterruptedException e) {
                return;
            }

            try {
                listable = performHdfsListing(hdfs, context, inputFlowFile, false);
            } catch (Exception e) {
                return;
            }
        }

        JsonNode jsonSummary = createJsonNode(listable);
        FlowFile flowFile = session.create();

        if (inputFlowFile != null) {
            final Map<String, String> attributesOut = inputFlowFile.getAttributes();
            flowFile = session.putAllAttributes(flowFile, attributesOut);
        }
        flowFile = session.putAttribute(flowFile,"hdfs.last.modified.ts", String.valueOf(latestModTimestampListed));
        flowFile = session.putAttribute(flowFile,"hdfs.number.of.files", String.valueOf(listable.size()));
        flowFile = session.write(flowFile, out -> out.write(objectMapper.writeValueAsBytes(jsonSummary)));

        session.transfer(flowFile, REL_SUCCESS);

        if (inputFlowFile != null) {
            session.transfer(inputFlowFile, REL_ORIGINAL);
        }
        final int listCount = listable.size();
        if ( listCount > 0 ) {
            getLogger().info("Successfully created listing with {} new files from HDFS", new Object[] {listCount});
            session.commit();
        } else {
            getLogger().debug("There is no data to list. Yielding.");
            context.yield();
        }
        fileFilterPattern = null;
        recursive = null;
        directory = null;
        latestModTimestampListed = -1L;
        latestModTimestampEmitted = -1L;
    }

    private Set<FileStatus> performHdfsListing(FileSystem hdfs, ProcessContext context, FlowFile inputFlowFile, boolean newQuery) throws Exception{

        final Set<FileStatus> statuses;
        try {
            final Path rootPath = new Path(directory);
            statuses = getStatuses(rootPath, hdfs, createPathFilter(fileFilterPattern));
            getLogger().debug("Found a total of {} files in HDFS", new Object[] {statuses.size()});
        } catch (final IOException | IllegalArgumentException e) {
            getLogger().error("Failed to perform listing of HDFS due to {}", new Object[] {e});
            throw new Exception(e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            getLogger().error("Interrupted while performing listing of HDFS", e);
            throw new Exception(e);
        }

        final Set<FileStatus> listable = determineListable(statuses, context, inputFlowFile, newQuery);
        getLogger().debug("Of the {} files found in HDFS, {} are listable", new Object[] {statuses.size(), listable.size()});

        return listable;
    }

    private JsonNode createJsonNode(Set<FileStatus> listable) {
        ObjectNode summaryNode = objectMapper.createObjectNode();
        ArrayNode results = summaryNode.putArray("files");
        summaryNode.put("last_modification_ts", latestModTimestampEmitted);
        summaryNode.put("directory", directory);
        summaryNode.put("recursive", recursive);
        summaryNode.put("regex", fileFilterPattern.pattern());
        summaryNode.put("num_files", listable.size());

        for (final FileStatus status : listable) {
            ObjectNode fileNode = objectMapper.createObjectNode();
            fileNode.put("owner", status.getOwner());
            fileNode.put("group", status.getGroup());
            fileNode.put("last_modified", String.valueOf(status.getModificationTime()));
            fileNode.put("last_accessed", String.valueOf(status.getAccessTime()));
            fileNode.put("length", String.valueOf(status.getLen()));
            fileNode.put("path", String.valueOf(Path.getPathWithoutSchemeAndAuthority(status.getPath()).toString()));
            fileNode.put("filename", String.valueOf(status.getPath().getName()));

            final FsPermission permission = status.getPermission();
            final String perms = getPerms(permission.getUserAction()) + getPerms(permission.getGroupAction()) + getPerms(permission.getOtherAction());
            fileNode.put("permissions", perms);
            results.add(fileNode);
        }
        return summaryNode;
    }

    private Set<FileStatus> getStatuses(final Path path, final FileSystem hdfs, final PathFilter filter) throws IOException, InterruptedException {
        final Set<FileStatus> statusSet = new HashSet<>();

        getLogger().debug("Fetching listing for {}", new Object[] {path});
        final FileStatus[] statuses;
        statuses = getUserGroupInformation().doAs((PrivilegedExceptionAction<FileStatus[]>) () -> hdfs.listStatus(path));

        for ( final FileStatus status : statuses ) {
            if ( status.isDirectory() ) {

                if ( recursive ) {
                    try {
                        statusSet.addAll(getStatuses(status.getPath(), hdfs, filter));
                    } catch (final IOException ioe) {
                        getLogger().error("Failed to retrieve HDFS listing for subdirectory {} due to {}; will continue listing others", new Object[] {status.getPath(), ioe});
                    }
                }
            } else {
                if (filter.accept(status.getPath())) {
                    statusSet.add(status);
                }
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

    private PathFilter createPathFilter(final Pattern filePattern) {
        return path -> {
            return filePattern.matcher(path.getName()).matches();
        };
    }
}