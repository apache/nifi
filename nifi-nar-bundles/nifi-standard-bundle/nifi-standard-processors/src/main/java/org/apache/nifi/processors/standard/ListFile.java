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

package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processors.standard.util.FileInfo;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"file", "get", "list", "ingest", "source", "filesystem"})
@CapabilityDescription("Retrieves a listing of files from the local filesystem. For each file that is listed, " +
        "creates a FlowFile that represents the file so that it can be fetched in conjunction with FetchFile. This " +
        "Processor is designed to run on Primary Node only in a cluster. If the primary node changes, the new " +
        "Primary Node will pick up where the previous node left off without duplicating all of the data. Unlike " +
        "GetFile, this Processor does not delete any data from the local filesystem.")
@WritesAttributes({
        @WritesAttribute(attribute="filename", description="The name of the file that was read from filesystem."),
        @WritesAttribute(attribute="path", description="The path is set to the relative path of the file's directory " +
                "on filesystem compared to the Input Directory property.  For example, if Input Directory is set to " +
                "/tmp, then files picked up from /tmp will have the path attribute set to \"/\". If the Recurse " +
                "Subdirectories property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path " +
                "attribute will be set to \"abc/1/2/3/\"."),
        @WritesAttribute(attribute="absolute.path", description="The absolute.path is set to the absolute path of " +
                "the file's directory on filesystem. For example, if the Input Directory property is set to /tmp, " +
                "then files picked up from /tmp will have the path attribute set to \"/tmp/\". If the Recurse " +
                "Subdirectories property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path " +
                "attribute will be set to \"/tmp/abc/1/2/3/\"."),
        @WritesAttribute(attribute=ListFile.FILE_OWNER_ATTRIBUTE, description="The user that owns the file in filesystem"),
        @WritesAttribute(attribute=ListFile.FILE_GROUP_ATTRIBUTE, description="The group that owns the file in filesystem"),
        @WritesAttribute(attribute=ListFile.FILE_SIZE_ATTRIBUTE, description="The number of bytes in the file in filesystem"),
        @WritesAttribute(attribute=ListFile.FILE_PERMISSIONS_ATTRIBUTE, description="The permissions for the file in filesystem. This " +
                "is formatted as 3 characters for the owner, 3 for the group, and 3 for other users. For example " +
                "rw-rw-r--"),
        @WritesAttribute(attribute=ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
                "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute=ListFile.FILE_LAST_ACCESS_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
                "last accessed as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute=ListFile.FILE_CREATION_TIME_ATTRIBUTE, description="The timestamp of when the file in filesystem was " +
                "created as 'yyyy-MM-dd'T'HH:mm:ssZ'")
})
@SeeAlso({GetFile.class, PutFile.class, FetchFile.class})
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
    + "This allows the Processor to list only files that have been added or modified after "
    + "this date the next time that the Processor is run. Whether the state is stored with a Local or Cluster scope depends on the value of the "
    + "<Input Directory Location> property.")
public class ListFile extends AbstractListProcessor<FileInfo> {
    static final AllowableValue LOCATION_LOCAL = new AllowableValue("Local", "Local", "Input Directory is located on a local disk. State will be stored locally on each node in the cluster.");
    static final AllowableValue LOCATION_REMOTE = new AllowableValue("Remote", "Remote", "Input Directory is located on a remote system. State will be stored across the cluster so that "
        + "the listing can be performed on Primary Node Only and another node can pick up where the last node left off, if the Primary Node changes");

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Input Directory")
            .description("The input directory from which files to pull files")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether to list files from subdirectories of the directory")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor DIRECTORY_LOCATION = new PropertyDescriptor.Builder()
            .name("Input Directory Location")
            .description("Specifies where the Input Directory is located. This is used to determine whether state should be stored locally or across the cluster.")
            .allowableValues(LOCATION_LOCAL, LOCATION_REMOTE)
            .defaultValue(LOCATION_LOCAL.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue("[^\\.].*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor PATH_FILTER = new PropertyDescriptor.Builder()
            .name("Path Filter")
            .description("When " + RECURSE.getName() + " is true, then only subdirectories whose path matches the given regular expression will be scanned")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();


    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("Minimum File Age")
            .description("The minimum age that a file must be in order to be pulled; any file younger than this amount of time (according to last modification date) will be ignored")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
            .name("Maximum File Age")
            .description("The maximum age that a file must be in order to be pulled; any file older than this amount of time (according to last modification date) will be ignored")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
            .build();

    public static final PropertyDescriptor MIN_SIZE = new PropertyDescriptor.Builder()
            .name("Minimum File Size")
            .description("The minimum size that a file must be in order to be pulled")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("0 B")
            .build();

    public static final PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum File Size")
            .description("The maximum size that a file can be in order to be pulled")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor IGNORE_HIDDEN_FILES = new PropertyDescriptor.Builder()
            .name("Ignore Hidden Files")
            .description("Indicates whether or not hidden files should be ignored")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final AtomicReference<FileFilter> fileFilterRef = new AtomicReference<>();

    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_SIZE_ATTRIBUTE = "file.size";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DIRECTORY);
        properties.add(RECURSE);
        properties.add(DIRECTORY_LOCATION);
        properties.add(FILE_FILTER);
        properties.add(PATH_FILTER);
        properties.add(MIN_AGE);
        properties.add(MAX_AGE);
        properties.add(MIN_SIZE);
        properties.add(MAX_SIZE);
        properties.add(IGNORE_HIDDEN_FILES);
        properties.add(TARGET_SYSTEM_TIMESTAMP_PRECISION);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        fileFilterRef.set(createFileFilter(context));
    }

    @Override
    protected Map<String, String> createAttributes(final FileInfo fileInfo, final ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();

        final String fullPath = fileInfo.getFullPathFileName();
        final File file = new File(fullPath);
        final Path filePath = file.toPath();
        final Path directoryPath = new File(getPath(context)).toPath();

        final Path relativePath = directoryPath.toAbsolutePath().relativize(filePath.getParent());
        String relativePathString = relativePath.toString();
        relativePathString = relativePathString.isEmpty() ? "." + File.separator : relativePathString + File.separator;

        final Path absPath = filePath.toAbsolutePath();
        final String absPathString = absPath.getParent().toString() + File.separator;

        attributes.put(CoreAttributes.PATH.key(), relativePathString);
        attributes.put(CoreAttributes.FILENAME.key(), fileInfo.getFileName());
        attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), absPathString);

        try {
            FileStore store = Files.getFileStore(filePath);
            if (store.supportsFileAttributeView("basic")) {
                try {
                    final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                    BasicFileAttributeView view = Files.getFileAttributeView(filePath, BasicFileAttributeView.class);
                    BasicFileAttributes attrs = view.readAttributes();
                    attributes.put(FILE_SIZE_ATTRIBUTE, Long.toString(attrs.size()));
                    attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastModifiedTime().toMillis())));
                    attributes.put(FILE_CREATION_TIME_ATTRIBUTE, formatter.format(new Date(attrs.creationTime().toMillis())));
                    attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastAccessTime().toMillis())));
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("owner")) {
                try {
                    FileOwnerAttributeView view = Files.getFileAttributeView(filePath, FileOwnerAttributeView.class);
                    attributes.put(FILE_OWNER_ATTRIBUTE, view.getOwner().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("posix")) {
                try {
                    PosixFileAttributeView view = Files.getFileAttributeView(filePath, PosixFileAttributeView.class);
                    attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
                    attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
        } catch (IOException ioe) {
            // well then this FlowFile gets none of these attributes
            getLogger().warn("Error collecting attributes for file {}, message is {}",
                    new Object[]{absPathString, ioe.getMessage()});
        }

        return attributes;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected Scope getStateScope(final ProcessContext context) {
        final String location = context.getProperty(DIRECTORY_LOCATION).getValue();
        if (LOCATION_REMOTE.getValue().equalsIgnoreCase(location)) {
            return Scope.CLUSTER;
        }

        return Scope.LOCAL;
    }

    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp) throws IOException {
        final File path = new File(getPath(context));
        final Boolean recurse = context.getProperty(RECURSE).asBoolean();
        return scanDirectory(path, fileFilterRef.get(), recurse, minTimestamp);
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return DIRECTORY.equals(property)
                || RECURSE.equals(property)
                || FILE_FILTER.equals(property)
                || PATH_FILTER.equals(property)
                || MIN_AGE.equals(property)
                || MAX_AGE.equals(property)
                || MIN_SIZE.equals(property)
                || MAX_SIZE.equals(property)
                || IGNORE_HIDDEN_FILES.equals(property);
    }

    private List<FileInfo> scanDirectory(final File path, final FileFilter filter, final Boolean recurse,
                                         final Long minTimestamp) throws IOException {
        final List<FileInfo> listing = new ArrayList<>();
        File[] files = path.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    if (recurse) {
                        listing.addAll(scanDirectory(file, filter, true, minTimestamp));
                    }
                } else {
                    if ((minTimestamp == null || file.lastModified() >= minTimestamp) && filter.accept(file)) {
                        listing.add(new FileInfo.Builder()
                                .directory(file.isDirectory())
                                .filename(file.getName())
                                .fullPathFileName(file.getAbsolutePath())
                                .lastModifiedTime(file.lastModified())
                                .build());
                    }
                }
            }
        }

        return listing;
    }

    private FileFilter createFileFilter(final ProcessContext context) {
        final long minSize = context.getProperty(MIN_SIZE).asDataSize(DataUnit.B).longValue();
        final Double maxSize = context.getProperty(MAX_SIZE).asDataSize(DataUnit.B);
        final long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAge = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final boolean ignoreHidden = context.getProperty(IGNORE_HIDDEN_FILES).asBoolean();
        final Pattern filePattern = Pattern.compile(context.getProperty(FILE_FILTER).getValue());
        final String indir = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        final boolean recurseDirs = context.getProperty(RECURSE).asBoolean();
        final String pathPatternStr = context.getProperty(PATH_FILTER).getValue();
        final Pattern pathPattern = (!recurseDirs || pathPatternStr == null) ? null : Pattern.compile(pathPatternStr);

        return new FileFilter() {
            @Override
            public boolean accept(final File file) {
                if (minSize > file.length()) {
                    return false;
                }
                if (maxSize != null && maxSize < file.length()) {
                    return false;
                }
                final long fileAge = System.currentTimeMillis() - file.lastModified();
                if (minAge > fileAge) {
                    return false;
                }
                if (maxAge != null && maxAge < fileAge) {
                    return false;
                }
                if (ignoreHidden && file.isHidden()) {
                    return false;
                }
                if (pathPattern != null) {
                    Path reldir = Paths.get(indir).relativize(file.toPath()).getParent();
                    if (reldir != null && !reldir.toString().isEmpty()) {
                        if (!pathPattern.matcher(reldir.toString()).matches()) {
                            return false;
                        }
                    }
                }
                //Verify that we have at least read permissions on the file we're considering grabbing
                if (!Files.isReadable(file.toPath())) {
                    return false;
                }
                return filePattern.matcher(file.getName()).matches();
            }
        };
    }

}
