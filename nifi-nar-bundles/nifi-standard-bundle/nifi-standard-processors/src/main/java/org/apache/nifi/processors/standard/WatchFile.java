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
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.watch.AbstractWatchEntities;
import org.apache.nifi.processor.util.watch.ListedEntity;
import org.apache.nifi.processors.standard.util.FileInfo;
import org.apache.nifi.processors.standard.util.FileInfoFilter;
import org.apache.nifi.processors.standard.util.ListFileUtil;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.standard.util.ListFileUtil.scanDirectory;

@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
// TODO: doc
public class WatchFile extends AbstractWatchEntities {

    private static final AllowableValue LOCATION_LOCAL = new AllowableValue("Local", "Local",
            "Target Directory is located on a local disk. Entities are tracked on each node separately in the cluster.");
    private static final AllowableValue LOCATION_REMOTE = new AllowableValue("Remote", "Remote",
            "Target Directory is located on a remote system. Entities are tracked across the cluster so that" +
                    " the listing can be performed on Primary Node Only and another node can pick up where the last node left off," +
                    " if the Primary Node changes");

    private static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("target-directory")
            .displayName("Target Directory")
            .description("The directory to watch files.")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
            .name("recurse-subdirectories")
            .displayName("Recurse Subdirectories")
            .description("Indicates whether to watch files from subdirectories of the directory.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    private static final PropertyDescriptor DIRECTORY_LOCATION = new PropertyDescriptor.Builder()
            .name("target-directory-location")
            .displayName("Target Directory Location")
            .description("Specifies where the Target Directory is located." +
                    " This is used to determine whether listed files should be tracked locally or across the cluster.")
            .allowableValues(LOCATION_LOCAL, LOCATION_REMOTE)
            .defaultValue(LOCATION_LOCAL.getValue())
            .required(true)
            .build();

    private static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("file-filter")
            .displayName("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue("[^\\.].*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    private static final PropertyDescriptor PATH_FILTER = new PropertyDescriptor.Builder()
            .name("path-filter")
            .displayName("Path Filter")
            .description("When " + RECURSE.getName() + " is true, then only subdirectories whose path matches the given regular expression will be scanned")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    private static final PropertyDescriptor INCLUDE_FILE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("include-file-attributes")
            .displayName("Include File Attributes")
            .description("Whether or not to include information such as the file's Last Modified Time and Owner as FlowFile Attributes." +
                    " Depending on the File System being used, gathering this information can be expensive and as a result should be disabled." +
                    " This is especially true of remote file shares.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    private static final PropertyDescriptor IGNORE_HIDDEN_FILES = new PropertyDescriptor.Builder()
            .name("ignore-hidden-files")
            .displayName("Ignore Hidden Files")
            .description("Indicates whether or not hidden files should be ignored")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Arrays.asList(
            DIRECTORY, RECURSE, DIRECTORY_LOCATION, FILE_FILTER, PATH_FILTER, INCLUDE_FILE_ATTRIBUTES, IGNORE_HIDDEN_FILES
    );


    @Override
    protected List<PropertyDescriptor> getSupportedProperties() {
        return PROPERTIES;
    }

    @Override
    protected Map<String, ListedEntity> performListing(ProcessContext context, long minTimestampToList) {
        final String path = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        final File targetDir = new File(path);
        final Boolean recurse = context.getProperty(RECURSE).asBoolean();
        final Boolean includeFileAttributes = context.getProperty(INCLUDE_FILE_ATTRIBUTES).asBoolean();
        final List<FileInfo> fileInfos = scanDirectory(targetDir, createFileFilter(context), recurse, minTimestampToList);

        return fileInfos.stream().collect(Collectors.toMap(FileInfo::getIdentifier,
                f -> new ListedFileInfo(f.getTimestamp(), f.getSize(), path, f, includeFileAttributes)));

    }

    private class ListedFileInfo extends ListedEntity {

        private final String dirPath;
        private final FileInfo fileInfo;
        private final boolean includeFileAttributes;

        private ListedFileInfo(long lastModifiedTimestamp, long size, String dirPath, FileInfo fileInfo, boolean includeFileAttributes) {
            super(lastModifiedTimestamp, size);
            this.dirPath = dirPath;
            this.fileInfo = fileInfo;
            this.includeFileAttributes = includeFileAttributes;
        }

        @Override
        public Map<String, String> createAttributes() {
            return ListFileUtil.createAttributes(dirPath, fileInfo, includeFileAttributes, getLogger());
        }
    }

    private FileInfoFilter createFileFilter(final ProcessContext context) {
        final boolean ignoreHidden = context.getProperty(IGNORE_HIDDEN_FILES).asBoolean();
        final Pattern filePattern = Pattern.compile(context.getProperty(FILE_FILTER).getValue());
        final String indir = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        final boolean recurseDirs = context.getProperty(RECURSE).asBoolean();
        final String pathPatternStr = context.getProperty(PATH_FILTER).getValue();
        final Pattern pathPattern = (!recurseDirs || pathPatternStr == null) ? null : Pattern.compile(pathPatternStr);

        return (file, info) -> {
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
        };
    }

}
