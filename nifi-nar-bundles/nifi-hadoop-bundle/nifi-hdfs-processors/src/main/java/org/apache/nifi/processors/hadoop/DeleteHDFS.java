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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({ "hadoop", "HDFS", "delete", "remove", "filesystem" })
@CapabilityDescription("Deletes a file from HDFS. The file can be provided as an attribute from an incoming FlowFile, "
        + "or a statically set file that is periodically removed. If this processor has an incoming connection, it"
        + "will ignore running on a periodic basis and instead rely on incoming FlowFiles to trigger a delete. "
        + "Optionally, you may specify use a wildcard character to match multiple files or directories.")
public class DeleteHDFS extends AbstractHadoopProcessor {
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles will be routed here if the delete command was successful")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles will be routed here if the delete command was unsuccessful")
            .build();

    public static final PropertyDescriptor FILE_OR_DIRECTORY = new PropertyDescriptor.Builder()
            .name("file_or_directory")
            .displayName("File or Directory")
            .description("The HDFS file or directory to delete. A wildcard expression may be used to only delete certain files")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor RECURSIVE = new PropertyDescriptor.Builder()
            .name("recursive")
            .displayName("Recursive")
            .description("Remove contents of a non-empty directory recursively")
            .allowableValues("true", "false")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected final Pattern GLOB_PATTERN = Pattern.compile("\\[|\\]|\\*|\\?|\\^|\\{|\\}|\\\\c");
    protected final Matcher GLOB_MATCHER = GLOB_PATTERN.matcher("");

    private static final Set<Relationship> relationships;

    static {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>(properties);
        props.add(FILE_OR_DIRECTORY);
        props.add(RECURSIVE);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        String fileOrDirectoryName = null;
        FlowFile flowFile = session.get();

        // If this processor has an incoming connection, then do not run unless a
        // FlowFile is actually sent through
        if (flowFile == null && context.hasIncomingConnection()) {
            context.yield();
            return;
        }

        if (flowFile != null) {
            fileOrDirectoryName = context.getProperty(FILE_OR_DIRECTORY).evaluateAttributeExpressions(flowFile).getValue();
        } else {
            fileOrDirectoryName = context.getProperty(FILE_OR_DIRECTORY).evaluateAttributeExpressions().getValue();
        }

        final FileSystem fileSystem = getFileSystem();
        try {
            // Check if the user has supplied a file or directory pattern
            List<Path> pathList = Lists.newArrayList();
            if (GLOB_MATCHER.reset(fileOrDirectoryName).find()) {
                FileStatus[] fileStatuses = fileSystem.globStatus(new Path(fileOrDirectoryName));
                if (fileStatuses != null) {
                    for (FileStatus fileStatus : fileStatuses) {
                        pathList.add(fileStatus.getPath());
                    }
                }
            } else {
                pathList.add(new Path(fileOrDirectoryName));
            }

            Map<String, String> attributes = Maps.newHashMapWithExpectedSize(2);
            for (Path path : pathList) {
                attributes.put("filename", path.getName());
                attributes.put("path", path.getParent().toString());
                if (fileSystem.exists(path)) {
                    fileSystem.delete(path, context.getProperty(RECURSIVE).asBoolean());
                    if (!context.hasIncomingConnection()) {
                        flowFile = session.create();
                    }
                    session.transfer(session.putAllAttributes(flowFile, attributes), REL_SUCCESS);
                } else {
                    getLogger().warn("File (" + path + ") does not exist");
                    if (!context.hasIncomingConnection()) {
                        flowFile = session.create();
                    }
                    session.transfer(session.putAllAttributes(flowFile, attributes), REL_FAILURE);
                }
            }
        } catch (IOException e) {
            getLogger().warn("Error processing delete for file or directory", e);
            if (flowFile != null) {
                session.rollback(true);
            }
        }
    }

}
