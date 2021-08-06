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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This processor copies FlowFiles to HDFS.
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "HCFS", "HDFS", "put", "copy", "filesystem"})
@CapabilityDescription("Write FlowFile data to Hadoop Distributed File System (HDFS)")
@ReadsAttribute(attribute = "filename", description = "The name of the file written to HDFS comes from the value of this attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file written to HDFS is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "The absolute path to the file on HDFS is stored in this attribute.")
})
@SeeAlso(GetHDFS.class)
@Restricted(restrictions = {
    @Restriction(
        requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM,
        explanation = "Provides operator the ability to delete any file that NiFi has access to in HDFS or the local filesystem.")
})
public class PutHDFS extends AbstractPutHDFS {
    // relationships

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to HDFS are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to HDFS for some reason are transferred to this relationship")
            .build();

    // properties

    public static final PropertyDescriptor BLOCK_SIZE = new PropertyDescriptor.Builder()
            .name("Block Size")
            .description("Size of each block as written to HDFS. This overrides the Hadoop Configuration")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("IO Buffer Size")
            .description("Amount of memory to use to buffer file contents during IO. This overrides the Hadoop Configuration")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor REPLICATION_FACTOR = new PropertyDescriptor.Builder()
            .name("Replication")
            .description("Number of times that HDFS will replicate each file. This overrides the Hadoop Configuration")
            .addValidator(HadoopValidators.POSITIVE_SHORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor UMASK = new PropertyDescriptor.Builder()
            .name("Permissions umask")
            .description(
                   "A umask represented as an octal number which determines the permissions of files written to HDFS. " +
                           "This overrides the Hadoop property \"fs.permissions.umask-mode\".  " +
                           "If this property and \"fs.permissions.umask-mode\" are undefined, the Hadoop default \"022\" will be used.")
            .addValidator(HadoopValidators.UMASK_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOTE_OWNER = new PropertyDescriptor.Builder()
            .name("Remote Owner")
            .description(
                    "Changes the owner of the HDFS file to this value after it is written. This only works if NiFi is running as a user that has HDFS super user privilege to change owner")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor REMOTE_GROUP = new PropertyDescriptor.Builder()
            .name("Remote Group")
            .description(
                    "Changes the group of the HDFS file to this value after it is written. This only works if NiFi is running as a user that has HDFS super user privilege to change group")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IGNORE_LOCALITY = new PropertyDescriptor.Builder()
            .name("Ignore Locality")
            .displayName("Ignore Locality")
            .description(
                    "Directs the HDFS system to ignore locality rules so that data is distributed randomly throughout the cluster")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final Set<Relationship> relationships;

    static {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>(properties);
        props.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(DIRECTORY)
                .description("The parent HDFS directory to which files should be written. The directory will be created if it doesn't exist.")
                .build());
        props.add(CONFLICT_RESOLUTION);
        props.add(BLOCK_SIZE);
        props.add(BUFFER_SIZE);
        props.add(REPLICATION_FACTOR);
        props.add(UMASK);
        props.add(REMOTE_OWNER);
        props.add(REMOTE_GROUP);
        props.add(COMPRESSION_CODEC);
        props.add(IGNORE_LOCALITY);
        return props;
    }

    @Override
    protected void preProcessConfiguration(final Configuration config, final ProcessContext context) {
        // Set umask once, to avoid thread safety issues doing it in onTrigger
        final PropertyValue umaskProp = context.getProperty(UMASK);
        final short dfsUmask;
        if (umaskProp.isSet()) {
            dfsUmask = Short.parseShort(umaskProp.getValue(), 8);
        } else {
            dfsUmask = FsPermission.getUMask(config).toShort();
        }
        FsPermission.setUMask(config, new FsPermission(dfsUmask));
    }

    @Override
    protected Relationship getSuccessRelationship() {
        return REL_SUCCESS;
    }

    @Override
    protected Relationship getFailureRelationship() {
        return REL_FAILURE;
    }

    @Override
    protected long getBlockSize(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, Path dirPath) {
        final Double blockSizeProp = context.getProperty(BLOCK_SIZE).asDataSize(DataUnit.B);
        return blockSizeProp != null ? blockSizeProp.longValue() : getFileSystem().getDefaultBlockSize(dirPath);
    }

    @Override
    protected int getBufferSize(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        final Double bufferSizeProp = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B);
        return bufferSizeProp != null ? bufferSizeProp.intValue() : getConfiguration().getInt(BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);
    }

    @Override
    protected short getReplication(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, Path dirPath) {
        final Integer replicationProp = context.getProperty(REPLICATION_FACTOR).asInteger();
        return replicationProp != null ? replicationProp.shortValue() : getFileSystem()
                .getDefaultReplication(dirPath);
    }

    @Override
    protected boolean shouldIgnoreLocality(final ProcessContext context, final ProcessSession session) {
        return context.getProperty(IGNORE_LOCALITY).asBoolean();
    }

    @Override
    protected String getOwner(final ProcessContext context, final FlowFile flowFile) {
        final String owner = context.getProperty(REMOTE_OWNER).evaluateAttributeExpressions(flowFile).getValue();
        return owner == null || owner.isEmpty() ? null : owner;
    }

    @Override
    protected String getGroup(final ProcessContext context, final FlowFile flowFile) {
        final String group = context.getProperty(REMOTE_GROUP).evaluateAttributeExpressions(flowFile).getValue();
        return group == null || group.isEmpty() ? null : group;
    }
}
