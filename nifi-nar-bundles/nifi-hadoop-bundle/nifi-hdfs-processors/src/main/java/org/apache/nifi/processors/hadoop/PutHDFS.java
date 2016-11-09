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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This processor copies FlowFiles to HDFS.
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "HDFS", "put", "copy", "filesystem"})
@CapabilityDescription("Write FlowFile data to Hadoop Distributed File System (HDFS)")
@ReadsAttribute(attribute = "filename", description = "The name of the file written to HDFS comes from the value of this attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file written to HDFS is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "The absolute path to the file on HDFS is stored in this attribute.")
})
@SeeAlso(GetHDFS.class)
public class PutHDFS extends AbstractHadoopProcessor {

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";
    public static final String APPEND_RESOLUTION = "append";

    public static final AllowableValue REPLACE_RESOLUTION_AV = new AllowableValue(REPLACE_RESOLUTION,
            REPLACE_RESOLUTION, "Replaces the existing file if any.");
    public static final AllowableValue IGNORE_RESOLUTION_AV = new AllowableValue(IGNORE_RESOLUTION, IGNORE_RESOLUTION,
            "Ignores the flow file and routes it to success.");
    public static final AllowableValue FAIL_RESOLUTION_AV = new AllowableValue(FAIL_RESOLUTION, FAIL_RESOLUTION,
            "Penalizes the flow file and routes it to failure.");
    public static final AllowableValue APPEND_RESOLUTION_AV = new AllowableValue(APPEND_RESOLUTION, APPEND_RESOLUTION,
            "Appends to the existing file if any, creates a new file otherwise.");

    public static final String BUFFER_SIZE_KEY = "io.file.buffer.size";
    public static final int BUFFER_SIZE_DEFAULT = 4096;

    public static final String ABSOLUTE_HDFS_PATH_ATTRIBUTE = "absolute.hdfs.path";

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to HDFS are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "Files that could not be written to HDFS for some reason are transferred to this relationship")
            .build();

    // properties

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
.defaultValue(FAIL_RESOLUTION_AV.getValue())
            .allowableValues(REPLACE_RESOLUTION_AV, IGNORE_RESOLUTION_AV, FAIL_RESOLUTION_AV, APPEND_RESOLUTION_AV)
            .build();

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
            .addValidator(createPositiveShortValidator())
            .build();

    public static final PropertyDescriptor UMASK = new PropertyDescriptor.Builder()
            .name("Permissions umask")
            .description(
                    "A umask represented as an octal number which determines the permissions of files written to HDFS. This overrides the Hadoop Configuration dfs.umaskmode")
            .addValidator(createUmaskValidator())
            .build();

    public static final PropertyDescriptor REMOTE_OWNER = new PropertyDescriptor.Builder()
            .name("Remote Owner")
            .description(
                    "Changes the owner of the HDFS file to this value after it is written. This only works if NiFi is running as a user that has HDFS super user privilege to change owner")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOTE_GROUP = new PropertyDescriptor.Builder()
            .name("Remote Group")
            .description(
                    "Changes the group of the HDFS file to this value after it is written. This only works if NiFi is running as a user that has HDFS super user privilege to change group")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
                .description("The parent HDFS directory to which files should be written")
                .build());
        props.add(CONFLICT_RESOLUTION);
        props.add(BLOCK_SIZE);
        props.add(BUFFER_SIZE);
        props.add(REPLICATION_FACTOR);
        props.add(UMASK);
        props.add(REMOTE_OWNER);
        props.add(REMOTE_GROUP);
        props.add(COMPRESSION_CODEC);
        return props;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws Exception {
        super.abstractOnScheduled(context);

        // Set umask once, to avoid thread safety issues doing it in onTrigger
        final PropertyValue umaskProp = context.getProperty(UMASK);
        final short dfsUmask;
        if (umaskProp.isSet()) {
            dfsUmask = Short.parseShort(umaskProp.getValue(), 8);
        } else {
            dfsUmask = FsPermission.DEFAULT_UMASK;
        }
        final Configuration conf = getConfiguration();
        FsPermission.setUMask(conf, new FsPermission(dfsUmask));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final FileSystem hdfs = getFileSystem();
        final Configuration configuration = getConfiguration();
        final UserGroupInformation ugi = getUserGroupInformation();

        if (configuration == null || hdfs == null || ugi == null) {
            getLogger().error("HDFS not configured properly");
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
            return;
        }

        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                Path tempDotCopyFile = null;
                FlowFile putFlowFile = flowFile;
                try {
                    final String dirValue = context.getProperty(DIRECTORY).evaluateAttributeExpressions(putFlowFile).getValue();
                    final Path configuredRootDirPath = new Path(dirValue);

                    final String conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();

                    final Double blockSizeProp = context.getProperty(BLOCK_SIZE).asDataSize(DataUnit.B);
                    final long blockSize = blockSizeProp != null ? blockSizeProp.longValue() : hdfs.getDefaultBlockSize(configuredRootDirPath);

                    final Double bufferSizeProp = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B);
                    final int bufferSize = bufferSizeProp != null ? bufferSizeProp.intValue() : configuration.getInt(BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);

                    final Integer replicationProp = context.getProperty(REPLICATION_FACTOR).asInteger();
                    final short replication = replicationProp != null ? replicationProp.shortValue() : hdfs
                            .getDefaultReplication(configuredRootDirPath);

                    final CompressionCodec codec = getCompressionCodec(context, configuration);

                    final String filename = codec != null
                            ? putFlowFile.getAttribute(CoreAttributes.FILENAME.key()) + codec.getDefaultExtension()
                            : putFlowFile.getAttribute(CoreAttributes.FILENAME.key());

                    final Path tempCopyFile = new Path(configuredRootDirPath, "." + filename);
                    final Path copyFile = new Path(configuredRootDirPath, filename);

                    // Create destination directory if it does not exist
                    try {
                        if (!hdfs.getFileStatus(configuredRootDirPath).isDirectory()) {
                            throw new IOException(configuredRootDirPath.toString() + " already exists and is not a directory");
                        }
                    } catch (FileNotFoundException fe) {
                        if (!hdfs.mkdirs(configuredRootDirPath)) {
                            throw new IOException(configuredRootDirPath.toString() + " could not be created");
                        }
                        changeOwner(context, hdfs, configuredRootDirPath);
                    }

                    final boolean destinationExists = hdfs.exists(copyFile);

                    // If destination file already exists, resolve that based on processor configuration
                    if (destinationExists) {
                        switch (conflictResponse) {
                        case REPLACE_RESOLUTION:
                                if (hdfs.delete(copyFile, false)) {
                                    getLogger().info("deleted {} in order to replace with the contents of {}",
                                            new Object[]{copyFile, putFlowFile});
                                }
                                break;
                        case IGNORE_RESOLUTION:
                                session.transfer(putFlowFile, REL_SUCCESS);
                                getLogger().info("transferring {} to success because file with same name already exists",
                                        new Object[]{putFlowFile});
                                return null;
                        case FAIL_RESOLUTION:
                                session.transfer(session.penalize(putFlowFile), REL_FAILURE);
                                getLogger().warn("penalizing {} and routing to failure because file with same name already exists",
                                        new Object[]{putFlowFile});
                                return null;
                            default:
                                break;
                        }
                    }

                    // Write FlowFile to temp file on HDFS
                    final StopWatch stopWatch = new StopWatch(true);
                    session.read(putFlowFile, new InputStreamCallback() {

                        @Override
                        public void process(InputStream in) throws IOException {
                            OutputStream fos = null;
                            Path createdFile = null;
                            try {
                                if (conflictResponse.equals(APPEND_RESOLUTION_AV.getValue()) && destinationExists) {
                                    fos = hdfs.append(copyFile, bufferSize);
                                } else {
                                    fos = hdfs.create(tempCopyFile, true, bufferSize, replication, blockSize);
                                }
                                if (codec != null) {
                                    fos = codec.createOutputStream(fos);
                                }
                                createdFile = tempCopyFile;
                                BufferedInputStream bis = new BufferedInputStream(in);
                                StreamUtils.copy(bis, fos);
                                bis = null;
                                fos.flush();
                            } finally {
                                try {
                                    if (fos != null) {
                                        fos.close();
                                    }
                                } catch (RemoteException re) {
                                    // when talking to remote HDFS clusters, we don't notice problems until fos.close()
                                    if (createdFile != null) {
                                        try {
                                            hdfs.delete(createdFile, false);
                                        } catch (Throwable ignore) {
                                        }
                                    }
                                    throw re;
                                } catch (Throwable ignore) {
                                }
                                fos = null;
                            }
                        }

                    });
                    stopWatch.stop();
                    final String dataRate = stopWatch.calculateDataRate(putFlowFile.getSize());
                    final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                    tempDotCopyFile = tempCopyFile;

                    if (!conflictResponse.equals(APPEND_RESOLUTION_AV.getValue())
                            || (conflictResponse.equals(APPEND_RESOLUTION_AV.getValue()) && !destinationExists)) {
                        boolean renamed = false;
                        for (int i = 0; i < 10; i++) { // try to rename multiple times.
                            if (hdfs.rename(tempCopyFile, copyFile)) {
                                renamed = true;
                                break;// rename was successful
                            }
                            Thread.sleep(200L);// try waiting to let whatever might cause rename failure to resolve
                        }
                        if (!renamed) {
                            hdfs.delete(tempCopyFile, false);
                            throw new ProcessException("Copied file to HDFS but could not rename dot file " + tempCopyFile
                                    + " to its final filename");
                        }

                        changeOwner(context, hdfs, copyFile);
                    }

                    getLogger().info("copied {} to HDFS at {} in {} milliseconds at a rate of {}",
                            new Object[]{putFlowFile, copyFile, millis, dataRate});

                    final String outputPath = copyFile.toString();
                    final String newFilename = copyFile.getName();
                    final String hdfsPath = copyFile.getParent().toString();
                    putFlowFile = session.putAttribute(putFlowFile, CoreAttributes.FILENAME.key(), newFilename);
                    putFlowFile = session.putAttribute(putFlowFile, ABSOLUTE_HDFS_PATH_ATTRIBUTE, hdfsPath);
                    final String transitUri = (outputPath.startsWith("/")) ? "hdfs:/" + outputPath : "hdfs://" + outputPath;
                    session.getProvenanceReporter().send(putFlowFile, transitUri);

                    session.transfer(putFlowFile, REL_SUCCESS);

                } catch (final Throwable t) {
                    if (tempDotCopyFile != null) {
                        try {
                            hdfs.delete(tempDotCopyFile, false);
                        } catch (Exception e) {
                            getLogger().error("Unable to remove temporary file {} due to {}", new Object[]{tempDotCopyFile, e});
                        }
                    }
                    getLogger().error("Failed to write to HDFS due to {}", new Object[]{t});
                    session.transfer(session.penalize(putFlowFile), REL_FAILURE);
                    context.yield();
                }

                return null;
            }
        });
    }

    protected void changeOwner(final ProcessContext context, final FileSystem hdfs, final Path name) {
        try {
            // Change owner and group of file if configured to do so
            String owner = context.getProperty(REMOTE_OWNER).getValue();
            String group = context.getProperty(REMOTE_GROUP).getValue();
            if (owner != null || group != null) {
                hdfs.setOwner(name, owner, group);
            }
        } catch (Exception e) {
            getLogger().warn("Could not change owner or group of {} on HDFS due to {}", new Object[]{name, e});
        }
    }

    /*
     * Validates that a property is a valid short number greater than 0.
     */
    static Validator createPositiveShortValidator() {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
                String reason = null;
                try {
                    final short shortVal = Short.parseShort(value);
                    if (shortVal <= 0) {
                        reason = "short integer must be greater than zero";
                    }
                } catch (final NumberFormatException e) {
                    reason = "[" + value + "] is not a valid short integer";
                }
                return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null)
                        .build();
            }
        };
    }

    /*
     * Validates that a property is a valid umask, i.e. a short octal number that is not negative.
     */
    static Validator createUmaskValidator() {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
                String reason = null;
                try {
                    final short shortVal = Short.parseShort(value, 8);
                    if (shortVal < 0) {
                        reason = "octal umask [" + value + "] cannot be negative";
                    } else if (shortVal > 511) {
                        // HDFS umask has 9 bits: rwxrwxrwx ; the sticky bit cannot be umasked
                        reason = "octal umask [" + value + "] is not a valid umask";
                    }
                } catch (final NumberFormatException e) {
                    reason = "[" + value + "] is not a valid short octal number";
                }
                return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null)
                        .build();
            }
        };
    }

}
