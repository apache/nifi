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

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.ietf.jgss.GSSException;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class AbstractPutHDFS extends AbstractHadoopProcessor {
    protected static final String BUFFER_SIZE_KEY = "io.file.buffer.size";
    protected static final int BUFFER_SIZE_DEFAULT = 4096;

    protected static final String REPLACE_RESOLUTION = "replace";
    protected static final String IGNORE_RESOLUTION = "ignore";
    protected static final String FAIL_RESOLUTION = "fail";
    protected static final String APPEND_RESOLUTION = "append";

    protected static final AllowableValue REPLACE_RESOLUTION_AV = new AllowableValue(REPLACE_RESOLUTION,
            REPLACE_RESOLUTION, "Replaces the existing file if any.");
    protected static final AllowableValue IGNORE_RESOLUTION_AV = new AllowableValue(IGNORE_RESOLUTION, IGNORE_RESOLUTION,
            "Ignores the flow file and routes it to success.");
    protected static final AllowableValue FAIL_RESOLUTION_AV = new AllowableValue(FAIL_RESOLUTION, FAIL_RESOLUTION,
            "Penalizes the flow file and routes it to failure.");
    protected static final AllowableValue APPEND_RESOLUTION_AV = new AllowableValue(APPEND_RESOLUTION, APPEND_RESOLUTION,
            "Appends to the existing file if any, creates a new file otherwise.");

    protected static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION_AV.getValue())
            .allowableValues(REPLACE_RESOLUTION_AV, IGNORE_RESOLUTION_AV, FAIL_RESOLUTION_AV, APPEND_RESOLUTION_AV)
            .build();

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
            session.transfer(flowFile, getFailureRelationship());
            context.yield();
            return;
        }

        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                Path tempDotCopyFile = null;
                FlowFile putFlowFile = flowFile;
                try {
                    final Path dirPath = getNormalizedPath(context, DIRECTORY, putFlowFile);

                    final String conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();
                    final long blockSize = getBlockSize(context, session, putFlowFile, dirPath);
                    final int bufferSize = getBufferSize(context, session, putFlowFile);
                    final short replication = getReplication(context, session, putFlowFile, dirPath);

                    final CompressionCodec codec = getCompressionCodec(context, configuration);

                    final String filename = codec != null
                            ? putFlowFile.getAttribute(CoreAttributes.FILENAME.key()) + codec.getDefaultExtension()
                            : putFlowFile.getAttribute(CoreAttributes.FILENAME.key());

                    final Path tempCopyFile = new Path(dirPath, "." + filename);
                    final Path copyFile = new Path(dirPath, filename);

                    // Create destination directory if it does not exist
                    try {
                        if (!hdfs.getFileStatus(dirPath).isDirectory()) {
                            throw new IOException(dirPath.toString() + " already exists and is not a directory");
                        }
                    } catch (FileNotFoundException fe) {
                        if (!hdfs.mkdirs(dirPath)) {
                            throw new IOException(dirPath.toString() + " could not be created");
                        }
                        changeOwner(context, hdfs, dirPath, flowFile);
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
                                session.transfer(putFlowFile, getSuccessRelationship());
                                getLogger().info("transferring {} to success because file with same name already exists",
                                        new Object[]{putFlowFile});
                                return null;
                            case FAIL_RESOLUTION:
                                session.transfer(session.penalize(putFlowFile), getFailureRelationship());
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
                                if (conflictResponse.equals(APPEND_RESOLUTION) && destinationExists) {
                                    fos = hdfs.append(copyFile, bufferSize);
                                } else {
                                    final EnumSet<CreateFlag> cflags = EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);

                                    if (shouldIgnoreLocality(context, session)) {
                                        cflags.add(CreateFlag.IGNORE_CLIENT_LOCALITY);
                                    }

                                    fos = hdfs.create(tempCopyFile, FsCreateModes.applyUMask(FsPermission.getFileDefault(),
                                            FsPermission.getUMask(hdfs.getConf())), cflags, bufferSize, replication, blockSize,
                                            null, null);
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
                                } catch (Throwable t) {
                                    // when talking to remote HDFS clusters, we don't notice problems until fos.close()
                                    if (createdFile != null) {
                                        try {
                                            hdfs.delete(createdFile, false);
                                        } catch (Throwable ignore) {
                                        }
                                    }
                                    throw t;
                                }
                                fos = null;
                            }
                        }

                    });
                    stopWatch.stop();
                    final String dataRate = stopWatch.calculateDataRate(putFlowFile.getSize());
                    final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                    tempDotCopyFile = tempCopyFile;

                    if (!conflictResponse.equals(APPEND_RESOLUTION)
                            || (conflictResponse.equals(APPEND_RESOLUTION) && !destinationExists)) {
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

                        changeOwner(context, hdfs, copyFile, flowFile);
                    }

                    getLogger().info("copied {} to HDFS at {} in {} milliseconds at a rate of {}",
                            new Object[]{putFlowFile, copyFile, millis, dataRate});

                    final String newFilename = copyFile.getName();
                    final String hdfsPath = copyFile.getParent().toString();
                    putFlowFile = session.putAttribute(putFlowFile, CoreAttributes.FILENAME.key(), newFilename);
                    putFlowFile = session.putAttribute(putFlowFile, ABSOLUTE_HDFS_PATH_ATTRIBUTE, hdfsPath);
                    final Path qualifiedPath = copyFile.makeQualified(hdfs.getUri(), hdfs.getWorkingDirectory());
                    session.getProvenanceReporter().send(putFlowFile, qualifiedPath.toString());

                    session.transfer(putFlowFile, getSuccessRelationship());

                } catch (final IOException e) {
                    Optional<GSSException> causeOptional = findCause(e, GSSException.class, gsse -> GSSException.NO_CRED == gsse.getMajor());
                    if (causeOptional.isPresent()) {
                        getLogger().warn("An error occurred while connecting to HDFS. "
                                        + "Rolling back session, and penalizing flow file {}",
                                new Object[] {putFlowFile.getAttribute(CoreAttributes.UUID.key()), causeOptional.get()});
                        session.rollback(true);
                    } else {
                        getLogger().error("Failed to access HDFS due to {}", new Object[]{e});
                        session.transfer(putFlowFile, getFailureRelationship());
                    }
                } catch (final Throwable t) {
                    if (tempDotCopyFile != null) {
                        try {
                            hdfs.delete(tempDotCopyFile, false);
                        } catch (Exception e) {
                            getLogger().error("Unable to remove temporary file {} due to {}", new Object[]{tempDotCopyFile, e});
                        }
                    }
                    getLogger().error("Failed to write to HDFS due to {}", new Object[]{t});
                    session.transfer(session.penalize(putFlowFile), getFailureRelationship());
                    context.yield();
                }

                return null;
            }
        });
    }

    /**
     * Returns with the expected block size.
     */
    protected abstract long getBlockSize(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Path dirPath);

    /**
     * Returns with the expected buffer size.
     */
    protected abstract int getBufferSize(final ProcessContext context, final ProcessSession session, final FlowFile flowFile);

    /**
     * Returns with the expected replication factor.
     */
    protected abstract short getReplication(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Path dirPath);

    /**
     * Returns if file system should ignore locality.
     */
    protected abstract boolean shouldIgnoreLocality(final ProcessContext context, final ProcessSession session);

    /**
     * If returns a non-null value, the uploaded file's owner will be changed to this value after it is written. This only
     * works if NiFi is running as a user that has privilege to change owner.
     */
    protected abstract String getOwner(final ProcessContext context, final FlowFile flowFile);

    /**
     * I returns a non-null value, thee uploaded file's group will be changed to this value after it is written. This only
     * works if NiFi is running as a user that has privilege to change group.
     */
    protected abstract String getGroup(final ProcessContext context, final FlowFile flowFile);

    /**
     * @return The relationship the flow file will be transferred in case of successful execution.
     */
    protected abstract Relationship getSuccessRelationship();

    /**
     * @return The relationship the flow file will be transferred in case of failed execution.
     */
    protected abstract Relationship getFailureRelationship();

    /**
     * Returns an optional with the first throwable in the causal chain that is assignable to the provided cause type,
     * and satisfies the provided cause predicate, {@link Optional#empty()} otherwise.
     * @param t The throwable to inspect for the cause.
     * @return
     */
    private <T extends Throwable> Optional<T> findCause(Throwable t, Class<T> expectedCauseType, Predicate<T> causePredicate) {
        Stream<Throwable> causalChain = Throwables.getCausalChain(t).stream();
        return causalChain
                .filter(expectedCauseType::isInstance)
                .map(expectedCauseType::cast)
                .filter(causePredicate)
                .findFirst();
    }

    protected void changeOwner(final ProcessContext context, final FileSystem hdfs, final Path name, final FlowFile flowFile) {
        try {
            // Change owner and group of file if configured to do so
            final String owner = getOwner(context, flowFile);
            final String group = getGroup(context, flowFile);

            if (owner != null || group != null) {
                hdfs.setOwner(name, owner, group);
            }
        } catch (Exception e) {
            getLogger().warn("Could not change owner or group of {} on HDFS due to {}", new Object[]{name, e});
        }
    }
}
