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
package org.apache.nifi.processors.hadoop.util.writer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.hadoop.util.FileStatusIterable;
import org.apache.nifi.processors.hadoop.util.FileStatusManager;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.util.List;

/**
 * Interface for common management of writing to records and to FlowFiles.
 */
public abstract class HadoopFileStatusWriter {

    protected final ProcessSession session;
    protected final Relationship successRelationship;
    protected final FileStatusIterable fileStatusIterable;
    protected final FileStatusManager fileStatusManager;
    protected final PathFilter pathFilter;
    protected final long minimumAge;
    protected final long maximumAge;
    protected final long previousLatestTimestamp;
    protected final List<String> previousLatestFiles;

    protected final RecordSetWriterFactory writerFactory;
    protected final String hdfsPrefix;
    protected final ComponentLog logger;
    protected final long currentTimeMillis;
    protected long fileCount;

    public HadoopFileStatusWriter(final ProcessSession session,
                                  final Relationship successRelationship,
                                  final FileStatusIterable fileStatusIterable,
                                  final FileStatusManager fileStatusManager,
                                  final PathFilter pathFilter,
                                  final long minimumAge,
                                  final long maximumAge,
                                  final long previousLatestTimestamp,
                                  final List<String> previousLatestFiles,
                                  final RecordSetWriterFactory writerFactory,
                                  final String hdfsPrefix,
                                  final ComponentLog logger) {
        this.session = session;
        this.successRelationship = successRelationship;
        this.fileStatusIterable = fileStatusIterable;
        this.fileStatusManager = fileStatusManager;
        this.pathFilter = pathFilter;
        this.minimumAge = minimumAge;
        this.maximumAge = maximumAge;
        this.previousLatestTimestamp = previousLatestTimestamp;
        this.previousLatestFiles = previousLatestFiles;
        this.writerFactory = writerFactory;
        this.hdfsPrefix = hdfsPrefix;
        this.logger = logger;
        currentTimeMillis = System.currentTimeMillis();
        fileCount = 0L;
    }

    public abstract void write();

    public long getListedFileCount() {
        return fileCount;
    }

    protected boolean determineListable(final FileStatus status) {

        final boolean isCopyInProgress = status.getPath().getName().endsWith("_COPYING_");
        final boolean isFilterAccepted = pathFilter.accept(status.getPath());
        if (isCopyInProgress || !isFilterAccepted) {
            return false;
        }
        // If the file was created during the processor's last iteration we have to check if it was already listed
        if (status.getModificationTime() == previousLatestTimestamp) {
            return !previousLatestFiles.contains(status.getPath().toString());
        }

        final long fileAge = currentTimeMillis - status.getModificationTime();
        if (minimumAge > fileAge || fileAge > maximumAge) {
            return false;
        }

        return status.getModificationTime() > previousLatestTimestamp;
    }

    String getAbsolutePath(final Path path) {
        final Path parent = path.getParent();
        final String prefix = (parent == null || parent.getName().equals("")) ? "" : getAbsolutePath(parent);
        return prefix + "/" + path.getName();
    }

    String getPerms(final FsAction action) {
        final StringBuilder sb = new StringBuilder();

        sb.append(action.implies(FsAction.READ) ? "r" : "-");
        sb.append(action.implies(FsAction.WRITE) ? "w" : "-");
        sb.append(action.implies(FsAction.EXECUTE) ? "x" : "-");

        return sb.toString();
    }

    String getPermissionsString(final FsPermission permission) {
        return String.format("%s%s%s", getPerms(permission.getUserAction()),
                getPerms(permission.getGroupAction()), getPerms(permission.getOtherAction()));
    }

    public static HadoopFileStatusWriter.Builder builder() {
        return new HadoopFileStatusWriter.Builder();
    }

    public static class Builder {
        private ProcessSession session;
        private FileStatusIterable fileStatusIterable;
        private long minimumAge;
        private long maximumAge;
        private PathFilter pathFilter;
        private FileStatusManager fileStatusManager;
        private long previousLatestTimestamp;
        private List<String> previousLatestFiles;
        private Relationship successRelationship;
        private RecordSetWriterFactory writerFactory;
        private ComponentLog logger;
        private String hdfsPrefix;

        public HadoopFileStatusWriter.Builder session(final ProcessSession session) {
            this.session = session;
            return this;
        }

        public HadoopFileStatusWriter.Builder fileStatusIterable(final FileStatusIterable fileStatusIterable) {
            this.fileStatusIterable = fileStatusIterable;
            return this;
        }

        public HadoopFileStatusWriter.Builder minimumAge(final long minimumAge) {
            this.minimumAge = minimumAge;
            return this;
        }

        public HadoopFileStatusWriter.Builder maximumAge(final long maximumAge) {
            this.maximumAge = maximumAge;
            return this;
        }

        public HadoopFileStatusWriter.Builder pathFilter(final PathFilter pathFilter) {
            this.pathFilter = pathFilter;
            return this;
        }

        public HadoopFileStatusWriter.Builder fileStatusManager(final FileStatusManager fileStatusManager) {
            this.fileStatusManager = fileStatusManager;
            return this;
        }

        public HadoopFileStatusWriter.Builder previousLatestTimestamp(final long previousLatestTimestamp) {
            this.previousLatestTimestamp = previousLatestTimestamp;
            return this;
        }

        public HadoopFileStatusWriter.Builder previousLatestFiles(final List<String> previousLatestFiles) {
            this.previousLatestFiles = previousLatestFiles;
            return this;
        }

        public HadoopFileStatusWriter.Builder successRelationship(final Relationship successRelationship) {
            this.successRelationship = successRelationship;
            return this;
        }

        public HadoopFileStatusWriter.Builder writerFactory(final RecordSetWriterFactory writerFactory) {
            this.writerFactory = writerFactory;
            return this;
        }

        public HadoopFileStatusWriter.Builder logger(final ComponentLog logger) {
            this.logger = logger;
            return this;
        }

        public HadoopFileStatusWriter.Builder hdfsPrefix(final String hdfsPrefix) {
            this.hdfsPrefix = hdfsPrefix;
            return this;
        }

        public HadoopFileStatusWriter build() {
            validateMandatoryField("session", session);
            validateMandatoryField("successRelationship", successRelationship);
            validateMandatoryField("fileStatusIterable", fileStatusIterable);
            validateMandatoryField("fileStatusManager", fileStatusManager);

            if (writerFactory == null) {
                return new FlowFileHadoopFileStatusWriter(session, successRelationship, fileStatusIterable, fileStatusManager, pathFilter, minimumAge, maximumAge,
                        previousLatestTimestamp, previousLatestFiles, writerFactory, hdfsPrefix, logger);
            } else {
                return new RecordHadoopFileStatusWriter(session, successRelationship, fileStatusIterable, fileStatusManager, pathFilter, minimumAge, maximumAge,
                        previousLatestTimestamp, previousLatestFiles, writerFactory, hdfsPrefix, logger);
            }
        }

        private void validateMandatoryField(String variableName, Object variable) {
            if (variable == null) {
                throw new IllegalArgumentException(variableName + " is null but must be set");
            }
        }
    }

    public ProcessSession getSession() {
        return session;
    }

    public Relationship getSuccessRelationship() {
        return successRelationship;
    }

    public FileStatusIterable getFileStatusIterable() {
        return fileStatusIterable;
    }

    public FileStatusManager getFileStatusManager() {
        return fileStatusManager;
    }

    public PathFilter getPathFilter() {
        return pathFilter;
    }

    public long getMinimumAge() {
        return minimumAge;
    }

    public long getMaximumAge() {
        return maximumAge;
    }

    public long getPreviousLatestTimestamp() {
        return previousLatestTimestamp;
    }

    public List<String> getPreviousLatestFiles() {
        return previousLatestFiles;
    }

    public RecordSetWriterFactory getWriterFactory() {
        return writerFactory;
    }

    public String getHdfsPrefix() {
        return hdfsPrefix;
    }

    public ComponentLog getLogger() {
        return logger;
    }
}
