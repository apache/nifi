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
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.hadoop.util.FileStatusIterable;
import org.apache.nifi.processors.hadoop.util.FileStatusManager;

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
    private final long currentTimeMillis;
    protected long fileCount;


    HadoopFileStatusWriter(final HadoopWriterContext hadoopWriterContext) {
        this.session = hadoopWriterContext.getSession();
        this.successRelationship = hadoopWriterContext.getSuccessRelationship();
        this.fileStatusIterable = hadoopWriterContext.getFileStatusIterable();
        this.fileStatusManager = hadoopWriterContext.getFileStatusManager();
        this.pathFilter = hadoopWriterContext.getPathFilter();
        this.minimumAge = hadoopWriterContext.getMinimumAge();
        this.maximumAge = hadoopWriterContext.getMaximumAge();
        this.previousLatestTimestamp = hadoopWriterContext.getPreviousLatestTimestamp();
        this.previousLatestFiles = hadoopWriterContext.getPreviousLatestFiles();
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
}
