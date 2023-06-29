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
import org.apache.nifi.processors.hadoop.util.FileStatusIterable;
import org.apache.nifi.processors.hadoop.util.FileStatusManager;

import java.util.List;

/**
 * Interface for common management of writing to records and to flowfiles.
 */
public abstract class HdfsObjectWriter {

    protected final ProcessSession session;
    protected final FileStatusIterable fileStatuses;
    final long minimumAge;
    final long maximumAge;
    final PathFilter pathFilter;
    final FileStatusManager fileStatusManager;
    final long latestModificationTime;
    final List<String> latestModifiedStatuses;
    final long currentTimeMillis;
    long fileCount;


    HdfsObjectWriter(ProcessSession session, FileStatusIterable fileStatuses, long minimumAge, long maximumAge, PathFilter pathFilter,
                     FileStatusManager fileStatusManager, long latestModificationTime, List<String> latestModifiedStatuses) {
        this.session = session;
        this.fileStatuses = fileStatuses;
        this.minimumAge = minimumAge;
        this.maximumAge = maximumAge;
        this.pathFilter = pathFilter;
        this.fileStatusManager = fileStatusManager;
        this.latestModificationTime = latestModificationTime;
        this.latestModifiedStatuses = latestModifiedStatuses;
        currentTimeMillis = System.currentTimeMillis();
        fileCount = 0L;
    }

    public abstract void write();

    public long getListedFileCount() {
        return fileCount;
    }

    boolean determineListable(final FileStatus status, final long minimumAge, final long maximumAge, final PathFilter filter,
                                      final long latestModificationTime, final List<String> latestModifiedStatuses) {

        final boolean isCopyInProgress = status.getPath().getName().endsWith("_COPYING_");
        final boolean isFilterAccepted = filter.accept(status.getPath());
        if (isCopyInProgress || !isFilterAccepted) {
            return false;
        }
        // If the file was created during the processor's last iteration we have to check if it was already listed
        if (status.getModificationTime() == latestModificationTime) {
            return !latestModifiedStatuses.contains(status.getPath().toString());
        }

        final long fileAge = currentTimeMillis - status.getModificationTime();
        if (minimumAge > fileAge || fileAge > maximumAge) {
            return false;
        }

        return status.getModificationTime() > latestModificationTime;
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
