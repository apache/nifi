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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.hadoop.ListHDFS;
import org.apache.nifi.processors.hadoop.util.FileStatusIterable;
import org.apache.nifi.processors.hadoop.util.FileStatusManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlowFileObjectWriter implements HdfsObjectWriter {

    private static final String HDFS_ATTRIBUTE_PREFIX = "hdfs";

    private final ProcessSession session;
    private final FileStatusIterable fileStatuses;
    final long minimumAge;
    final long maximumAge;
    final PathFilter pathFilter;
    final FileStatusManager fileStatusManager;
    final long latestModificationTime;
    final List<String> latestModifiedStatuses;
    private long listedFileCount;


    public FlowFileObjectWriter(final ProcessSession session, final FileStatusIterable fileStatuses, final long minimumAge,
                                final long maximumAge, final PathFilter pathFilter, final FileStatusManager fileStatusManager,
                                final long latestModificationTime, final List<String> latestModifiedStatuses) {
        this.session = session;
        this.fileStatuses = fileStatuses;
        this.minimumAge = minimumAge;
        this.maximumAge = maximumAge;
        this.pathFilter = pathFilter;
        this.fileStatusManager = fileStatusManager;
        this.latestModificationTime = latestModificationTime;
        this.latestModifiedStatuses = latestModifiedStatuses;
        listedFileCount = 0;
    }

    @Override
    public void write() {
        for (FileStatus status : fileStatuses) {
            if (determineListable(status, minimumAge, maximumAge, pathFilter, latestModificationTime, latestModifiedStatuses)) {

                final Map<String, String> attributes = createAttributes(status);
                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, ListHDFS.REL_SUCCESS);

                fileStatusManager.update(status);
                listedFileCount++;
            }
        }
    }

    public long getListedFileCount() {
        return listedFileCount;
    }

    private Map<String, String> createAttributes(final FileStatus status) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), status.getPath().getName());
        attributes.put(CoreAttributes.PATH.key(), getAbsolutePath(status.getPath().getParent()));
        attributes.put(HDFS_ATTRIBUTE_PREFIX + ".owner", status.getOwner());
        attributes.put(HDFS_ATTRIBUTE_PREFIX + ".group", status.getGroup());
        attributes.put(HDFS_ATTRIBUTE_PREFIX + ".lastModified", String.valueOf(status.getModificationTime()));
        attributes.put(HDFS_ATTRIBUTE_PREFIX + ".length", String.valueOf(status.getLen()));
        attributes.put(HDFS_ATTRIBUTE_PREFIX + ".replication", String.valueOf(status.getReplication()));
        attributes.put(HDFS_ATTRIBUTE_PREFIX + ".permissions", getPermissionsString(status.getPermission()));
        return attributes;
    }

    private String getPermissionsString(final FsPermission permission) {
        return String.format("%s%s%s", getPerms(permission.getUserAction()),
                getPerms(permission.getGroupAction()), getPerms(permission.getOtherAction()));
    }

    private String getPerms(final FsAction action) {
        final StringBuilder sb = new StringBuilder();

        sb.append(action.implies(FsAction.READ) ? "r" : "-");
        sb.append(action.implies(FsAction.WRITE) ? "w" : "-");
        sb.append(action.implies(FsAction.EXECUTE) ? "x" : "-");

        return sb.toString();
    }

    private String getAbsolutePath(final Path path) {
        final Path parent = path.getParent();
        final String prefix = (parent == null || parent.getName().equals("")) ? "" : getAbsolutePath(parent);
        return prefix + "/" + path.getName();
    }
}
