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
package org.apache.nifi.processors.hadoop.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class FileStatusIterable implements Iterable<FileStatus> {

    private final Path path;
    private final boolean recursive;
    private final FileSystem hdfs;
    private final UserGroupInformation userGroupInformation;
    private final AtomicLong totalFileCount = new AtomicLong();

    public FileStatusIterable(final Path path, final boolean recursive, final FileSystem hdfs, final UserGroupInformation userGroupInformation) {
        this.path = path;
        this.recursive = recursive;
        this.hdfs = hdfs;
        this.userGroupInformation = userGroupInformation;
    }

    @Override
    public Iterator<FileStatus> iterator() {
        return new FileStatusIterator(path, recursive, hdfs, userGroupInformation, totalFileCount);
    }

    public long getTotalFileCount() {
        return totalFileCount.get();
    }
}
