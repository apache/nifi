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
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

public class FileStatusIterator implements Iterator<FileStatus> {

    private static final String IO_ERROR_MESSAGE = "IO error occured while iterating HFDS";

    private final boolean recursive;
    private final FileSystem hdfs;
    private final UserGroupInformation userGroupInformation;
    private final Deque<FileStatus> fileStatuses;
    private final Deque<FileStatus> dirStatuses;
    private final AtomicLong totalFileCount;

    public FileStatusIterator(final Path path, final boolean recursive, final FileSystem hdfs, final UserGroupInformation userGroupInformation,
                              final AtomicLong totalFileCount) {
        this.recursive = recursive;
        this.hdfs = hdfs;
        this.userGroupInformation = userGroupInformation;
        this.totalFileCount = totalFileCount;
        fileStatuses = new ArrayDeque<>();
        dirStatuses = new ArrayDeque<>();

        addFileStatuses(path);
    }

    @Override
    public boolean hasNext() {
        if (dirStatuses.isEmpty() && fileStatuses.isEmpty()) {
            return false;
        }
        while (recursive && fileStatuses.isEmpty() && !dirStatuses.isEmpty()) {
            final FileStatus dirStatus = dirStatuses.pop();
            addFileStatuses(dirStatus.getPath());
        }
        return !fileStatuses.isEmpty();
    }

    @Override
    public FileStatus next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        totalFileCount.incrementAndGet();
        return fileStatuses.pop();
    }

    private void addFileStatuses(Path path) {
        FileStatus[] statuses;
        try {
            statuses = userGroupInformation.doAs((PrivilegedExceptionAction<FileStatus[]>) () -> hdfs.listStatus(path));
        } catch (IOException e) {
            throw new ProcessException(IO_ERROR_MESSAGE, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessException("Thread was interrupted while iterating HDFS", e);
        }
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                dirStatuses.push(status);
            } else {
                fileStatuses.push(status);
            }
        }
    }
}

