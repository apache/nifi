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
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
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
        return new FileStatusIterator();
    }

    public long getTotalFileCount() {
        return totalFileCount.get();
    }

    class FileStatusIterator implements Iterator<FileStatus> {

        private static final String IO_ERROR_MESSAGE = "IO error occurred while iterating HFDS";

        private final Deque<Path> dirStatuses;

        private FileStatus nextFileStatus;
        private RemoteIterator<FileStatus> hdfsIterator;

        public FileStatusIterator() {
            dirStatuses = new ArrayDeque<>();
            hdfsIterator = getHdfsIterator(path);
        }

        @Override
        public boolean hasNext() {
            if (nextFileStatus != null) {
                return true;
            }
            try {
                while (hdfsIterator.hasNext() || !dirStatuses.isEmpty()) {
                    if (hdfsIterator.hasNext()) {
                        FileStatus fs = hdfsIterator.next();
                        if (fs.isDirectory()) {
                            if (recursive) {
                                dirStatuses.push(fs.getPath());
                            }
                            // if not recursive, continue
                        } else {
                            nextFileStatus = fs;
                            return true;
                        }
                    } else {
                        hdfsIterator = getHdfsIterator(dirStatuses.pop());
                    }
                }
                return false;
            } catch (IOException e) {
                throw new ProcessException(IO_ERROR_MESSAGE, e);
            }
        }

        @Override
        public FileStatus next() {
            if (nextFileStatus == null) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
            }
            totalFileCount.incrementAndGet();
            final FileStatus nextFileStatus = this.nextFileStatus;
            this.nextFileStatus = null;
            return nextFileStatus;
        }

        private RemoteIterator<FileStatus> getHdfsIterator(final Path hdfsPath) {
            try {
                return userGroupInformation.doAs((PrivilegedExceptionAction<RemoteIterator<FileStatus>>) () -> hdfs.listStatusIterator(hdfsPath));
            } catch (IOException e) {
                throw new ProcessException(IO_ERROR_MESSAGE, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException("Thread was interrupted while iterating HDFS", e);
            }
        }
    }
}
