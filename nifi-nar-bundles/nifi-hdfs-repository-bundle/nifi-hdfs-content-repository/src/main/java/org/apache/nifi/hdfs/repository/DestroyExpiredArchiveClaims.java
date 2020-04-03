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
package org.apache.nifi.hdfs.repository;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.nifi.controller.repository.ContentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DestroyExpiredArchiveClaims implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DestroyExpiredArchiveClaims.class);
    private final Container container;
    private final ContentRepository repository;
    private final long maxArchiveMillis;
    private volatile long oldestArchiveDate = 0;

    public DestroyExpiredArchiveClaims(ContentRepository repository, Container container, long maxArchiveMillis) {
        this.container = container;
        this.repository = repository;
        this.maxArchiveMillis = maxArchiveMillis;
    }

    @Override
    public void run() {
        Thread thread = Thread.currentThread();
        String origName = thread.getName();
        try {
            if (oldestArchiveDate > System.currentTimeMillis() - maxArchiveMillis) {
                try {
                    long usableSpace = repository.getContainerUsableSpace(container.getName());
                    if (usableSpace > container.getMinUsableSpaceForArchive()) {
                        return;
                    }
                } catch (Exception ex) {
                    LOG.error("Failed to determine space available in container {}; will attempt to cleanup archive", container.getName());
                }
            }

            thread.setName("Cleanup Archive for " + container.getName());

            try {
                oldestArchiveDate = destroyExpiredArchives();
            } catch (IOException ex) {
                container.failureOcurred();
                LOG.error("Failed to cleanup archive for container {} due to {}", container.getName(), ex.toString());
                if (LOG.isDebugEnabled()) {
                    LOG.error("", ex);
                }
            } finally {
                thread.setName(origName);
            }
        } catch (final Throwable t) {
            LOG.error("Failed to cleanup archive for container " + container.getName() + " due to " + t.toString(), t);
        }
    }

    protected long destroyExpiredArchives() throws IOException {
        long duration = System.currentTimeMillis();

        FileSystem fs = container.getFileSystem();
        Path path = container.getPath();
        FileStatus[] sections = fs.listStatus(path);

        if (sections == null) {
            LOG.warn("No section found for container " + container.getName());
            return 0;
        }

        long usableSpace = repository.getContainerUsableSpace(container.getName());
        long toFree = container.getMinUsableSpaceForArchive() - usableSpace;
        if (LOG.isDebugEnabled()) {
            if (toFree < 0) {
                LOG.debug("Currently {} bytes free for Container {}; requirement is {} byte free, so no need to free space until an additional {} bytes are used",
                        usableSpace, container.getName(), container.getMinUsableSpaceForArchive(), -1 * toFree);
            } else {
                LOG.debug("Currently {} bytes free for Container {}; requirement is {} byte free, so need to free {} bytes",
                        usableSpace, container.getName(), container.getMinUsableSpaceForArchive(), toFree);
            }
        }

        long removalTimeThreshold = System.currentTimeMillis() - maxArchiveMillis;

        long originalToFree = toFree;

        int agedOff = 0;

        NavigableSet<FileInfo> oldestFiles = new TreeSet<>();
        long oldestSize = 0;

        // go through each section's archive and remove expires files
        // also, keep track of the oldest files that are not yet expired
        // so we can delete those that are over the container size limit
        for (FileStatus section : sections) {
            Path archiveDir = new Path(section.getPath(), HdfsContentRepository.ARCHIVE_DIR_NAME);

            RemoteIterator<FileStatus> archivedFiles;
            try {
                archivedFiles = fs.listStatusIterator(archiveDir);
            } catch (FileNotFoundException ex) {
                // this shouldn't really happen - an empty archive
                // directory should have been created at startup
                continue;
            }

            while (archivedFiles.hasNext()) {
                FileStatus file = archivedFiles.next();
                if (file.getModificationTime() < removalTimeThreshold) {
                    if (!fs.delete(file.getPath(), false)) {
                        LOG.warn("Failed to remove archived ContentClaim: " + file.getPath());
                    } else {
                        agedOff++;
                        toFree -= file.getLen();
                    }
                } else {
                    oldestFiles.add(new FileInfo(file));
                    oldestSize += file.getLen();

                    // remove extra files over the toFree limit so we don't blow out memory
                    while (oldestFiles.size() > 1 && (oldestSize - oldestFiles.last().getSize()) > toFree) {
                        oldestSize -= oldestFiles.pollLast().getSize();
                    }
                }
            }
        }

        int overSizeDeleted = oldestFiles.size();

        long oldestRemainingFile = 0;

        // in case we remove all of the oldest files, mark the 'oldest'
        // remaining as the newest one in the set. This is obviously wrong
        // but it shouldn't matter since this is for optimization purposes only
        if (oldestFiles.size() > 0) {
            oldestRemainingFile = oldestFiles.last().getTime();
        }

        // delete the oldest files first until we don't
        // have to free any more space in the container
        while (toFree > 0 && oldestFiles.size() > 0) {
            FileInfo file = oldestFiles.pollFirst();
            if (!fs.delete(file.getPath(), false)) {
                LOG.warn("Failed to remove archived ContentClaim: " + file.getPath());
            } else {
                toFree -= file.getSize();
            }
        }

        if (oldestFiles.size() > 0) {
            oldestRemainingFile = oldestFiles.first().getTime();
        }

        overSizeDeleted = overSizeDeleted - oldestFiles.size();

        long spaceFreed = originalToFree - toFree;
        duration = System.currentTimeMillis() - duration;

        LOG.debug("Aged off " + agedOff + " files and removed an additional " + overSizeDeleted + " files. Total space free: " + spaceFreed + " in " + duration + " ms");

        return oldestRemainingFile;
    }

    protected static class FileInfo implements Comparable<FileInfo> {
        private final Path path;
        private final long size;
        private final long time;

        public FileInfo(FileStatus status) {
            this.path = status.getPath();
            this.size = status.getLen();
            this.time = status.getModificationTime();
        }

        public long getSize() {
            return size;
        }
        public Path getPath() {
            return path;
        }
        public long getTime() {
            return time;
        }

        @Override
        public int compareTo(FileInfo other) {
            int diff = Long.compare(time, other.time);
            if (diff != 0) {
                return diff;
            }
            diff = Long.compare(size, other.size);
            if (diff != 0) {
                return diff;
            }
            return path.compareTo(other.path);
        }

    }

}
