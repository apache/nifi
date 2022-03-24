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
package org.apache.nifi.provenance;

import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.util.DirectoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class IndexConfiguration {

    private final RepositoryConfiguration repoConfig;
    private final Map<File, List<File>> indexDirectoryMap = new HashMap<>();
    private final Pattern indexNamePattern = DirectoryUtils.INDEX_DIRECTORY_NAME_PATTERN;

    private final Lock lock = new ReentrantLock();
    private static final Logger logger = LoggerFactory.getLogger(IndexConfiguration.class);

    private Long maxIndexedId = null;
    private Long minIndexedId = null;

    public IndexConfiguration(final RepositoryConfiguration repoConfig) {
        this.repoConfig = repoConfig;
        indexDirectoryMap.putAll(recoverIndexDirectories());
    }

    private Map<File, List<File>> recoverIndexDirectories() {
        final Map<File, List<File>> indexDirectoryMap = new HashMap<>();

        for (final File storageDirectory : repoConfig.getStorageDirectories().values()) {
            final List<File> indexDirectories = new ArrayList<>();
            final File[] matching = storageDirectory.listFiles(new FileFilter() {
                @Override
                public boolean accept(final File pathname) {
                    return pathname.isDirectory() && indexNamePattern.matcher(pathname.getName()).matches();
                }
            });

            if (matching != null) {
                indexDirectories.addAll(Arrays.asList(matching));
            }

            indexDirectoryMap.put(storageDirectory, indexDirectories);
        }

        return indexDirectoryMap;
    }

    private Long getFirstEntryTime(final File provenanceLogFile) {
        if (provenanceLogFile == null) {
            return null;
        }

        try (final RecordReader reader = RecordReaders.newRecordReader(provenanceLogFile, null, Integer.MAX_VALUE)) {
            final StandardProvenanceEventRecord firstRecord = reader.nextRecord();
            if (firstRecord == null) {
                return provenanceLogFile.lastModified();
            }
            return firstRecord.getEventTime();
        } catch (final FileNotFoundException | EOFException fnf) {
            return null; // file no longer exists or there's no record in this file
        } catch (final IOException ioe) {
            logger.warn("Failed to read first entry in file {} due to {}", provenanceLogFile, ioe.toString());
            logger.warn("", ioe);
            return null;
        }
    }

    public void removeIndexDirectory(final File indexDirectory) {
        lock.lock();
        try {
            final Set<File> keysToRemove = new HashSet<>();
            for (final Map.Entry<File, List<File>> entry : indexDirectoryMap.entrySet()) {
                final List<File> list = entry.getValue();
                list.remove(indexDirectory);

                if (list.isEmpty()) {
                    keysToRemove.add(entry.getKey());
                }
            }

            for (final File keyToRemove : keysToRemove) {
                indexDirectoryMap.remove(keyToRemove);
            }
        } finally {
            lock.unlock();
        }
    }


    public File getWritableIndexDirectory(final File provenanceLogFile, final long newIndexTimestamp) {
        return getWritableIndexDirectoryForStorageDirectory(provenanceLogFile.getParentFile(), provenanceLogFile, newIndexTimestamp);
    }

    public File getWritableIndexDirectoryForStorageDirectory(final File storageDirectory, final File provenanceLogFile, final long newIndexTimestamp) {
        lock.lock();
        try {
            List<File> indexDirectories = this.indexDirectoryMap.get(storageDirectory);
            if (indexDirectories == null) {
                final File newDir = addNewIndex(storageDirectory, provenanceLogFile, newIndexTimestamp);
                indexDirectories = new ArrayList<>();
                indexDirectories.add(newDir);
                indexDirectoryMap.put(storageDirectory, indexDirectories);
                return newDir;
            }

            if (indexDirectories.isEmpty()) {
                final File newDir = addNewIndex(storageDirectory, provenanceLogFile, newIndexTimestamp);
                indexDirectories.add(newDir);
                return newDir;
            }

            final File lastDir = indexDirectories.get(indexDirectories.size() - 1);
            final long size = getSize(lastDir);
            if (size > repoConfig.getDesiredIndexSize()) {
                final File newDir = addNewIndex(storageDirectory, provenanceLogFile, newIndexTimestamp);
                indexDirectories.add(newDir);
                return newDir;
            } else {
                return lastDir;
            }
        } finally {
            lock.unlock();
        }
    }

    private File addNewIndex(final File storageDirectory, final File provenanceLogFile, final long newIndexTimestamp) {
        // Build the event time of the first record into the index's filename so that we can determine
        // which index files to look at when we perform a search. We use the timestamp of the first record
        // in the Provenance Log file, rather than the current time, because we may perform the Indexing
        // retroactively.
        Long firstEntryTime = getFirstEntryTime(provenanceLogFile);
        if (firstEntryTime == null) {
            firstEntryTime = newIndexTimestamp;
        }
        return new File(storageDirectory, "lucene-8-index-" + firstEntryTime);
    }

    public List<File> getIndexDirectories() {
        lock.lock();
        try {
            final List<File> files = new ArrayList<>();
            for (final List<File> list : indexDirectoryMap.values()) {
                files.addAll(list);
            }
            return files;
        } finally {
            lock.unlock();
        }
    }

    private long getIndexStartTime(final File indexDir) {
        if (indexDir == null) {
            return -1L;
        }

        final Matcher matcher = indexNamePattern.matcher(indexDir.getName());
        final boolean matches = matcher.matches();
        if (matches) {
            return Long.parseLong(matcher.group(1));
        } else {
            return -1L;
        }
    }

    /**
     * Returns the index directories that are applicable only for the given time
     * span (times inclusive).
     *
     * @param startTime the start time of the query for which the indices are
     * desired
     * @param endTime the end time of the query for which the indices are
     * desired
     * @return the index directories that are applicable only for the given time
     * span (times inclusive).
     */
    public List<File> getIndexDirectories(final Long startTime, final Long endTime) {
        if (startTime == null && endTime == null) {
            return getIndexDirectories();
        }

        final List<File> dirs = new ArrayList<>();
        lock.lock();
        try {
            // Sort directories so that we return the newest index first
            final List<File> sortedIndexDirectories = getIndexDirectories();
            Collections.sort(sortedIndexDirectories, new Comparator<File>() {
                @Override
                public int compare(final File o1, final File o2) {
                    final long epochTimestamp1 = getIndexStartTime(o1);
                    final long epochTimestamp2 = getIndexStartTime(o2);
                    return Long.compare(epochTimestamp2, epochTimestamp1);
                }
            });

            for (final File indexDir : sortedIndexDirectories) {
                // If the index was last modified before the start time, we know that it doesn't
                // contain any data for us to query.
                if (startTime != null && indexDir.lastModified() < startTime) {
                    continue;
                }

                // If the index was created after the given end time, we know it doesn't contain any
                // data for us to query.
                if (endTime != null) {
                    final long indexStartTime = getIndexStartTime(indexDir);
                    if (indexStartTime > endTime) {
                        continue;
                    }
                }

                dirs.add(indexDir);
            }

            return dirs;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the index directories that are applicable only for the given
     * event log
     *
     * @param provenanceLogFile the provenance log file for which the index
     * directories are desired
     * @return the index directories that are applicable only for the given
     * event log
     */
    public List<File> getIndexDirectories(final File provenanceLogFile) {
        final List<File> dirs = new ArrayList<>();
        lock.lock();
        try {
            final List<File> indices = indexDirectoryMap.get(provenanceLogFile.getParentFile());
            if (indices == null) {
                return Collections.<File>emptyList();
            }

            final List<File> sortedIndexDirectories = new ArrayList<>(indices);
            Collections.sort(sortedIndexDirectories, new Comparator<File>() {
                @Override
                public int compare(final File o1, final File o2) {
                    final long epochTimestamp1 = getIndexStartTime(o1);
                    final long epochTimestamp2 = getIndexStartTime(o2);
                    return Long.compare(epochTimestamp1, epochTimestamp2);
                }
            });

            final Long firstEntryTime = getFirstEntryTime(provenanceLogFile);
            if (firstEntryTime == null) {
                logger.debug("Found no records in {} so returning no Indices for it", provenanceLogFile);
                return Collections.<File>emptyList();
            }

            boolean foundIndexCreatedLater = false;
            for (final File indexDir : sortedIndexDirectories) {
                // If the index was last modified before the log file was created, we know the index doesn't include
                // any data for the provenance log.
                if (indexDir.lastModified() < firstEntryTime) {
                    continue;
                }

                final long indexStartTime = getIndexStartTime(indexDir);
                if (indexStartTime > provenanceLogFile.lastModified()) {
                    // the index was created after the provenance log file was finished being modified.
                    // Either this index doesn't contain info for this provenance log OR this provenance log
                    // file triggered the index to be created. If we've already seen another index that was created
                    // after this log file was finished being modified, we can rest assured that this index wasn't
                    // created for the log file (because the previous one was or the one before that or the one before
                    // that, etc.)
                    if (foundIndexCreatedLater) {
                        continue;
                    } else {
                        foundIndexCreatedLater = true;
                    }
                }

                dirs.add(indexDir);
            }

            return dirs;
        } finally {
            lock.unlock();
        }
    }

    private long getSize(final File indexDirectory) {
        if (!indexDirectory.exists()) {
            return 0L;
        }
        if (!indexDirectory.isDirectory()) {
            throw new IllegalArgumentException("Must specify a directory but specified " + indexDirectory);
        }

        // List all files in the Index Directory.
        final File[] files = indexDirectory.listFiles();
        if (files == null) {
            return 0L;
        }

        long sum = 0L;
        for (final File file : files) {
            sum += file.length();
        }

        return sum;
    }

    /**
     * @return the amount of disk space in bytes used by all of the indices
     */
    public long getIndexSize() {
        lock.lock();
        try {
            long sum = 0L;
            for (final File indexDir : getIndexDirectories()) {
                sum += getSize(indexDir);
            }

            return sum;
        } finally {
            lock.unlock();
        }
    }

    public void setMaxIdIndexed(final long id) {
        lock.lock();
        try {
            if (maxIndexedId == null || id > maxIndexedId) {
                maxIndexedId = id;
            }
        } finally {
            lock.unlock();
        }
    }

    public Long getMaxIdIndexed() {
        lock.lock();
        try {
            return maxIndexedId;
        } finally {
            lock.unlock();
        }
    }

    public void setMinIdIndexed(final long id) {
        lock.lock();
        try {
            if (minIndexedId == null || id > minIndexedId) {
                if (maxIndexedId == null || id > maxIndexedId) {  // id will be > maxIndexedId if all records were expired
                    minIndexedId = maxIndexedId;
                } else {
                    minIndexedId = id;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public Long getMinIdIndexed() {
        lock.lock();
        try {
            return minIndexedId;
        } finally {
            lock.unlock();
        }
    }
}
