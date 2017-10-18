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

package org.apache.nifi.provenance.index.lucene;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.util.DirectoryUtils;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexDirectoryManager {
    private static final Logger logger = LoggerFactory.getLogger(IndexDirectoryManager.class);
    private static final FileFilter INDEX_DIRECTORY_FILTER = f -> f.getName().startsWith("index-");
    private static final Pattern INDEX_FILENAME_PATTERN = Pattern.compile("index-(\\d+)");

    private final RepositoryConfiguration repoConfig;

    // guarded by synchronizing on 'this'
    private final SortedMap<Long, List<IndexLocation>> indexLocationByTimestamp = new TreeMap<>();
    private final Map<String, IndexLocation> activeIndices = new HashMap<>();

    public IndexDirectoryManager(final RepositoryConfiguration repoConfig) {
        this.repoConfig = repoConfig;
    }

    public synchronized void initialize() {
        final Map<File, Tuple<Long, IndexLocation>> latestIndexByStorageDir = new HashMap<>();

        for (final Map.Entry<String, File> entry : repoConfig.getStorageDirectories().entrySet()) {
            final String partitionName = entry.getKey();
            final File storageDir = entry.getValue();

            final File[] indexDirs = storageDir.listFiles(INDEX_DIRECTORY_FILTER);
            if (indexDirs == null) {
                logger.warn("Unable to access Provenance Repository storage directory {}", storageDir);
                continue;
            }

            for (final File indexDir : indexDirs) {
                final Matcher matcher = INDEX_FILENAME_PATTERN.matcher(indexDir.getName());
                if (!matcher.matches()) {
                    continue;
                }

                final long startTime = DirectoryUtils.getIndexTimestamp(indexDir);
                final List<IndexLocation> dirsForTimestamp = indexLocationByTimestamp.computeIfAbsent(startTime, t -> new ArrayList<>());
                final IndexLocation indexLoc = new IndexLocation(indexDir, startTime, partitionName);
                dirsForTimestamp.add(indexLoc);

                final Tuple<Long, IndexLocation> tuple = latestIndexByStorageDir.get(storageDir);
                if (tuple == null || startTime > tuple.getKey()) {
                    latestIndexByStorageDir.put(storageDir, new Tuple<>(startTime, indexLoc));
                }
            }
        }

        // Restore the activeIndices to point at the newest index in each storage location.
        for (final Tuple<Long, IndexLocation> tuple : latestIndexByStorageDir.values()) {
            final IndexLocation indexLoc = tuple.getValue();
            activeIndices.put(indexLoc.getPartitionName(), indexLoc);
        }
    }


    public synchronized void deleteDirectory(final File directory) {
        final Iterator<Map.Entry<Long, List<IndexLocation>>> itr = indexLocationByTimestamp.entrySet().iterator();
        while (itr.hasNext()) {
            final Map.Entry<Long, List<IndexLocation>> entry = itr.next();
            final List<IndexLocation> locations = entry.getValue();

            final IndexLocation locToRemove = new IndexLocation(directory, DirectoryUtils.getIndexTimestamp(directory), directory.getName());
            locations.remove(locToRemove);
            if (locations.isEmpty()) {
                itr.remove();
            }
        }
    }

    /**
     * Returns a List of all indexes where the latest event in the index has an event time before the given timestamp
     *
     * @param timestamp the cutoff
     * @return all Files that belong to an index, where the index has no events later than the given time
     */
    public synchronized List<File> getDirectoriesBefore(final long timestamp) {
        final List<File> selected = new ArrayList<>();

        // An index cannot be expired if it is the latest index in the storage directory. As a result, we need to
        // separate the indexes by Storage Directory so that we can easily determine if this is the case.
        final Map<String, List<IndexLocation>> startTimeWithFileByStorageDirectory = flattenDirectoriesByTimestamp().stream()
            .collect(Collectors.groupingBy(indexLoc -> indexLoc.getPartitionName()));

        // Scan through the index directories and the associated index event start time.
        // If looking at index N, we can determine the index end time by assuming that it is the same as the
        // start time of index N+1. So we determine the time range of each index and select an index only if
        // its start time is before the given timestamp and its end time is <= the given timestamp.
        for (final List<IndexLocation> locationList : startTimeWithFileByStorageDirectory.values()) {
            for (int i = 0; i < locationList.size(); i++) {
                final IndexLocation indexLoc = locationList.get(i);

                final String partition = indexLoc.getPartitionName();
                final IndexLocation activeLocation = activeIndices.get(partition);
                if (indexLoc.equals(activeLocation)) {
                    continue;
                }

                final Long indexStartTime = indexLoc.getIndexStartTimestamp();
                if (indexStartTime > timestamp) {
                    // If the first timestamp in the index is later than the desired timestamp,
                    // then we are done. We can do this because the list is ordered by monotonically
                    // increasing timestamp as the Tuple key.
                    break;
                }

                final long indexEndTime = indexLoc.getIndexEndTimestamp();
                if (indexEndTime <= timestamp) {
                    logger.debug("Considering Index Location {} older than {} ({}) because its events have an EventTime "
                        + "ranging from {} ({}) to {} ({}) based on the following IndexLocations: {}", indexLoc, timestamp, new Date(timestamp),
                        indexStartTime, new Date(indexStartTime), indexEndTime, new Date(indexEndTime), locationList);

                    selected.add(indexLoc.getIndexDirectory());
                }
            }
        }

        logger.debug("Returning the following list of index locations because they were finished being written to before {}: {}", timestamp, selected);
        return selected;
    }

    /**
     * Convert directoriesByTimestamp to a List of IndexLocations.
     * This allows us to easily get the 'next' value when iterating over the elements.
     * This is useful because we know that the 'next' value will have a timestamp that is when that
     * file started being written to - which is the same as when this index stopped being written to.
     *
     * @return a List of all IndexLocations known
     */
    private List<IndexLocation> flattenDirectoriesByTimestamp() {
        final List<IndexLocation> startTimeWithFile = new ArrayList<>();
        for (final Map.Entry<Long, List<IndexLocation>> entry : indexLocationByTimestamp.entrySet()) {
            for (final IndexLocation indexLoc : entry.getValue()) {
                startTimeWithFile.add(indexLoc);
            }
        }

        return startTimeWithFile;
    }

    public synchronized List<File> getDirectories(final Long startTime, final Long endTime) {
        final List<File> selected = new ArrayList<>();

        // An index cannot be expired if it is the latest index in the partition. As a result, we need to
        // separate the indexes by partition so that we can easily determine if this is the case.
        final Map<String, List<IndexLocation>> startTimeWithFileByStorageDirectory = flattenDirectoriesByTimestamp().stream()
            .collect(Collectors.groupingBy(indexLoc -> indexLoc.getPartitionName()));

        for (final List<IndexLocation> locationList : startTimeWithFileByStorageDirectory.values()) {
            selected.addAll(getDirectories(startTime, endTime, locationList));
        }

        return selected;
    }

    public synchronized List<File> getDirectories(final Long startTime, final Long endTime, final String partitionName) {
        // An index cannot be expired if it is the latest index in the partition. As a result, we need to
        // separate the indexes by partition so that we can easily determine if this is the case.
        final Map<String, List<IndexLocation>> startTimeWithFileByStorageDirectory = flattenDirectoriesByTimestamp().stream()
            .collect(Collectors.groupingBy(indexLoc -> indexLoc.getPartitionName()));

        final List<IndexLocation> indexLocations = startTimeWithFileByStorageDirectory.get(partitionName);
        if (indexLocations == null) {
            return Collections.emptyList();
        }

        return getDirectories(startTime, endTime, indexLocations);
    }

    protected static List<File> getDirectories(final Long startTime, final Long endTime, final List<IndexLocation> locations) {
        final List<File> selected = new ArrayList<>();

        int overlapCount = 0;
        for (int i = 0; i < locations.size(); i++) {
            final IndexLocation indexLoc = locations.get(i);
            final Long indexStartTimestamp = indexLoc.getIndexStartTimestamp();
            if (endTime != null && indexStartTimestamp > endTime) {
                if (overlapCount == 0) {
                    // Because of how we handle index timestamps and the multi-threading, it is possible
                    // the we could have some overlap where Thread T1 gets an Event with start time 1,000
                    // for instance. Then T2 gets and Event with start time 1,002 and ends up creating a
                    // new index directory with a start time of 1,002. Then T1 could end up writing events
                    // with timestamp 1,000 to an index with a 'start time' of 1,002. Because of this,
                    // the index start times are approximate. To address this, we include one extra Index
                    // Directory based on start time, so that if we want index directories for Time Range
                    // 1,000 - 1,001 and have indexes 999 and 1,002 we will include the 999 and the 'overlapping'
                    // directory of 1,002 since it could potentially have an event with overlapping timestamp.
                    overlapCount++;
                } else {
                    continue;
                }
            }

            if (startTime != null) {
                final Long indexEndTimestamp;
                if (i < locations.size() - 1) {
                    final IndexLocation nextIndexLoc = locations.get(i + 1);
                    indexEndTimestamp = nextIndexLoc.getIndexStartTimestamp();
                    if (indexEndTimestamp < startTime) {
                        continue;
                    }
                }
            }

            selected.add(indexLoc.getIndexDirectory());
        }

        return selected;
    }

    /**
     * Notifies the Index Directory Manager that an Index Writer has been committed for the
     * given index directory. This allows the Directory Manager to know that it needs to check
     * the size of the index directory and not return this directory as a writable directory
     * any more if the size has reached the configured threshold.
     *
     * @param indexDir the directory that was written to
     * @return <code>true</code> if the index directory has reached its max threshold and should no
     *         longer be written to, <code>false</code> if the index directory is not full.
     */
    public boolean onIndexCommitted(final File indexDir) {
        final long indexSize = getSize(indexDir);
        synchronized (this) {
            String partitionName = null;
            for (final Map.Entry<String, IndexLocation> entry : activeIndices.entrySet()) {
                if (indexDir.equals(entry.getValue().getIndexDirectory())) {
                    partitionName = entry.getKey();
                    break;
                }
            }

            // If the index is not the active index directory, it should no longer be written to.
            if (partitionName == null) {
                logger.debug("Size of Provenance Index at {} is now {}. However, was unable to find the appropriate Active Index to roll over.", indexDir, indexSize);
                return true;
            }

            // If the index size >= desired index size, it should no longer be written to.
            if (indexSize >= repoConfig.getDesiredIndexSize()) {
                logger.info("Size of Provenance Index at {} is now {}. Will close this index and roll over to a new one.", indexDir, indexSize);
                activeIndices.remove(partitionName);

                return true;
            }

            // Index directory is the active index directory and has not yet exceeded the desired size.
            return false;
        }
    }

    public synchronized Optional<File> getActiveIndexDirectory(final String partitionName) {
        final IndexLocation indexLocation = activeIndices.get(partitionName);
        if (indexLocation == null) {
            return Optional.empty();
        }

        return Optional.of(indexLocation.getIndexDirectory());
    }

    private long getSize(final File indexDir) {
        if (!indexDir.exists()) {
            return 0L;
        }
        if (!indexDir.isDirectory()) {
            throw new IllegalArgumentException("Must specify a directory but specified " + indexDir);
        }

        // List all files in the Index Directory.
        final File[] files = indexDir.listFiles();
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
     * Provides the File that is the directory for the index that should be written to. If there is no index yet
     * to be written to, or if the index has reached its max size, a new one will be created. The given {@code earliestTimestamp}
     * should represent the event time of the first event that will go into the index. This is used for file naming purposes so
     * that the appropriate directories can be looked up quickly later.
     *
     * @param earliestTimestamp the event time of the first event that will go into a new index, if a new index is created by this call.
     * @param partitionName the name of the partition to write to
     * @return the directory that should be written to
     */
    public synchronized File getWritableIndexingDirectory(final long earliestTimestamp, final String partitionName) {
        IndexLocation indexLoc = activeIndices.get(partitionName);
        if (indexLoc == null) {
            indexLoc = new IndexLocation(createIndex(earliestTimestamp, partitionName), earliestTimestamp, partitionName);
            logger.debug("Created new Index Directory {}", indexLoc);

            indexLocationByTimestamp.computeIfAbsent(earliestTimestamp, t -> new ArrayList<>()).add(indexLoc);
            activeIndices.put(partitionName, indexLoc);
        }

        return indexLoc.getIndexDirectory();
    }

    private File createIndex(final long earliestTimestamp, final String partitionName) {
        final File storageDir = repoConfig.getStorageDirectories().entrySet().stream()
            .filter(e -> e.getKey().equals(partitionName))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Invalid Partition: " + partitionName));
        final File indexDir = new File(storageDir, "index-" + earliestTimestamp);

        return indexDir;
    }
}
