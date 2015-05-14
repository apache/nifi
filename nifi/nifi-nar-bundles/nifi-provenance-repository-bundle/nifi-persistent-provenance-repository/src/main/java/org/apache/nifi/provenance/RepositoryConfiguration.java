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

import org.apache.nifi.provenance.search.SearchableField;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RepositoryConfiguration {

    private final List<File> storageDirectories = new ArrayList<>();
    private long recordLifeMillis = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
    private long storageCapacity = 1024L * 1024L * 1024L;   // 1 GB
    private long eventFileMillis = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
    private long eventFileBytes = 1024L * 1024L * 5L;   // 5 MB
    private long desiredIndexBytes = 1024L * 1024L * 500L; // 500 MB
    private int journalCount = 16;
    private int compressionBlockBytes = 1024 * 1024;

    private List<SearchableField> searchableFields = new ArrayList<>();
    private List<SearchableField> searchableAttributes = new ArrayList<>();
    private boolean compress = true;
    private boolean alwaysSync = false;
    private int queryThreadPoolSize = 1;
    private boolean allowRollover = true;

    public void setAllowRollover(final boolean allow) {
        this.allowRollover = allow;
    }

    public boolean isAllowRollover() {
        return allowRollover;
    }


    public int getCompressionBlockBytes() {
        return compressionBlockBytes;
    }

    public void setCompressionBlockBytes(int compressionBlockBytes) {
        this.compressionBlockBytes = compressionBlockBytes;
    }

    /**
     * Specifies where the repository will store data
     *
     * @return the directories where provenance files will be stored
     */
    public List<File> getStorageDirectories() {
        return Collections.unmodifiableList(storageDirectories);
    }

    /**
     * Specifies where the repository should store data
     *
     * @param storageDirectory the directory to store provenance files
     */
    public void addStorageDirectory(final File storageDirectory) {
        this.storageDirectories.add(storageDirectory);
    }

    /**
     * @param timeUnit the desired time unit
     * @return the max amount of time that a given record will stay in the repository
     */
    public long getMaxRecordLife(final TimeUnit timeUnit) {
        return timeUnit.convert(recordLifeMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Specifies how long a record should stay in the repository
     *
     * @param maxRecordLife the max amount of time to keep a record in the repo
     * @param timeUnit the period of time used by maxRecordLife
     */
    public void setMaxRecordLife(final long maxRecordLife, final TimeUnit timeUnit) {
        this.recordLifeMillis = TimeUnit.MILLISECONDS.convert(maxRecordLife, timeUnit);
    }

    /**
     * Returns the maximum amount of data to store in the repository (in bytes)
     *
     * @return the maximum amount of disk space to use for the prov repo
     */
    public long getMaxStorageCapacity() {
        return storageCapacity;
    }

    /**
     * Sets the maximum amount of data to store in the repository (in bytes)
     *
     * @param maxStorageCapacity the maximum amount of disk space to use for the prov repo
     */
    public void setMaxStorageCapacity(final long maxStorageCapacity) {
        this.storageCapacity = maxStorageCapacity;
    }

    /**
     * @param timeUnit the desired time unit for the returned value
     * @return the maximum amount of time that the repo will write to a single event file
     */
    public long getMaxEventFileLife(final TimeUnit timeUnit) {
        return timeUnit.convert(eventFileMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * @param maxEventFileTime the max amount of time to write to a single event file
     * @param timeUnit the units for the value supplied by maxEventFileTime
     */
    public void setMaxEventFileLife(final long maxEventFileTime, final TimeUnit timeUnit) {
        this.eventFileMillis = TimeUnit.MILLISECONDS.convert(maxEventFileTime, timeUnit);
    }

    /**
     * @return the maximum number of bytes (pre-compression) that will be
     * written to a single event file before the file is rolled over
     */
    public long getMaxEventFileCapacity() {
        return eventFileBytes;
    }

    /**
     * @param maxEventFileBytes the maximum number of bytes (pre-compression) that will be written
     * to a single event file before the file is rolled over
     */
    public void setMaxEventFileCapacity(final long maxEventFileBytes) {
        this.eventFileBytes = maxEventFileBytes;
    }

    /**
     * @return the fields that should be indexed
     */
    public List<SearchableField> getSearchableFields() {
        return Collections.unmodifiableList(searchableFields);
    }

    /**
     * @param searchableFields the fields to index
     */
    public void setSearchableFields(final List<SearchableField> searchableFields) {
        this.searchableFields = new ArrayList<>(searchableFields);
    }

    /**
     * @return the FlowFile attributes that should be indexed
     */
    public List<SearchableField> getSearchableAttributes() {
        return Collections.unmodifiableList(searchableAttributes);
    }

    /**
     * @param searchableAttributes the FlowFile attributes to index
     */
    public void setSearchableAttributes(final List<SearchableField> searchableAttributes) {
        this.searchableAttributes = new ArrayList<>(searchableAttributes);
    }

    /**
     * @return whether or not event files will be compressed when they are
     * rolled over
     */
    public boolean isCompressOnRollover() {
        return compress;
    }

    /**
     * @param compress if true, the data will be compressed when rolled over
     */
    public void setCompressOnRollover(final boolean compress) {
        this.compress = compress;
    }

    /**
     * @return the number of threads to use to query the repo
     */
    public int getQueryThreadPoolSize() {
        return queryThreadPoolSize;
    }

    public void setQueryThreadPoolSize(final int queryThreadPoolSize) {
        if (queryThreadPoolSize < 1) {
            throw new IllegalArgumentException();
        }
        this.queryThreadPoolSize = queryThreadPoolSize;
    }

    /**
     * <p>
     * Specifies the desired size of each Provenance Event index shard, in
     * bytes. We shard the index for a few reasons:
     * </p>
     *
     * <ol>
     * <li>
     * A very large index requires a significant amount of Java heap space to
     * search. As the size of the shard increases, the required Java heap space
     * also increases.
     * </li>
     * <li>
     * By having multiple shards, we have the ability to use multiple concurrent
     * threads to search the individual shards, resulting in far less latency
     * when performing a search across millions or billions of records.
     * </li>
     * <li>
     * We keep track of which time ranges each index shard spans. As a result,
     * we are able to determine which shards need to be searched if a search
     * provides a date range. This can greatly increase the speed of a search
     * and reduce resource utilization.
     * </li>
     * </ol>
     *
     * @param bytes the number of bytes to write to an index before beginning a new shard
     */
    public void setDesiredIndexSize(final long bytes) {
        this.desiredIndexBytes = bytes;
    }

    /**
     * @return the desired size of each index shard. See the
     * {@link #setDesiredIndexSize} method for an explanation of why we choose
     * to shard the index.
     */
    public long getDesiredIndexSize() {
        return desiredIndexBytes;
    }

    /**
     * @param numJournals the number of Journal files to use when persisting records.
     */
    public void setJournalCount(final int numJournals) {
        if (numJournals < 1) {
            throw new IllegalArgumentException();
        }

        this.journalCount = numJournals;
    }

    /**
     * @return the number of Journal files that will be used when persisting records.
     */
    public int getJournalCount() {
        return journalCount;
    }

    /**
     * @return <code>true</code> if the repository will perform an 'fsync' for all updates to disk
     */
    public boolean isAlwaysSync() {
        return alwaysSync;
    }

    /**
     * Configures whether or not the Repository should sync all updates to disk.
     * Setting this value to true means that updates are guaranteed to be
     * persisted across restarted, even if there is a power failure or a sudden
     * Operating System crash, but it can be very expensive.
     *
     * @param alwaysSync whether or not to perform an 'fsync' for all updates to disk
     */
    public void setAlwaysSync(boolean alwaysSync) {
        this.alwaysSync = alwaysSync;
    }
}
