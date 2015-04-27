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
     * @return
     */
    public List<File> getStorageDirectories() {
        return Collections.unmodifiableList(storageDirectories);
    }

    /**
     * Specifies where the repository should store data
     *
     * @param storageDirectory
     */
    public void addStorageDirectory(final File storageDirectory) {
        this.storageDirectories.add(storageDirectory);
    }

    /**
     * Returns the minimum amount of time that a given record will stay in the
     * repository
     *
     * @param timeUnit
     * @return
     */
    public long getMaxRecordLife(final TimeUnit timeUnit) {
        return timeUnit.convert(recordLifeMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Specifies how long a record should stay in the repository
     *
     * @param maxRecordLife
     * @param timeUnit
     */
    public void setMaxRecordLife(final long maxRecordLife, final TimeUnit timeUnit) {
        this.recordLifeMillis = TimeUnit.MILLISECONDS.convert(maxRecordLife, timeUnit);
    }

    /**
     * Returns the maximum amount of data to store in the repository (in bytes)
     *
     * @return
     */
    public long getMaxStorageCapacity() {
        return storageCapacity;
    }

    /**
     * Sets the maximum amount of data to store in the repository (in bytes)
     * @param maxStorageCapacity
     */
    public void setMaxStorageCapacity(final long maxStorageCapacity) {
        this.storageCapacity = maxStorageCapacity;
    }

    /**
     * Returns the maximum amount of time to write to a single event file
     *
     * @param timeUnit
     * @return
     */
    public long getMaxEventFileLife(final TimeUnit timeUnit) {
        return timeUnit.convert(eventFileMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum amount of time to write to a single event file
     *
     * @param maxEventFileTime
     * @param timeUnit
     */
    public void setMaxEventFileLife(final long maxEventFileTime, final TimeUnit timeUnit) {
        this.eventFileMillis = TimeUnit.MILLISECONDS.convert(maxEventFileTime, timeUnit);
    }

    /**
     * Returns the maximum number of bytes (pre-compression) that will be
     * written to a single event file before the file is rolled over
     *
     * @return
     */
    public long getMaxEventFileCapacity() {
        return eventFileBytes;
    }

    /**
     * Sets the maximum number of bytes (pre-compression) that will be written
     * to a single event file before the file is rolled over
     *
     * @param maxEventFileBytes
     */
    public void setMaxEventFileCapacity(final long maxEventFileBytes) {
        this.eventFileBytes = maxEventFileBytes;
    }

    /**
     * Returns the fields that can be indexed
     *
     * @return
     */
    public List<SearchableField> getSearchableFields() {
        return Collections.unmodifiableList(searchableFields);
    }

    /**
     * Sets the fields to index
     *
     * @param searchableFields
     */
    public void setSearchableFields(final List<SearchableField> searchableFields) {
        this.searchableFields = new ArrayList<>(searchableFields);
    }

    /**
     * Returns the FlowFile attributes that can be indexed
     *
     * @return
     */
    public List<SearchableField> getSearchableAttributes() {
        return Collections.unmodifiableList(searchableAttributes);
    }

    /**
     * Sets the FlowFile attributes to index
     *
     * @param searchableAttributes
     */
    public void setSearchableAttributes(final List<SearchableField> searchableAttributes) {
        this.searchableAttributes = new ArrayList<>(searchableAttributes);
    }

    /**
     * Indicates whether or not event files will be compressed when they are
     * rolled over
     *
     * @return
     */
    public boolean isCompressOnRollover() {
        return compress;
    }

    /**
     * Specifies whether or not to compress event files on rollover
     *
     * @param compress
     */
    public void setCompressOnRollover(final boolean compress) {
        this.compress = compress;
    }

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
     * @param bytes
     */
    public void setDesiredIndexSize(final long bytes) {
        this.desiredIndexBytes = bytes;
    }

    /**
     * Returns the desired size of each index shard. See the
     * {@Link #setDesiredIndexSize} method for an explanation of why we choose
     * to shard the index.
     *
     * @return
     */
    public long getDesiredIndexSize() {
        return desiredIndexBytes;
    }

    /**
     * Sets the number of Journal files to use when persisting records.
     *
     * @param numJournals
     */
    public void setJournalCount(final int numJournals) {
        if (numJournals < 1) {
            throw new IllegalArgumentException();
        }

        this.journalCount = numJournals;
    }

    /**
     * Returns the number of Journal files that will be used when persisting
     * records.
     *
     * @return
     */
    public int getJournalCount() {
        return journalCount;
    }

    /**
     * Specifies whether or not the Repository should sync all updates to disk.
     *
     * @return
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
     * @param alwaysSync
     */
    public void setAlwaysSync(boolean alwaysSync) {
        this.alwaysSync = alwaysSync;
    }
}
