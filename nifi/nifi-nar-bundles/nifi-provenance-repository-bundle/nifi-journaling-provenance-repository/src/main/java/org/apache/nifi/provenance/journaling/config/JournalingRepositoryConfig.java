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
package org.apache.nifi.provenance.journaling.config;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.provenance.search.SearchableField;

public class JournalingRepositoryConfig {
    private Map<String, File> containers = new HashMap<>();
    private long expirationMillis = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
    private long storageCapacity = 1024L * 1024L * 1024L;   // 1 GB
    private long rolloverMillis = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
    private long journalCapacity = 1024L * 1024L * 5L;   // 5 MB
    private long desiredIndexBytes = 1024L * 1024L * 500L; // 500 MB
    private int partitionCount = 16;
    private int blockSize = 5000;

    private List<SearchableField> searchableFields = new ArrayList<>();
    private List<SearchableField> searchableAttributes = new ArrayList<>();
    private boolean compress = true;
    private boolean alwaysSync = false;
    private int threadPoolSize = 4;
    private boolean readOnly = false;

    public void setReadOnly(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Specifies where the repository will store data
     *
     * @return
     */
    public Map<String, File> getContainers() {
        return Collections.unmodifiableMap(containers);
    }

    /**
     * Specifies where the repository should store data
     *
     * @param storageDirectory
     */
    public void setContainers(final Map<String, File> containers) {
        this.containers = new HashMap<>(containers);
    }

    /**
     * Returns the maximum amount of time that a given record will stay in the
     * repository
     *
     * @param timeUnit
     * @return
     */
    public long getEventExpiration(final TimeUnit timeUnit) {
        return timeUnit.convert(expirationMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Specifies how long a record should stay in the repository
     *
     * @param expiration
     * @param timeUnit
     */
    public void setEventExpiration(final long expiration, final TimeUnit timeUnit) {
        this.expirationMillis = TimeUnit.MILLISECONDS.convert(expiration, timeUnit);
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
    public long getJournalRolloverPeriod(final TimeUnit timeUnit) {
        return timeUnit.convert(rolloverMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum amount of time to write to a single event file
     *
     * @param rolloverPeriod
     * @param timeUnit
     */
    public void setJournalRolloverPeriod(final long rolloverPeriod, final TimeUnit timeUnit) {
        this.rolloverMillis = TimeUnit.MILLISECONDS.convert(rolloverPeriod, timeUnit);
    }

    /**
     * Returns the number of bytes (pre-compression) that will be
     * written to a single journal file before the file is rolled over
     *
     * @return
     */
    public long getJournalCapacity() {
        return journalCapacity;
    }

    /**
     * Sets the number of bytes (pre-compression) that will be written
     * to a single journal file before the file is rolled over
     *
     * @param journalCapacity
     */
    public void setJournalCapacity(final long journalCapacity) {
        this.journalCapacity = journalCapacity;
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

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(final int queryThreadPoolSize) {
        if (queryThreadPoolSize < 1) {
            throw new IllegalArgumentException();
        }
        this.threadPoolSize = queryThreadPoolSize;
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
    public void setPartitionCount(final int numJournals) {
        if (numJournals < 1) {
            throw new IllegalArgumentException();
        }

        this.partitionCount = numJournals;
    }

    /**
     * Returns the number of Journal files that will be used when persisting
     * records.
     *
     * @return
     */
    public int getPartitionCount() {
        return partitionCount;
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

    /**
     * Returns the minimum number of Provenance Events that should be written to a single Block.
     * Events are written out in blocks, which are later optionally compressed. A larger block size
     * will potentially result in better compression. However, a smaller block size will result
     * in better performance when reading the data. The default value is 100 events per block.
     * 
     * @return
     */
    public int getBlockSize() {
        return blockSize;
    }
    
    /**
     * Sets the minimum number of Provenance Events that should be written to a single Block.
     * Events are written out in blocks, which are later optionally compressed. A larger block size
     * will potentially result in better compression. However, a smaller block size will result
     * in better performance when reading the data. The default value is 100 events per block.
     * 
     * @return
     */
    public void setBlockSize(final int blockSize) {
        if ( blockSize < 1 ) {
            throw new IllegalArgumentException("Cannot set block size to " + blockSize + "; must be a positive integer");
        }
        this.blockSize = blockSize;
    }
}
