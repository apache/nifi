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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryConfiguration.class);

    public static final String CONCURRENT_MERGE_THREADS = "nifi.provenance.repository.concurrent.merge.threads";
    public static final String WARM_CACHE_FREQUENCY = "nifi.provenance.repository.warm.cache.frequency";

    private final Map<String, File> storageDirectories = new LinkedHashMap<>();
    private long recordLifeMillis = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
    private long storageCapacity = 1024L * 1024L * 1024L;   // 1 GB
    private long eventFileMillis = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
    private long eventFileBytes = 1024L * 1024L * 5L;   // 5 MB
    private int maxFileEvents = Integer.MAX_VALUE;
    private long desiredIndexBytes = 1024L * 1024L * 500L; // 500 MB
    private int journalCount = 16;
    private int compressionBlockBytes = 1024 * 1024;
    private int maxAttributeChars = 65536;
    private int debugFrequency = 1_000_000;

    private Map<String, String> encryptionKeys;
    private String keyId;
    private String keyProviderImplementation;
    private String keyProviderLocation;

    private List<SearchableField> searchableFields = new ArrayList<>();
    private List<SearchableField> searchableAttributes = new ArrayList<>();
    private boolean compress = true;
    private boolean alwaysSync = false;
    private int queryThreadPoolSize = 2;
    private int indexThreadPoolSize = 1;
    private boolean allowRollover = true;
    private int concurrentMergeThreads = 4;
    private Integer warmCacheFrequencyMinutes = null;

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
    public Map<String, File> getStorageDirectories() {
        return Collections.unmodifiableMap(storageDirectories);
    }

    /**
     * Specifies where the repository should store data
     *
     * @param storageDirectory the directory to store provenance files
     */
    public void addStorageDirectory(final String partitionName, final File storageDirectory) {
        this.storageDirectories.put(partitionName, storageDirectory);
    }

    public void addStorageDirectories(final Map<String, File> storageDirectories) {
        this.storageDirectories.putAll(storageDirectories);
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
     * @return the maximum number of events that should be written to a single event file before the file is rolled over
     */
    public int getMaxEventFileCount() {
        return maxFileEvents;
    }

    /**
     * @param maxCount the maximum number of events that should be written to a single event file before the file is rolled over
     */
    public void setMaxEventFileCount(final int maxCount) {
        this.maxFileEvents = maxCount;
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
     * @return the number of threads to use to index provenance events
     */
    public int getIndexThreadPoolSize() {
        return indexThreadPoolSize;
    }

    public void setIndexThreadPoolSize(final int indexThreadPoolSize) {
        if (indexThreadPoolSize < 1) {
            throw new IllegalArgumentException();
        }
        this.indexThreadPoolSize = indexThreadPoolSize;
    }

    public void setConcurrentMergeThreads(final int mergeThreads) {
        this.concurrentMergeThreads = mergeThreads;
    }

    public int getConcurrentMergeThreads() {
        return concurrentMergeThreads;
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
     * also increases.</li>
     * <li>
     * By having multiple shards, we have the ability to use multiple concurrent
     * threads to search the individual shards, resulting in far less latency
     * when performing a search across millions or billions of records.</li>
     * <li>
     * We keep track of which time ranges each index shard spans. As a result,
     * we are able to determine which shards need to be searched if a search
     * provides a date range. This can greatly increase the speed of a search
     * and reduce resource utilization.</li>
     * </ol>
     *
     * @param bytes
     *            the number of bytes to write to an index before beginning a
     *            new shard
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

    /**
     * @return the maximum number of characters to include in any attribute. If an attribute in a Provenance
     *         Event has more than this number of characters, it will be truncated when the event is retrieved.
     */
    public int getMaxAttributeChars() {
        return maxAttributeChars;
    }

    /**
     * Sets the maximum number of characters to include in any attribute. If an attribute in a Provenance
     * Event has more than this number of characters, it will be truncated when it is retrieved.
     */
    public void setMaxAttributeChars(int maxAttributeChars) {
        this.maxAttributeChars = maxAttributeChars;
    }

    public void setWarmCacheFrequencyMinutes(Integer frequencyMinutes) {
        this.warmCacheFrequencyMinutes = frequencyMinutes;
    }

    public Optional<Integer> getWarmCacheFrequencyMinutes() {
        return Optional.ofNullable(warmCacheFrequencyMinutes);
    }

    public boolean supportsEncryption() {
        boolean keyProviderIsConfigured = CryptoUtils.isValidKeyProvider(keyProviderImplementation, keyProviderLocation, keyId, encryptionKeys);

        return keyProviderIsConfigured;
    }

    // TODO: Add verbose error output for encryption support failure if requested

    public Map<String, String> getEncryptionKeys() {
        return encryptionKeys;
    }

    public void setEncryptionKeys(Map<String, String> encryptionKeys) {
        this.encryptionKeys = encryptionKeys;
    }

    public String getKeyId() {
        return keyId;
    }

    public void setKeyId(String keyId) {
        this.keyId = keyId;
    }

    public String getKeyProviderImplementation() {
        return keyProviderImplementation;
    }

    public void setKeyProviderImplementation(String keyProviderImplementation) {
        this.keyProviderImplementation = keyProviderImplementation;
    }

    public String getKeyProviderLocation() {
        return keyProviderLocation;
    }

    public void setKeyProviderLocation(String keyProviderLocation) {
        this.keyProviderLocation = keyProviderLocation;
    }


    public int getDebugFrequency() {
        return debugFrequency;
    }

    public void setDebugFrequency(int debugFrequency) {
        this.debugFrequency = debugFrequency;
    }


    public static RepositoryConfiguration create(final NiFiProperties nifiProperties) {
        final Map<String, Path> storageDirectories = nifiProperties.getProvenanceRepositoryPaths();
        if (storageDirectories.isEmpty()) {
            storageDirectories.put("provenance_repository", Paths.get("provenance_repository"));
        }
        final String storageTime = nifiProperties.getProperty(NiFiProperties.PROVENANCE_MAX_STORAGE_TIME, "24 hours");
        final String storageSize = nifiProperties.getProperty(NiFiProperties.PROVENANCE_MAX_STORAGE_SIZE, "1 GB");
        final String rolloverTime = nifiProperties.getProperty(NiFiProperties.PROVENANCE_ROLLOVER_TIME, "5 mins");
        final String rolloverSize = nifiProperties.getProperty(NiFiProperties.PROVENANCE_ROLLOVER_SIZE, "100 MB");
        final String shardSize = nifiProperties.getProperty(NiFiProperties.PROVENANCE_INDEX_SHARD_SIZE, "500 MB");
        final int queryThreads = nifiProperties.getIntegerProperty(NiFiProperties.PROVENANCE_QUERY_THREAD_POOL_SIZE, 2);
        final int indexThreads = nifiProperties.getIntegerProperty(NiFiProperties.PROVENANCE_INDEX_THREAD_POOL_SIZE, 2);
        final int journalCount = nifiProperties.getIntegerProperty(NiFiProperties.PROVENANCE_JOURNAL_COUNT, 16);
        final int concurrentMergeThreads = nifiProperties.getIntegerProperty(CONCURRENT_MERGE_THREADS, 2);
        final String warmCacheFrequency = nifiProperties.getProperty(WARM_CACHE_FREQUENCY);

        final long storageMillis = FormatUtils.getTimeDuration(storageTime, TimeUnit.MILLISECONDS);
        final long maxStorageBytes = DataUnit.parseDataSize(storageSize, DataUnit.B).longValue();
        final long rolloverMillis = FormatUtils.getTimeDuration(rolloverTime, TimeUnit.MILLISECONDS);
        final long rolloverBytes = DataUnit.parseDataSize(rolloverSize, DataUnit.B).longValue();

        final boolean compressOnRollover = Boolean.parseBoolean(nifiProperties.getProperty(NiFiProperties.PROVENANCE_COMPRESS_ON_ROLLOVER));
        final String indexedFieldString = nifiProperties.getProperty(NiFiProperties.PROVENANCE_INDEXED_FIELDS);
        final String indexedAttrString = nifiProperties.getProperty(NiFiProperties.PROVENANCE_INDEXED_ATTRIBUTES);

        final Boolean alwaysSync = Boolean.parseBoolean(nifiProperties.getProperty("nifi.provenance.repository.always.sync", "false"));

        final int defaultMaxAttrChars = 65536;
        final String maxAttrLength = nifiProperties.getProperty("nifi.provenance.repository.max.attribute.length", String.valueOf(defaultMaxAttrChars));
        int maxAttrChars;
        try {
            maxAttrChars = Integer.parseInt(maxAttrLength);
            // must be at least 36 characters because that's the length of the uuid attribute,
            // which must be kept intact
            if (maxAttrChars < 36) {
                maxAttrChars = 36;
                logger.warn("Found max attribute length property set to " + maxAttrLength + " but minimum length is 36; using 36 instead");
            }
        } catch (final Exception e) {
            maxAttrChars = defaultMaxAttrChars;
        }

        final List<SearchableField> searchableFields = SearchableFieldParser.extractSearchableFields(indexedFieldString, true);
        final List<SearchableField> searchableAttributes = SearchableFieldParser.extractSearchableFields(indexedAttrString, false);

        // We always want to index the Event Time.
        if (!searchableFields.contains(SearchableFields.EventTime)) {
            searchableFields.add(SearchableFields.EventTime);
        }

        final RepositoryConfiguration config = new RepositoryConfiguration();
        for (final Map.Entry<String, Path> entry : storageDirectories.entrySet()) {
            config.addStorageDirectory(entry.getKey(), entry.getValue().toFile());
        }
        config.setCompressOnRollover(compressOnRollover);
        config.setSearchableFields(searchableFields);
        config.setSearchableAttributes(searchableAttributes);
        config.setMaxEventFileCapacity(rolloverBytes);
        config.setMaxEventFileLife(rolloverMillis, TimeUnit.MILLISECONDS);
        config.setMaxRecordLife(storageMillis, TimeUnit.MILLISECONDS);
        config.setMaxStorageCapacity(maxStorageBytes);
        config.setQueryThreadPoolSize(queryThreads);
        config.setIndexThreadPoolSize(indexThreads);
        config.setJournalCount(journalCount);
        config.setMaxAttributeChars(maxAttrChars);
        config.setConcurrentMergeThreads(concurrentMergeThreads);

        if (warmCacheFrequency != null && !warmCacheFrequency.trim().equals("")) {
            config.setWarmCacheFrequencyMinutes((int) FormatUtils.getTimeDuration(warmCacheFrequency, TimeUnit.MINUTES));
        }
        if (shardSize != null) {
            config.setDesiredIndexSize(DataUnit.parseDataSize(shardSize, DataUnit.B).longValue());
        }

        config.setAlwaysSync(alwaysSync);

        config.setDebugFrequency(nifiProperties.getIntegerProperty(NiFiProperties.PROVENANCE_REPO_DEBUG_FREQUENCY, config.getDebugFrequency()));

        // Encryption values may not be present but are only required for EncryptedWriteAheadProvenanceRepository
        final String implementationClassName = nifiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS);
        if (EncryptedWriteAheadProvenanceRepository.class.getName().equals(implementationClassName)) {
            config.setEncryptionKeys(nifiProperties.getProvenanceRepoEncryptionKeys());
            config.setKeyId(nifiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_ID));
            config.setKeyProviderImplementation(nifiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS));
            config.setKeyProviderLocation(nifiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_LOCATION));
        }

        return config;
    }
}
