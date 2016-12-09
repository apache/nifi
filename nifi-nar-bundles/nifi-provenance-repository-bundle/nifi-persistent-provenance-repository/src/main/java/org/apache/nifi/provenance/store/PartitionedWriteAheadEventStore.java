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

package org.apache.nifi.provenance.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.NamedThreadFactory;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.index.EventIndex;
import org.apache.nifi.provenance.serialization.EventFileCompressor;

public class PartitionedWriteAheadEventStore extends PartitionedEventStore {
    private final BlockingQueue<File> filesToCompress;
    private final List<WriteAheadStorePartition> partitions;
    private final RepositoryConfiguration repoConfig;

    private final ExecutorService compressionExecutor;
    private final List<EventFileCompressor> fileCompressors = Collections.synchronizedList(new ArrayList<>());
    private final EventReporter eventReporter;
    private final EventFileManager fileManager;

    public PartitionedWriteAheadEventStore(final RepositoryConfiguration repoConfig, final RecordWriterFactory recordWriterFactory,
        final RecordReaderFactory recordReaderFactory, final EventReporter eventReporter, final EventFileManager fileManager) {
        super(repoConfig, eventReporter);
        this.repoConfig = repoConfig;
        this.eventReporter = eventReporter;
        this.filesToCompress = new LinkedBlockingQueue<>(100);
        final AtomicLong idGenerator = new AtomicLong(0L);
        this.partitions = createPartitions(repoConfig, recordWriterFactory, recordReaderFactory, idGenerator);
        this.fileManager = fileManager;

        // Creates tasks to compress data on rollover
        if (repoConfig.isCompressOnRollover()) {
            compressionExecutor = Executors.newFixedThreadPool(repoConfig.getIndexThreadPoolSize(), new NamedThreadFactory("Compress Provenance Logs"));
        } else {
            compressionExecutor = null;
        }
    }

    private List<WriteAheadStorePartition> createPartitions(final RepositoryConfiguration repoConfig, final RecordWriterFactory recordWriterFactory,
        final RecordReaderFactory recordReaderFactory, final AtomicLong idGenerator) {
        final Map<String, File> storageDirectories = repoConfig.getStorageDirectories();
        final List<WriteAheadStorePartition> partitions = new ArrayList<>(storageDirectories.size());

        for (final Map.Entry<String, File> entry : storageDirectories.entrySet()) {
            // Need to ensure that the same partition directory always gets the same partition index.
            // If we don't, then we will end up re-indexing the events from 1 index into another index, and
            // this will result in a lot of duplicates (up to a million per index per restart). This is the reason
            // that we use a partition name here based on the properties file.
            final String partitionName = entry.getKey();
            final File storageDirectory = entry.getValue();
            partitions.add(new WriteAheadStorePartition(storageDirectory, partitionName, repoConfig,
                recordWriterFactory, recordReaderFactory, filesToCompress, idGenerator, eventReporter));
        }

        return partitions;
    }

    @Override
    public void initialize() throws IOException {
        if (repoConfig.isCompressOnRollover()) {
            for (int i = 0; i < repoConfig.getIndexThreadPoolSize(); i++) {
                final EventFileCompressor compressor = new EventFileCompressor(filesToCompress, fileManager);
                compressionExecutor.submit(compressor);
                fileCompressors.add(compressor);
            }
        }

        super.initialize();
    }

    @Override
    public void close() throws IOException {
        super.close();

        for (final EventFileCompressor compressor : fileCompressors) {
            compressor.shutdown();
        }

        if (compressionExecutor != null) {
            compressionExecutor.shutdown();
        }
    }

    @Override
    public void reindexLatestEvents(final EventIndex eventIndex) {
        final List<WriteAheadStorePartition> partitions = getPartitions();
        final int numPartitions = partitions.size();

        final List<Future<?>> futures = new ArrayList<>(numPartitions);
        final ExecutorService executor = Executors.newFixedThreadPool(numPartitions);

        for (final WriteAheadStorePartition partition : partitions) {
            futures.add(executor.submit(() -> partition.reindexLatestEvents(eventIndex)));
        }

        executor.shutdown();
        for (final Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Failed to re-index events because Thread was interrupted", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Failed to re-index events", e);
            }
        }
    }

    @Override
    protected List<WriteAheadStorePartition> getPartitions() {
        return partitions;
    }
}
