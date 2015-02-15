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
package org.apache.nifi.provenance.journaling.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneIndexManager implements IndexManager {
    private static final Logger logger = LoggerFactory.getLogger(LuceneIndexManager.class);
    
    private final JournalingRepositoryConfig config;
    private final ScheduledExecutorService executor;
    
    private final Map<String, List<LuceneIndexWriter>> writers = new HashMap<>();
    private final Map<String, AtomicLong> writerIndexes = new HashMap<>();
    
    public LuceneIndexManager(final JournalingRepositoryConfig config, final ScheduledExecutorService executor) throws IOException {
        this.config = config;
        this.executor = executor;
        
        final int rolloverSeconds = (int) config.getJournalRolloverPeriod(TimeUnit.SECONDS);
        if ( !config.isReadOnly() ) {
            for ( final Map.Entry<String, File> entry : config.getContainers().entrySet() ) {
                final String containerName = entry.getKey();
                final File container = entry.getValue();
                
                final List<LuceneIndexWriter> writerList = new ArrayList<>(config.getIndexesPerContainer());
                writers.put(containerName, writerList);
                writerIndexes.put(containerName, new AtomicLong(0L));
                
                for ( int i=0; i < config.getIndexesPerContainer(); i++ ){
                    final File indexDir = new File(container, "indices/" + i);
                    writerList.add(new LuceneIndexWriter(indexDir, config));
                }
                
                executor.scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sync(containerName);
                        } catch (final Throwable t) {
                            logger.error("Failed to sync Provenance Repository Container {} due to {}", containerName, t);
                            if ( logger.isDebugEnabled() ) {
                                logger.error("", t);
                            }
                        }
                    }
                }, rolloverSeconds, rolloverSeconds, TimeUnit.SECONDS);
            }
        }
    }
    
    @Override
    public EventIndexSearcher newIndexSearcher(final String containerName) throws IOException {
        final File containerDir = config.getContainers().get(containerName);
        if ( containerDir == null ) {
            throw new IllegalArgumentException();
        }
        
        final List<EventIndexSearcher> searchers = new ArrayList<>();
        
        try {
            if (config.isReadOnly()) {
                for (int i=0; i < config.getIndexesPerContainer(); i++) {
                    final File indexDir = new File(containerName, "indices/" + i);
                    searchers.add(new LuceneIndexSearcher(indexDir));
                }
            } else {
                final List<LuceneIndexWriter> writerList = writers.get(containerName);
                for ( final LuceneIndexWriter writer : writerList ) {
                    searchers.add(writer.newIndexSearcher());
                }
            }
        } catch (final IOException ioe) {
            // If we failed to create a searcher, we need to close all that we've already created.
            for ( final EventIndexSearcher searcher : searchers ) {
                try {
                    searcher.close();
                } catch (final IOException ioe2) {
                    ioe.addSuppressed(ioe2);
                }
            }
            
            throw ioe;
        }
        
        return new MultiIndexSearcher(searchers);
    }
    
    @Override
    public LuceneIndexWriter getIndexWriter(final String container) {
        if (config.isReadOnly() ) {
            throw new IllegalStateException("Cannot obtain Index Writer because repository is read-only");
        }
        
        final AtomicLong index = writerIndexes.get(container);
        if (index == null ) {
            throw new IllegalArgumentException();
        }
        
        final long curVal = index.get();
        final List<LuceneIndexWriter> writerList = writers.get(container);
        return writerList.get((int) (curVal % writerList.size()));
    }

    @Override
    public Long getMaxEventId(final String container, final String section) throws IOException {
        final List<LuceneIndexWriter> writerList = writers.get(container);
        if ( writerList == null ) {
            return null;
        }

        Long max = null;
        for ( final LuceneIndexWriter writer : writerList ) {
            try (final EventIndexSearcher searcher = writer.newIndexSearcher()) {
                final Long maxForWriter = searcher.getMaxEventId(container, section);
                if ( maxForWriter != null ) {
                    if (max == null || maxForWriter.longValue() > max.longValue() ) {
                        max = maxForWriter;
                    }
                }
            }
        }
        
        return max;
    }

    
    private void sync(final String containerName) throws IOException {
        final AtomicLong index = writerIndexes.get(containerName);
        final long curIndex = index.getAndIncrement();
        
        final List<LuceneIndexWriter> writerList = writers.get(containerName);
        final EventIndexWriter toSync = writerList.get((int) (curIndex % writerList.size()));
        toSync.sync();
    }

    @Override
    public void close() throws IOException {
        for ( final List<LuceneIndexWriter> writerList : writers.values() ) {
            for ( final LuceneIndexWriter writer : writerList ) {
                try {
                    writer.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close {} due to {}", writer, ioe);
                    if ( logger.isDebugEnabled() ) {
                        logger.warn("", ioe);
                    }
                }
            }
        }        
    }
}
