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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.provenance.journaling.JournaledProvenanceEvent;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.journaling.toc.TocJournalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneIndexManager implements IndexManager {
    private static final Logger logger = LoggerFactory.getLogger(LuceneIndexManager.class);
    
    private final JournalingRepositoryConfig config;
    private final ExecutorService queryExecutor;
    
    private final Map<String, List<LuceneIndexWriter>> writers = new HashMap<>();
    private final Map<String, AtomicLong> writerIndexes = new HashMap<>();
    private final ConcurrentMap<String, IndexSize> indexSizes = new ConcurrentHashMap<>();
    
    public LuceneIndexManager(final JournalingRepositoryConfig config, final ScheduledExecutorService workerExecutor, final ExecutorService queryExecutor) throws IOException {
        this.config = config;
        this.queryExecutor = queryExecutor;
        
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
                
                workerExecutor.scheduleWithFixedDelay(new Runnable() {
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

    @Override
    public void sync() throws IOException {
        for ( final List<LuceneIndexWriter> writerList : writers.values() ) {
            for ( final LuceneIndexWriter writer : writerList ) {
                writer.sync();
            }
        }
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
    
    @Override
    public <T> Set<T> withEachIndex(final IndexAction<T> action) throws IOException {
        final Set<T> results = new HashSet<>();
        final Map<String, Future<T>> futures = new HashMap<>();
        final Set<String> containerNames = config.getContainers().keySet();
        for (final String containerName : containerNames) {
            final Callable<T> callable = new Callable<T>() {
                @Override
                public T call() throws Exception {
                    try (final EventIndexSearcher searcher = newIndexSearcher(containerName)) {
                        return action.perform(searcher);
                    }
                }
            };
            
            final Future<T> future = queryExecutor.submit(callable);
            futures.put(containerName, future);
        }
        
        for ( final Map.Entry<String, Future<T>> entry : futures.entrySet() ) {
            try {
                final T result = entry.getValue().get();
                results.add(result);
            } catch (final ExecutionException ee) {
                final Throwable cause = ee.getCause();
                if ( cause instanceof IOException ) {
                    throw (IOException) cause;
                } else {
                    throw new RuntimeException("Failed to query Container " + entry.getKey() + " due to " + cause, cause);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        
        return results;
    }
    
    @Override
    public void withEachIndex(final VoidIndexAction action) throws IOException {
        withEachIndex(action, false);
    }
    
    @Override
    public void withEachIndex(final VoidIndexAction action, final boolean async) throws IOException {
        final Map<String, Future<?>> futures = new HashMap<>();
        final Set<String> containerNames = config.getContainers().keySet();

        for (final String containerName : containerNames) {
            final Callable<Object> callable = new Callable<Object>() {
                @Override
                public Object call() throws IOException {
                    try (final EventIndexSearcher searcher = newIndexSearcher(containerName)) {
                        action.perform(searcher);
                        return null;
                    } catch (final Throwable t) {
                        if ( async ) {
                            logger.error("Failed to perform action against container " + containerName + " due to " + t, t);
                            if ( logger.isDebugEnabled() ) {
                                logger.error("", t);
                            }
                            
                            return null;
                        } else {
                            throw new IOException("Failed to perform action against container " + containerName + " due to " + t, t);
                        }
                    }
                }
            };
            
            final Future<?> future = queryExecutor.submit(callable);
            futures.put(containerName, future);
        }
        
        if ( !async ) {
            for ( final Map.Entry<String, Future<?>> entry : futures.entrySet() ) {
                try {
                    // throw any exception thrown by runnable
                    entry.getValue().get();
                } catch (final ExecutionException ee) {
                    final Throwable cause = ee.getCause();
                    if ( cause instanceof IOException ) {
                        throw ((IOException) cause);
                    }
                    
                    throw new RuntimeException("Failed to query Partition " + entry.getKey() + " due to " + cause, cause);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    
    @Override
    public int getNumberOfIndices() {
        return config.getContainers().size();
    }
    
    @Override
    public void deleteEvents(final String containerName, final int sectionIndex, final Long journalId) throws IOException {
        final List<LuceneIndexWriter> writerList = writers.get(containerName);
        for ( final LuceneIndexWriter writer : writerList ) {
            writer.delete(containerName, String.valueOf(sectionIndex), journalId);
        }
    }
    
    @Override
    public void deleteEventsBefore(final String containerName, final int sectionIndex, final Long journalId) throws IOException {
        final List<LuceneIndexWriter> writerList = writers.get(containerName);
        for ( final LuceneIndexWriter writer : writerList ) {
            writer.deleteEventsBefore(containerName, String.valueOf(sectionIndex), journalId);
        }
    }
    
    @Override
    public void reindex(final String containerName, final int sectionIndex, final Long journalId, final File journalFile) throws IOException {
        deleteEvents(containerName, sectionIndex, journalId);
        
        final LuceneIndexWriter writer = getIndexWriter(containerName);
        try (final TocJournalReader reader = new TocJournalReader(containerName, String.valueOf(sectionIndex), journalId, journalFile)) {
            final List<JournaledProvenanceEvent> events = new ArrayList<>(1000);
            JournaledProvenanceEvent event;
            
            while ((event = reader.nextJournaledEvent()) != null) {
                events.add(event);
                if ( events.size() >= 1000 ) {
                    writer.index(events);
                    events.clear();
                }
            }
            
            if (!events.isEmpty() ) {
                writer.index(events);
            }
        }
    }
    
    @Override
    public long getNumberOfEvents() throws IOException {
        final AtomicLong totalCount = new AtomicLong(0L);
        withEachIndex(new VoidIndexAction() {
            @Override
            public void perform(final EventIndexSearcher searcher) throws IOException {
                totalCount.addAndGet(searcher.getNumberOfEvents());
            }
        });
        
        return totalCount.get();
    }
    
    @Override
    public void deleteOldEvents(final long earliestEventTimeToDelete) throws IOException {
        for ( final String containerName : config.getContainers().keySet() ) {
            final List<LuceneIndexWriter> writerList = writers.get(containerName);
            for ( final LuceneIndexWriter writer : writerList ) {
                writer.deleteOldEvents(earliestEventTimeToDelete);
            }
        }
    }
    
    
    @Override
    public long getSize(final String containerName) {
        // Cache index sizes so that we don't have to continually calculate it, as calculating it requires
        // disk accesses, which are quite expensive.
        final IndexSize indexSize = indexSizes.get(containerName);
        if ( indexSize != null && !indexSize.isExpired() ) {
            return indexSize.getSize();
        }
        
        final File containerFile = config.getContainers().get(containerName);
        final File indicesDir = new File(containerFile, "indices");
        
        final long size = getSize(indicesDir);
        indexSizes.put(containerName, new IndexSize(size));
        return size;
    }
    
    private long getSize(final File file) {
        if ( file.isDirectory() ) {
            long totalSize = 0L;
            
            final File[] children = file.listFiles();
            if ( children != null ) {
                for ( final File child : children ) {
                    totalSize += getSize(child);
                }
            }
            
            return totalSize;
        } else {
            return file.length();
        }
    }
    
    
    private static class IndexSize {
        private final long size;
        private final long expirationTime;
        
        public IndexSize(final long size) {
            this.size = size;
            this.expirationTime = System.currentTimeMillis() + 5000L;   // good for 5 seconds
        }
        
        public long getSize() {
            return size;
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expirationTime;
        }
    }
}
