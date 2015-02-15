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
package org.apache.nifi.provenance.journaling.partition;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.journaling.index.IndexManager;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueuingPartitionManager implements PartitionManager {
    
    private static final Logger logger = LoggerFactory.getLogger(QueuingPartitionManager.class);
    
    private final JournalingRepositoryConfig config;
    private final BlockingQueue<Partition> partitionQueue;
    private final JournalingPartition[] partitionArray;
    private final ExecutorService executor;
    private volatile boolean shutdown = false;
    
    private final AtomicInteger blacklistedCount = new AtomicInteger(0);
    
    public QueuingPartitionManager(final IndexManager indexManager, final JournalingRepositoryConfig config, final ExecutorService executor) throws IOException {
        this.config = config;
        this.partitionQueue = new LinkedBlockingQueue<>(config.getPartitionCount());
        this.partitionArray = new JournalingPartition[config.getPartitionCount()];
        
        final List<Tuple<String, File>> containerTuples = new ArrayList<>(config.getContainers().size());
        for ( final Map.Entry<String, File> entry : config.getContainers().entrySet() ) {
            containerTuples.add(new Tuple<>(entry.getKey(), entry.getValue()));
        }
        
        for (int i=0; i < config.getPartitionCount(); i++) {
            final Tuple<String, File> tuple = containerTuples.get(i % containerTuples.size());
            final File section = new File(tuple.getValue(), String.valueOf(i));
            
            final JournalingPartition partition = new JournalingPartition(indexManager, tuple.getKey(), i, section, config, executor);
            partitionQueue.offer(partition);
            partitionArray[i] = partition;
        }
        
        this.executor = executor;
    }
    
    @Override
    public void shutdown() {
        this.shutdown = true;
        
        for ( final Partition partition : partitionArray ) {
            partition.shutdown();
        }
    }
    
    private Partition nextPartition() {
        Partition partition = null;
        
        while(partition == null) {
            if (shutdown) {
                throw new RuntimeException("Journaling Provenance Repository is shutting down");
            }
            
            try {
                partition = partitionQueue.poll(1, TimeUnit.SECONDS);
            } catch (final InterruptedException ie) {
            }
            
            if ( partition == null ) {
                if ( blacklistedCount.get() >= config.getPartitionCount() ) {
                    throw new RuntimeException("Cannot persist to the Journal Provenance Repository because all partitions have been blacklisted due to write failures");
                }
            }
        }
        
        return partition;
    }
    
    @Override
    public <T> T withPartition(final PartitionAction<T> action, final boolean writeAction) throws IOException {
        final Partition partition = nextPartition();

        boolean ioe = false;
        try {
            return action.perform(partition);
        } catch (final IOException e) {
            ioe = true;
            throw e;
        } finally {
            if ( ioe && writeAction ) {
                // We failed to write to this Partition. This partition will no longer be usable until NiFi is restarted!
                blacklistedCount.incrementAndGet();
            } else {
                partitionQueue.offer(partition);
            }
        }
    }
    
    @Override
    public void withPartition(final VoidPartitionAction action, final boolean writeAction) throws IOException {
        final Partition partition = nextPartition();

        boolean ioe = false;
        try {
            action.perform(partition);
        } catch (final IOException e) {
            ioe = true;
            throw e;
        } finally {
            if ( ioe && writeAction ) {
                // We failed to write to this Partition. This partition will no longer be usable until NiFi is restarted!
                blacklistedCount.incrementAndGet();
            } else {
                partitionQueue.offer(partition);
            }
        }
    }

    
    @Override
    public <T> Set<T> withEachPartition(final PartitionAction<T> action) throws IOException {
        final Set<T> results = new HashSet<>(partitionArray.length);
        
        // TODO: Do not use blacklisted partitions.
        final Map<Partition, Future<T>> futures = new HashMap<>(partitionArray.length);
        for ( final Partition partition : partitionArray ) {
            final Callable<T> callable = new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return action.perform(partition);
                }
            };
            
            final Future<T> future = executor.submit(callable);
            futures.put(partition, future);
        }
        
        for ( final Map.Entry<Partition, Future<T>> entry : futures.entrySet() ) {
            try {
                final T result = entry.getValue().get();
                results.add(result);
            } catch (final ExecutionException ee) {
                final Throwable cause = ee.getCause();
                if ( cause instanceof IOException ) {
                    throw (IOException) cause;
                } else {
                    throw new RuntimeException("Failed to query Partition " + entry.getKey() + " due to " + cause, cause);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        
        return results;
    }
    
    @Override
    public <T> Set<T> withEachPartitionSerially(final PartitionAction<T> action) throws IOException {
        // TODO: Do not use blacklisted partitions.
        final Set<T> results = new HashSet<>(partitionArray.length);
        for ( final Partition partition : partitionArray ) {
            results.add( action.perform(partition) );
        }
        
        return results;
    }
    
    @Override
    public void withEachPartition(final VoidPartitionAction action, final boolean async) {
        // TODO: Do not use blacklisted partitions.
        final Map<Partition, Future<?>> futures = new HashMap<>(partitionArray.length);
        for ( final Partition partition : partitionArray ) {
            final Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        action.perform(partition);
                    } catch (final Throwable t) {
                        logger.error("Failed to perform action against " + partition + " due to " + t);
                        if ( logger.isDebugEnabled() ) {
                            logger.error("", t);
                        }
                    }
                }
            };
            
            final Future<?> future = executor.submit(runnable);
            futures.put(partition, future);
        }
        
        if ( !async ) {
            for ( final Map.Entry<Partition, Future<?>> entry : futures.entrySet() ) {
                try {
                    // throw any exception thrown by runnable
                    entry.getValue().get();
                } catch (final ExecutionException ee) {
                    final Throwable cause = ee.getCause();
                    throw new RuntimeException("Failed to query Partition " + entry.getKey() + " due to " + cause, cause);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
