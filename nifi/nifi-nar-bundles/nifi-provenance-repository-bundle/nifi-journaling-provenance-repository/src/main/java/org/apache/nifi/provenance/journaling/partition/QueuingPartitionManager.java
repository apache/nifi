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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.journaling.index.IndexManager;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueuingPartitionManager implements PartitionManager {
    
    private static final Logger logger = LoggerFactory.getLogger(QueuingPartitionManager.class);
    
    private final IndexManager indexManager;
    private final JournalingRepositoryConfig config;
    private final BlockingQueue<Partition> partitionQueue;
    private final JournalingPartition[] partitionArray;
    private final AtomicLong eventIdGenerator;
    private volatile boolean shutdown = false;
    
    private final Set<Partition> blackListedPartitions = Collections.synchronizedSet(new HashSet<Partition>());
    
    public QueuingPartitionManager(final IndexManager indexManager, final AtomicLong eventIdGenerator, final JournalingRepositoryConfig config, final ScheduledExecutorService workerExecutor, final ExecutorService compressionExecutor) throws IOException {
        this.indexManager = indexManager;
        this.config = config;
        this.eventIdGenerator = eventIdGenerator;
        
        // We can consider using a PriorityQueue here instead. Keep track of how many Partitions are being written
        // to for each container, as a container usually maps to a physical drive. Then, prioritize the queue
        // so that the partitions that belong to Container A get a higher priority than those belonging to Container B
        // if there are currently more partitions on Container B being written to (i.e., we prefer a partition for the
        // container that is the least used at this moment). Would require significant performance testing to see if it
        // really provides any benefit.
        this.partitionQueue = new LinkedBlockingQueue<>(config.getPartitionCount());
        this.partitionArray = new JournalingPartition[config.getPartitionCount()];
        
        final List<Tuple<String, File>> containerTuples = new ArrayList<>(config.getContainers().size());
        for ( final Map.Entry<String, File> entry : config.getContainers().entrySet() ) {
            containerTuples.add(new Tuple<>(entry.getKey(), entry.getValue()));
        }
        
        final Map<String, AtomicLong> containerSizes = new HashMap<>();
        for ( final String containerName : config.getContainers().keySet() ) {
            containerSizes.put(containerName, new AtomicLong(0L));
        }
        
        for (int i=0; i < config.getPartitionCount(); i++) {
            final Tuple<String, File> tuple = containerTuples.get(i % containerTuples.size());
            final File section = new File(tuple.getValue(), String.valueOf(i));
            
            final String containerName = tuple.getKey();
            final JournalingPartition partition = new JournalingPartition(indexManager, containerName, i, 
                    section, config, containerSizes.get(containerName), compressionExecutor);
            partition.restore();
            partitionQueue.offer(partition);
            partitionArray[i] = partition;
        }
        
        workerExecutor.scheduleWithFixedDelay(new CheckBlackListedPartitions(), 30, 30, TimeUnit.SECONDS);
    }
    
    @Override
    public void shutdown() {
        this.shutdown = true;
        
        for ( final Partition partition : partitionArray ) {
            partition.shutdown();
        }
    }
    
    private Partition nextPartition(final boolean writeAction) {
        Partition partition = null;
        
        final List<Partition> partitionsSkipped = new ArrayList<>();
        try {
            while (partition == null) {
                if (shutdown) {
                    throw new RuntimeException("Journaling Provenance Repository is shutting down");
                }
                
                try {
                    partition = partitionQueue.poll(1, TimeUnit.SECONDS);
                } catch (final InterruptedException ie) {
                }
                
                if ( partition == null ) {
                    if ( blackListedPartitions.size() >= config.getPartitionCount() ) {
                        throw new RuntimeException("Cannot persist to the Journal Provenance Repository because all partitions have been blacklisted due to write failures");
                    }
                    
                    // we are out of partitions. Add back all of the partitions that we skipped so we 
                    // can try them again.
                    partitionQueue.addAll(partitionsSkipped);
                    partitionsSkipped.clear();
                } else if (writeAction) {
                    // determine if the container is full.
                    final String containerName = partition.getContainerName();
                    long desiredMaxContainerCapacity = config.getMaxCapacity(containerName);
                    
                    // If no max capacity set for the container itself, use 1/N of repo max
                    // where N is the number of containers
                    if ( desiredMaxContainerCapacity == config.getMaxStorageCapacity() ) {
                        desiredMaxContainerCapacity = config.getMaxStorageCapacity() / config.getContainers().size();
                    }
                    
                    // if the partition is more than 10% over its desired capacity, we don't want to write to it.
                    if ( partition.getContainerSize() > 1.1 * desiredMaxContainerCapacity ) {
                        partitionsSkipped.add(partition);
                        continue;
                    }
                }
            }
        } finally {
            partitionQueue.addAll( partitionsSkipped );
        }
        
        return partition;
    }
    
    
    private void blackList(final Partition partition) {
        blackListedPartitions.add(partition);
    }
    
    @Override
    public <T> T withPartition(final PartitionAction<T> action, final boolean writeAction) throws IOException {
        final Partition partition = nextPartition(writeAction);

        boolean ioe = false;
        try {
            return action.perform(partition);
        } catch (final IOException e) {
            ioe = true;
            throw e;
        } finally {
            if ( ioe && writeAction ) {
                blackList(partition);
            } else {
                partitionQueue.offer(partition);
            }
        }
    }
    
    @Override
    public void withPartition(final VoidPartitionAction action, final boolean writeAction) throws IOException {
        final Partition partition = nextPartition(writeAction);

        boolean ioe = false;
        try {
            action.perform(partition);
        } catch (final IOException e) {
            ioe = true;
            throw e;
        } finally {
            if ( ioe && writeAction ) {
                blackList(partition);
            } else {
                partitionQueue.offer(partition);
            }
        }
    }

    
//    @Override
//    public <T> Set<T> withEachPartition(final PartitionAction<T> action) throws IOException {
//        if ( writeAction && blackListedPartitions.size() > 0 ) {
//            throw new IOException("Cannot perform action {} because at least one partition has been blacklisted (i.e., writint to the partition failed)");
//        }
//
//        final Set<T> results = new HashSet<>(partitionArray.length);
//        
//        final Map<Partition, Future<T>> futures = new HashMap<>(partitionArray.length);
//        for ( final Partition partition : partitionArray ) {
//            final Callable<T> callable = new Callable<T>() {
//                @Override
//                public T call() throws Exception {
//                    return action.perform(partition);
//                }
//            };
//            
//            final Future<T> future = executor.submit(callable);
//            futures.put(partition, future);
//        }
//        
//        for ( final Map.Entry<Partition, Future<T>> entry : futures.entrySet() ) {
//            try {
//                final T result = entry.getValue().get();
//                results.add(result);
//            } catch (final ExecutionException ee) {
//                final Throwable cause = ee.getCause();
//                if ( cause instanceof IOException ) {
//                    throw (IOException) cause;
//                } else {
//                    throw new RuntimeException("Failed to query Partition " + entry.getKey() + " due to " + cause, cause);
//                }
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        
//        return results;
//    }
    
    @Override
    public <T> Set<T> withEachPartitionSerially(final PartitionAction<T> action, final boolean writeAction) throws IOException {
        if ( writeAction && blackListedPartitions.size() > 0 ) {
            throw new IOException("Cannot perform action {} because at least one partition has been blacklisted (i.e., writint to the partition failed)");
        }
        
        final Set<T> results = new HashSet<>(partitionArray.length);
        for ( final Partition partition : partitionArray ) {
            results.add( action.perform(partition) );
        }
        
        return results;
    }
    
    @Override
    public void withEachPartitionSerially(final VoidPartitionAction action, final boolean writeAction) throws IOException {
        if ( writeAction && blackListedPartitions.size() > 0 ) {
            throw new IOException("Cannot perform action {} because at least one partition has been blacklisted (i.e., writint to the partition failed)");
        }
        
        for ( final Partition partition : partitionArray ) {
            action.perform(partition);
        }
    }
    
//    @Override
//    public void withEachPartition(final VoidPartitionAction action, final boolean async) {
//        // TODO: skip blacklisted partitions
//        final Map<Partition, Future<?>> futures = new HashMap<>(partitionArray.length);
//        for ( final Partition partition : partitionArray ) {
//            final Runnable runnable = new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        action.perform(partition);
//                    } catch (final Throwable t) {
//                        logger.error("Failed to perform action against " + partition + " due to " + t);
//                        if ( logger.isDebugEnabled() ) {
//                            logger.error("", t);
//                        }
//                    }
//                }
//            };
//            
//            final Future<?> future = executor.submit(runnable);
//            futures.put(partition, future);
//        }
//        
//        if ( !async ) {
//            for ( final Map.Entry<Partition, Future<?>> entry : futures.entrySet() ) {
//                try {
//                    // throw any exception thrown by runnable
//                    entry.getValue().get();
//                } catch (final ExecutionException ee) {
//                    final Throwable cause = ee.getCause();
//                    throw new RuntimeException("Failed to query Partition " + entry.getKey() + " due to " + cause, cause);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
//    }
    
    private long getTotalSize() {
        long totalSize = 0L;

        for ( final JournalingPartition partition : partitionArray ) {
            totalSize += partition.getPartitionSize();
        }
        
        for ( final String containerName : config.getContainers().keySet() ) {
            totalSize += indexManager.getSize(containerName);
        }
        
        return totalSize;
    }
    
    
    /**
     * Responsible for looking at partitions that have been marked as blacklisted and checking if they
     * are able to be written to now. If so, adds them back to the partition queue; otherwise, leaves
     * them as blacklisted
     */
    private class CheckBlackListedPartitions implements Runnable {
        @Override
        public void run() {
            final Set<Partition> reclaimed = new HashSet<>();
            
            final Set<Partition> partitions = new HashSet<>(blackListedPartitions);
            for ( final Partition partition : partitions ) {
                final long nextId = eventIdGenerator.get();
                if ( nextId <= 0 ) {
                    // we don't have an ID to use yet. Don't attempt to do anything yet.
                    return;
                }
                
                try {
                    partition.verifyWritable(nextId);
                    reclaimed.add(partition);
                 } catch (final IOException ioe) {
                     logger.debug("{} is still blackListed due to {}", partition, ioe);
                 }
            }
            
            // any partition that is reclaimable is now removed from the set of blacklisted
            // partitions and added back to our queue of partitions
            blackListedPartitions.removeAll(reclaimed);
            partitionQueue.addAll(reclaimed);
        }
    }
    
    
    @Override
    public void deleteEventsBasedOnSize() {
        final Map<String, List<JournalingPartition>> containerPartitionMap = new HashMap<>();
        
        for ( final JournalingPartition partition : partitionArray ) {
            final String container = partition.getContainerName();
            List<JournalingPartition> list = containerPartitionMap.get(container);
            if ( list == null ) {
                list = new ArrayList<>();
                containerPartitionMap.put(container, list);
            }
            
            list.add(partition);
        }
        
        int iterations = 0;
        for ( final String containerName : config.getContainers().keySet() ) {
            // continue as long as we need to delete data from this container.
            while (true) {
                // don't hammer the disks if we can't delete anything
                if ( iterations++ > 0 ) {
                    try {
                        Thread.sleep(1000L);
                    } catch (final InterruptedException ie) {}
                }
                
                final List<JournalingPartition> containerPartitions = containerPartitionMap.get(containerName);
                final long containerSize = containerPartitions.get(0).getContainerSize();
                final long maxContainerCapacity = config.getMaxCapacity(containerName);
                if ( containerSize < maxContainerCapacity ) {
                    break;
                }
                
                logger.debug("Container {} exceeds max capacity of {} bytes with a size of {} bytes; deleting oldest events", containerName, maxContainerCapacity, containerSize);
                
                // container is too large. Delete oldest journal from each partition in this container.
                for ( final Partition partition : containerPartitions ) {
                    try {
                        partition.deleteOldest();
                    } catch (final IOException ioe) {
                        logger.error("Failed to delete events from {} due to {}", partition, ioe.toString());
                        if ( logger.isDebugEnabled() ) {
                            logger.error("", ioe);
                        }
                    }
                }
            }
        }
        
        long totalSize;
        iterations = 0;
        while ((totalSize = getTotalSize()) >= config.getMaxStorageCapacity()) {
            logger.debug("Provenance Repository exceeds max capacity of {} bytes with a size of {}; deleting oldest events", config.getMaxStorageCapacity(), totalSize);
            
            // don't hammer the disks if we can't delete anything
            if ( iterations++ > 0 ) {
                try {
                    Thread.sleep(1000L);
                } catch (final InterruptedException ie) {}
            }

            for ( final Partition partition : partitionArray ) {
                try {
                    partition.deleteOldest();
                } catch (final IOException ioe) {
                    logger.error("Failed to delete events from {} due to {}", partition, ioe.toString());
                    if ( logger.isDebugEnabled() ) {
                        logger.error("", ioe);
                    }
                }
            }
            
            // don't hammer the disks if we can't delete anything
            try {
                Thread.sleep(1000L);
            } catch (final InterruptedException ie) {}
        }
    }
}
