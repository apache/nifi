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
package org.apache.nifi.controller.status.history.questdb;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.GarbageCollectionStatus;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class BufferedStatusHistoryStorage implements StatusHistoryStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedStatusHistoryStorage.class);

    private final String id = UUID.randomUUID().toString();
    private final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newScheduledThreadPool(1, new BasicThreadFactory.Builder().namingPattern("BufferedStatusHistoryStorage-" + id + "-%d").build());

    private final StatusHistoryStorage storage;
    private final long persistFrequencyInMs;
    private final int persistBatchSize;

    private final BlockingQueue<CapturedStatus<NodeStatus>> nodeStatusQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<CapturedStatus<GarbageCollectionStatus>> garbageCollectionStatusQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<CapturedStatus<ProcessGroupStatus>> processGroupStatusQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<CapturedStatus<ConnectionStatus>> connectionStatusQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<CapturedStatus<RemoteProcessGroupStatus>> remoteProcessGroupStatusQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<CapturedStatus<ProcessorStatus>> processorStatusQueue = new LinkedBlockingQueue<>();

    public BufferedStatusHistoryStorage(final StatusHistoryStorage storage, final long persistFrequencyInMs, final int persistBatchSize) {
        this.storage = storage;
        this.persistFrequencyInMs = persistFrequencyInMs;
        this.persistBatchSize = persistBatchSize;
    }

    @Override
    public void init() {
        storage.init();
        final ScheduledFuture<?> future = scheduledExecutorService.scheduleWithFixedDelay(
                new BufferedStatusHistoryStorageWorker(), persistFrequencyInMs, persistFrequencyInMs, TimeUnit.MILLISECONDS);
        scheduledFutures.add(future);
        LOGGER.info("Flushing is initiated");
    }

    @Override
    public void close() {
        storage.close();
        LOGGER.debug("Flushing shutdown started");
        int cancelCompleted = 0;
        int cancelFailed = 0;

        for (final ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            final boolean cancelled = scheduledFuture.cancel(true);
            if (cancelled) {
                cancelCompleted++;
            } else {
                cancelFailed++;
            }
        }

        LOGGER.debug("Flushing shutdown task cancellation status: completed [{}] failed [{}]", cancelCompleted, cancelFailed);
        final List<Runnable> tasks = scheduledExecutorService.shutdownNow();
        LOGGER.debug("Scheduled Task Service shutdown remaining tasks [{}]", tasks.size());

    }

    @Override
    public List<StatusSnapshot> getConnectionSnapshots(final String componentId, final Date start, final Date end) {
        return storage.getConnectionSnapshots(componentId, start, end);
    }

    @Override
    public List<StatusSnapshot> getProcessGroupSnapshots(final String componentId, final Date start, final Date end) {
        return storage.getProcessGroupSnapshots(componentId, start, end);
    }

    @Override
    public List<StatusSnapshot> getRemoteProcessGroupSnapshots(final String componentId, final Date start, final Date end) {
        return storage.getRemoteProcessGroupSnapshots(componentId, start, end);
    }

    @Override
    public List<StatusSnapshot> getProcessorSnapshots(final String componentId, final Date start, final Date end) {
        return storage.getProcessorSnapshots(componentId, start, end);
    }

    @Override
    public List<StatusSnapshot> getProcessorSnapshotsWithCounters(final String componentId, final Date start, final Date end) {
        return storage.getProcessorSnapshotsWithCounters(componentId, start, end);
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionSnapshots(final Date start, final Date end) {
        return storage.getGarbageCollectionSnapshots(start, end);
    }

    @Override
    public List<StatusSnapshot> getNodeStatusSnapshots(final Date start, final Date end) {
        return storage.getNodeStatusSnapshots(start, end);
    }

    @Override
    public void storeNodeStatuses(final Collection<CapturedStatus<NodeStatus>> statuses) {
        nodeStatusQueue.addAll(statuses);
    }

    @Override
    public void storeGarbageCollectionStatuses(final Collection<CapturedStatus<GarbageCollectionStatus>> statuses) {
        garbageCollectionStatusQueue.addAll(statuses);
    }

    @Override
    public void storeProcessGroupStatuses(final Collection<CapturedStatus<ProcessGroupStatus>> statuses) {
        processGroupStatusQueue.addAll(statuses);
    }

    @Override
    public void storeConnectionStatuses(final Collection<CapturedStatus<ConnectionStatus>> statuses) {
        connectionStatusQueue.addAll(statuses);
    }

    @Override
    public void storeRemoteProcessorGroupStatuses(final Collection<CapturedStatus<RemoteProcessGroupStatus>> statuses) {
        remoteProcessGroupStatusQueue.addAll(statuses);
    }

    @Override
    public void storeProcessorStatuses(final Collection<CapturedStatus<ProcessorStatus>> statuses) {
        processorStatusQueue.addAll(statuses);
    }

    private class BufferedStatusHistoryStorageWorker implements Runnable {
        @Override
        public void run() {
            LOGGER.debug("Start flushing");
            flush(nodeStatusQueue, storage::storeNodeStatuses);
            flush(garbageCollectionStatusQueue, storage::storeGarbageCollectionStatuses);
            flush(processGroupStatusQueue, storage::storeProcessGroupStatuses);
            flush(connectionStatusQueue, storage::storeConnectionStatuses);
            flush(remoteProcessGroupStatusQueue, storage::storeRemoteProcessorGroupStatuses);
            flush(processorStatusQueue, storage::storeProcessorStatuses);
            LOGGER.debug("Finish flushing");
        }

        private <T> void flush(final BlockingQueue<T> source, final Consumer<Collection<T>> target) {
            final ArrayList<T> statusEntries = new ArrayList<>(persistBatchSize);
            source.drainTo(statusEntries, persistBatchSize);

            if (!statusEntries.isEmpty()) {
                try {
                    target.accept(statusEntries);
                } catch (final Exception e) {
                    LOGGER.error("Error during flushing buffered status history information.", e);
                }
            }
        }
    }
}
