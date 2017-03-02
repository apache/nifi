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
package org.apache.nifi.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.Connectables;

public class EventDrivenWorkerQueue implements WorkerQueue {

    private final Object workMonitor = new Object();

    private final Map<Connectable, Worker> workerMap = new HashMap<>();   // protected by synchronizing on workMonitor
    private final WorkerReadyQueue workerQueue;

    public EventDrivenWorkerQueue(final boolean clustered, final boolean primary, final ProcessScheduler scheduler) {
        workerQueue = new WorkerReadyQueue(scheduler);
        workerQueue.setClustered(clustered);
        workerQueue.setPrimary(primary);
    }

    @Override
    public void setClustered(final boolean clustered) {
        workerQueue.setClustered(clustered);
    }

    @Override
    public void setPrimary(final boolean primary) {
        workerQueue.setPrimary(primary);
    }

    @Override
    public Worker poll(final long timeout, final TimeUnit timeUnit) {
        final long maxTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
        while (System.currentTimeMillis() < maxTime) {
            synchronized (workMonitor) {
                final Worker worker = workerQueue.poll();
                if (worker == null) {
                    // nothing to do. wait until we have something to do.
                    final long timeLeft = maxTime - System.currentTimeMillis();
                    if (timeLeft <= 0) {
                        return null;
                    }

                    try {
                        workMonitor.wait(timeLeft);
                    } catch (final InterruptedException ignored) {
                    }
                } else {
                    // Decrement the amount of work there is to do for this worker.
                    final int workLeft = worker.decrementEventCount();
                    if (workLeft > 0) {
                        workerQueue.offer(worker);
                    }

                    return worker;
                }
            }
        }

        return null;
    }

    @Override
    public void offer(final Connectable connectable) {
        synchronized (workMonitor) {
            Worker worker = workerMap.get(connectable);
            if (worker == null) {
                // if worker is null, then it has not been scheduled to run; ignore the event.
                return;
            }

            final int countBefore = worker.incrementEventCount();
            if (countBefore < 0) {
                worker.setWorkCount(1);
            }
            if (countBefore <= 0) {
                // If countBefore > 0 then it's already on the queue, so just incrementing its counter is sufficient.
                workerQueue.offer(worker);
            }

            workMonitor.notify();
        }
    }

    private int getWorkCount(final Connectable connectable) {
        int sum = 0;
        for (final Connection connection : connectable.getIncomingConnections()) {
            sum += connection.getFlowFileQueue().size().getObjectCount();
        }
        return sum;
    }

    @Override
    public void resumeWork(final Connectable connectable) {
        synchronized (workMonitor) {
            final int workCount = getWorkCount(connectable);
            final Worker worker = new Worker(connectable);
            workerMap.put(connectable, worker);

            if (workCount > 0) {
                worker.setWorkCount(workCount);
                workerQueue.offer(worker);
                workMonitor.notify();
            }
        }
    }

    @Override
    public void suspendWork(final Connectable connectable) {
        synchronized (workMonitor) {
            final Worker worker = this.workerMap.remove(connectable);
            if (worker == null) {
                return;
            }

            worker.resetWorkCount();
            workerQueue.remove(worker);
        }
    }

    public static class Worker implements EventBasedWorker {

        private final Connectable connectable;
        private final AtomicInteger workCount = new AtomicInteger(0);

        public Worker(final Connectable connectable) {
            this.connectable = connectable;
        }

        @Override
        public Connectable getConnectable() {
            return connectable;
        }

        @Override
        public int decrementEventCount() {
            return workCount.decrementAndGet();
        }

        @Override
        public int incrementEventCount() {
            return workCount.getAndIncrement();
        }

        void resetWorkCount() {
            workCount.set(0);
        }

        void setWorkCount(final int workCount) {
            this.workCount.set(workCount);
        }
    }

    @SuppressWarnings("serial")
    private static class WorkerReadyQueue extends LinkedList<Worker> {

        private final ProcessScheduler scheduler;

        private volatile boolean clustered = false;
        private volatile boolean primary = false;

        public WorkerReadyQueue(final ProcessScheduler scheduler) {
            this.scheduler = scheduler;
        }

        public void setClustered(final boolean clustered) {
            this.clustered = clustered;
        }

        public void setPrimary(final boolean primary) {
            this.primary = primary;
        }

        @Override
        public Worker poll() {
            final List<Worker> putBack = new ArrayList<>();

            Worker worker;
            try {
                while ((worker = super.poll()) != null) {
                    final DelayProcessingReason reason = getDelayReason(worker);
                    if (reason == null) {
                        return worker;
                    } else {
                        // Worker is not ready. We may want to add him back to the queue, depending on the reason that he is unready.
                        switch (reason) {
                            case YIELDED:
                            case ISOLATED:
                            case DESTINATION_FULL:
                            case ALL_WORK_PENALIZED:
                            case NO_WORK:
                            case TOO_MANY_THREADS:
                                // there will not be an event that triggers this to happen, so we add this worker back to the queue.
                                putBack.add(worker);
                                break;
                            default:
                            case NOT_RUNNING:
                                // There's no need to check if this worker is available again until a another event
                                // occurs. Therefore, we keep him off of the queue and reset his work count
                                worker.resetWorkCount();
                                break;
                        }
                    }
                }
            } finally {
                if (!putBack.isEmpty()) {
                    super.addAll(putBack);
                }
            }

            return null;
        }

        private DelayProcessingReason getDelayReason(final Worker worker) {
            final Connectable connectable = worker.getConnectable();

            if (ScheduledState.RUNNING != connectable.getScheduledState()) {
                return DelayProcessingReason.NOT_RUNNING;
            }

            if (connectable.getYieldExpiration() > System.currentTimeMillis()) {
                return DelayProcessingReason.YIELDED;
            }

            // For Remote Output Ports,
            int availableRelationshipCount = 0;
            if (!connectable.getRelationships().isEmpty()) {
                availableRelationshipCount = getAvailableRelationshipCount(connectable);

                if (availableRelationshipCount == 0) {
                    return DelayProcessingReason.DESTINATION_FULL;
                }
            }

            if (connectable.hasIncomingConnection() && !Connectables.flowFilesQueued(connectable)) {
                return DelayProcessingReason.NO_WORK;
            }

            final int activeThreadCount = scheduler.getActiveThreadCount(worker.getConnectable());
            final int maxThreadCount = worker.getConnectable().getMaxConcurrentTasks();
            if (maxThreadCount > 0 && activeThreadCount >= maxThreadCount) {
                return DelayProcessingReason.TOO_MANY_THREADS;
            }

            if (connectable instanceof ProcessorNode) {
                final ProcessorNode procNode = (ProcessorNode) connectable;
                if (procNode.isIsolated() && clustered && !primary) {
                    return DelayProcessingReason.ISOLATED;
                }

                final boolean triggerWhenAnyAvailable = procNode.isTriggerWhenAnyDestinationAvailable();
                final boolean allDestinationsAvailable = availableRelationshipCount == procNode.getRelationships().size();
                if (!triggerWhenAnyAvailable && !allDestinationsAvailable) {
                    return DelayProcessingReason.DESTINATION_FULL;
                }
            }

            return null;
        }

        private int getAvailableRelationshipCount(final Connectable connectable) {
            int count = 0;
            for (final Relationship relationship : connectable.getRelationships()) {
                final Collection<Connection> connections = connectable.getConnections(relationship);

                if (connections == null || connections.isEmpty()) {
                    if (connectable.isAutoTerminated(relationship)) {
                        // If the relationship is auto-terminated, consider it available.
                        count++;
                    }
                } else {
                    boolean available = true;
                    for (final Connection connection : connections) {
                        if (connection.getSource() == connection.getDestination()) {
                            // don't count self-loops
                            continue;
                        }

                        if (connection.getFlowFileQueue().isFull()) {
                            available = false;
                        }
                    }

                    if (available) {
                        count++;
                    }
                }
            }

            return count;
        }
    }

    private static enum DelayProcessingReason {

        YIELDED,
        DESTINATION_FULL,
        NO_WORK,
        ALL_WORK_PENALIZED,
        ISOLATED,
        NOT_RUNNING,
        TOO_MANY_THREADS;
    }
}
