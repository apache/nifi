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

package org.apache.nifi.controller.queue;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.events.EventReporter;

import java.util.Collection;
import java.util.Set;

public class BlockingSwappablePriorityQueue extends SwappablePriorityQueue {
    private final Object monitor = new Object();

    public BlockingSwappablePriorityQueue(final FlowFileSwapManager swapManager, final int swapThreshold, final EventReporter eventReporter, final FlowFileQueue flowFileQueue,
        final DropFlowFileAction dropAction, final String partitionName) {

        super(swapManager, swapThreshold, eventReporter, flowFileQueue, dropAction, partitionName);
    }

    @Override
    public void put(final FlowFileRecord flowFile) {
        super.put(flowFile);

        synchronized (monitor) {
            monitor.notify();
        }
    }

    @Override
    public void putAll(final Collection<FlowFileRecord> flowFiles) {
        super.putAll(flowFiles);

        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    public FlowFileRecord poll(final Set<FlowFileRecord> expiredRecords, final long expirationMillis, final long waitMillis) throws InterruptedException {
        final long maxTimestamp = System.currentTimeMillis() + waitMillis;

        synchronized (monitor) {
            FlowFileRecord flowFile = null;
            do {
                flowFile = super.poll(expiredRecords, expirationMillis);
                if (flowFile != null) {
                    return flowFile;
                }

                monitor.wait(waitMillis);
            } while (System.currentTimeMillis() < maxTimestamp);

            return null;
        }
    }

    @Override
    public void inheritQueueContents(final FlowFileQueueContents queueContents) {
        // We have to override this method and synchronize on monitor before calling super.inheritQueueContents.
        // If we don't do this, then our super class will obtain the write lock and call putAll, which will cause
        // us to synchronize on monitor AFTER obtaining the write lock (WriteLock then monitor).
        // If poll() is then called, we will synchronize on monitor, THEN attempt to obtain the write lock (monitor then WriteLock),
        // which would cause a deadlock.
        synchronized (monitor) {
            super.inheritQueueContents(queueContents);
        }
    }

}
