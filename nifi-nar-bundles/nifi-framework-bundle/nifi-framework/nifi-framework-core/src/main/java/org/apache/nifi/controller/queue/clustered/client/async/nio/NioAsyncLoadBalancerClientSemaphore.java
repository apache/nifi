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
package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * This fine grain lock intends to limit the access for every given (remote) partitions and protect them from concurrent access.
 * The implementation does not work in a reentrant manner but thread safe.
 */
public class NioAsyncLoadBalancerClientSemaphore {
    private final static Logger LOGGER = LoggerFactory.getLogger(NioAsyncLoadBalancerClientSemaphore.class);

    private final Object monitor = new Object();
    private final Set<RegisteredPartition> partitionsInUse = new HashSet<>();

    /**
     * Clients might attempt to acquire usage on a partition using this method.
     *
     * @param partitionToUse The remote partition the client wants to work with.
     *
     * @returs If the acquirement is successful (no other client has acquired the given partition), the method returns with a {@code Reservation} and the partition is safe to use.
     * This reservation might be called to free the partition. Otherwise the result is an empty optional.
     */
    Optional<Reservation> acquire(final RegisteredPartition partitionToUse) {
        synchronized (monitor) {
            if (partitionsInUse.contains(partitionToUse)) {
                LOGGER.debug("Attempted to lock partition for connection {} and node {} unsuccessfully", partitionToUse.getConnectionId(), partitionToUse.getNodeIdentifier().getId());
                return Optional.empty();
            } else {
                partitionsInUse.add(partitionToUse);
                LOGGER.debug("Partition for connection {} and node {} is locked", partitionToUse.getConnectionId(), partitionToUse.getNodeIdentifier().getId());
                return Optional.of(new Reservation(partitionToUse));
            }
        }
    }

    private void release(final RegisteredPartition usedPartition) {
        synchronized (monitor) {
            if (partitionsInUse.contains(usedPartition)) {
                LOGGER.debug("Released partition for connection {} and node {}", usedPartition.getConnectionId(), usedPartition.getNodeIdentifier().getId());
                partitionsInUse.remove(usedPartition);
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Tried to release partition for connection {} and node {} but was not locked", usedPartition.getConnectionId(), usedPartition.getNodeIdentifier().getId());
            }
        }
    }

    /**
     * Represents the usage of the given partition. Client can use the instance to release partition after finishing the work with it.
     */
    public final class Reservation {
        private final RegisteredPartition usedPartition;

        private Reservation(RegisteredPartition usedPartition) {
            this.usedPartition = usedPartition;
        }

        /**
         * After the client finished working with the partition, it might be released by calling this method.
         * It is eminent that the client must release the partition after it finished it's work with it, otherwise the partition might stuck.
         */
        public void release() {
            NioAsyncLoadBalancerClientSemaphore.this.release(usedPartition);
        }
    }
}
