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

package org.apache.nifi.c2.client.service;

import static java.util.Optional.ofNullable;
import static java.util.function.Predicate.not;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C2HeartbeatManager implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(C2HeartbeatManager.class);

    private final C2Client client;
    private final C2HeartbeatFactory c2HeartbeatFactory;
    private final ReentrantLock heartbeatLock;
    private final Supplier<RuntimeInfoWrapper> runtimeInfoSupplier;
    private final C2OperationManager c2OperationManager;

    public C2HeartbeatManager(C2Client client, C2HeartbeatFactory c2HeartbeatFactory, ReentrantLock heartbeatLock, Supplier<RuntimeInfoWrapper> runtimeInfoSupplier,
                              C2OperationManager c2OperationManager) {
        this.client = client;
        this.c2HeartbeatFactory = c2HeartbeatFactory;
        this.heartbeatLock = heartbeatLock;
        this.runtimeInfoSupplier = runtimeInfoSupplier;
        this.c2OperationManager = c2OperationManager;
    }

    @Override
    public void run() {
        if (!heartbeatLock.tryLock()) {
            LOGGER.debug("Heartbeat lock is hold by another thread, skipping heartbeat sending");
            return;
        }
        try {
            LOGGER.debug("Heartbeat lock is acquired, sending heartbeat");
            C2Heartbeat c2Heartbeat = c2HeartbeatFactory.create(runtimeInfoSupplier.get());
            client.publishHeartbeat(c2Heartbeat).ifPresent(this::processResponse);
        } catch (Exception e) {
            LOGGER.error("Failed to send/process heartbeat", e);
        } finally {
            heartbeatLock.unlock();
            LOGGER.debug("Heartbeat unlocked lock and heartbeat is sent successfully");
        }
    }

    private void processResponse(C2HeartbeatResponse response) {
        ofNullable(response.getRequestedOperations())
            .filter(not(List::isEmpty))
            .ifPresentOrElse(operations -> {
                    LOGGER.info("Received {} operations from the C2 server", operations.size());
                    operations.forEach(c2OperationManager::add);
                },
                () -> LOGGER.debug("No operations received from the C2 server")
            );
    }
}
