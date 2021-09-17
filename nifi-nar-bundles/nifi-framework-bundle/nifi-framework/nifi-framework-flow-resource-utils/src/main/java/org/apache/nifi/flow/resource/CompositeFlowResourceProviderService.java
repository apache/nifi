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
package org.apache.nifi.flow.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * This service implementation is capable to manage multiple {@code FlowResourceProviderWorker}
 * instances all pointing to a different external source, possibly using even different type of providers.
 */
final class CompositeFlowResourceProviderService implements FlowResourceProviderService {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeFlowResourceProviderService.class);

    private final String name;
    private final Set<FlowResourceProviderWorker> workers = new HashSet<>();
    private final CountDownLatch restrainStartupLatch;
    private volatile boolean started = false;

    CompositeFlowResourceProviderService(final String name, final Set<FlowResourceProviderWorker> workers, final CountDownLatch restrainStartupLatch) {
        this.name = name;
        this.workers.addAll(workers);
        this.restrainStartupLatch = restrainStartupLatch;
    }

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }

        LOGGER.info("Starting Flow Resource Provider Service ...");

        for (final FlowResourceProviderWorker worker : workers) {
            final Thread workerThread = getThread(worker);
            workerThread.start();
        }

        try {
            restrainStartupLatch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlowResourceProviderException("Starting Flow Resource Provider Service is interrupted");
        }

        LOGGER.info("Flow Resource Provider Service is started successfully");
    }

    @Override
    public synchronized void stop() {
        started = false;

        if (workers != null) {
            workers.forEach(FlowResourceProviderWorker::stop);
            workers.clear();
        }

        LOGGER.info("Flow Resource Provider Service is stopped");
    }

    private Thread getThread(final FlowResourceProviderWorker worker) {
        final Thread thread = new Thread(worker);
        thread.setName("Flow Resource Provider Service -  " + name +  " - " + worker.getName());
        thread.setDaemon(true);
        thread.setContextClassLoader(worker.getProvider().getClass().getClassLoader());
        return thread;
    }
}
