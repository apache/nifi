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
package org.apache.nifi.jms.processors;

import jakarta.jms.ConnectionFactory;
import org.apache.nifi.jms.cf.IJMSConnectionFactoryProvider;
import org.apache.nifi.logging.ComponentLog;
import org.springframework.jms.core.JmsTemplate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

final class JmsWorkerLifecycle<T extends JMSWorker> {

    private final IJMSConnectionFactoryProvider connectionFactoryProvider;
    private final int maxPoolSize;
    private final ComponentLog logger;

    private boolean cycleOpen = true;
    private Generation<T> currentGeneration;

    JmsWorkerLifecycle(final IJMSConnectionFactoryProvider connectionFactoryProvider, final int maxPoolSize, final ComponentLog logger) {
        this.connectionFactoryProvider = connectionFactoryProvider;
        this.maxPoolSize = maxPoolSize;
        this.logger = logger;
        this.currentGeneration = new Generation<>(maxPoolSize);
    }

    synchronized Generation<T> captureGeneration() {
        return currentGeneration;
    }

    synchronized T pollIdleWorker(final Generation<T> generation) {
        return isCurrentOpenGeneration(generation) ? generation.idleWorkers.poll() : null;
    }

    synchronized boolean canCreateWorker(final Generation<T> generation) {
        return isCurrentOpenGeneration(generation);
    }

    boolean registerWorker(final Generation<T> generation, final T worker) {
        boolean accepted;
        synchronized (this) {
            generation.allWorkers.add(worker);
            accepted = isCurrentOpenGeneration(generation);
            if (!accepted) {
                generation.allWorkers.remove(worker);
            }
        }
        return accepted;
    }

    void releaseWorker(final Generation<T> generation, final T worker) {
        if (worker == null) {
            return;
        }

        boolean closeWorker = true;
        synchronized (this) {
            if (worker.isValid() && isCurrentOpenGeneration(generation)) {
                resetWorker(worker.jmsTemplate);
                if (generation.idleWorkers.offer(worker)) {
                    closeWorker = false;
                } else {
                    generation.allWorkers.remove(worker);
                }
            } else {
                generation.allWorkers.remove(worker);
            }
        }

        if (closeWorker) {
            worker.shutdown();
        }
    }

    boolean handleInvalidWorker(final Generation<T> generation, final T worker, final ConnectionFactory cachedConnectionFactory) {
        synchronized (this) {
            generation.allWorkers.remove(worker);
        }

        worker.shutdown();

        synchronized (this) {
            if (!isCurrentOpenGeneration(generation)) {
                return false;
            }

            connectionFactoryProvider.resetConnectionFactory(cachedConnectionFactory);
            return true;
        }
    }

    void retireGeneration() {
        closeWorkers(retireCurrentGeneration());
    }

    synchronized void activateFreshGeneration() {
        if (cycleOpen && currentGeneration.retired) {
            currentGeneration = new Generation<>(maxPoolSize);
        }
    }

    void closeCycle() {
        closeWorkers(closeCurrentCycle());
    }

    IJMSConnectionFactoryProvider getConnectionFactoryProvider() {
        return connectionFactoryProvider;
    }

    private synchronized List<T> retireCurrentGeneration() {
        return currentGeneration.retireAndSnapshot();
    }

    private synchronized List<T> closeCurrentCycle() {
        if (!cycleOpen) {
            return List.of();
        }

        cycleOpen = false;
        return currentGeneration.retireAndSnapshot();
    }

    private synchronized boolean isCurrentOpenGeneration(final Generation<T> generation) {
        return cycleOpen && currentGeneration == generation && !generation.retired;
    }

    private void closeWorkers(final List<T> workers) {
        for (final T worker : workers) {
            try {
                worker.shutdown();
            } catch (final Exception e) {
                logger.error("Failed to close JMS worker {}", worker, e);
            }
        }
    }

    private void resetWorker(final JmsTemplate jmsTemplate) {
        jmsTemplate.setExplicitQosEnabled(false);
        jmsTemplate.setDeliveryMode(jakarta.jms.Message.DEFAULT_DELIVERY_MODE);
        jmsTemplate.setTimeToLive(jakarta.jms.Message.DEFAULT_TIME_TO_LIVE);
        jmsTemplate.setPriority(jakarta.jms.Message.DEFAULT_PRIORITY);
    }

    static final class Generation<T extends JMSWorker> {

        private final BlockingQueue<T> idleWorkers;
        private final Set<T> allWorkers = new HashSet<>();

        private boolean retired;

        private Generation(final int maxPoolSize) {
            idleWorkers = new LinkedBlockingQueue<>(maxPoolSize);
        }

        private List<T> retireAndSnapshot() {
            if (retired) {
                return List.of();
            }

            retired = true;
            idleWorkers.clear();

            final List<T> workers = new ArrayList<>(allWorkers);
            allWorkers.clear();
            return workers;
        }
    }
}
