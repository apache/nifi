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
package org.apache.nifi.io.nio.example;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.io.nio.BufferPool;
import org.apache.nifi.io.nio.ChannelListener;
import org.apache.nifi.io.nio.consumer.StreamConsumer;
import org.apache.nifi.io.nio.consumer.StreamConsumerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class ServerMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerMain.class);

    public static void main(final String[] args) throws IOException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");

        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
        final Map<StreamConsumer, ScheduledFuture<?>> consumerMap = new ConcurrentHashMap<>();
        final BufferPool bufferPool = new BufferPool(10, 5 << 20, false, 40.0);
        ChannelListener listener = null;
        try {
            executor.scheduleWithFixedDelay(bufferPool, 0L, 5L, TimeUnit.SECONDS);
            listener = new ChannelListener(5, new ExampleStreamConsumerFactory(executor, consumerMap), bufferPool, 5, TimeUnit.MILLISECONDS, false);
            listener.setChannelReaderSchedulingPeriod(50L, TimeUnit.MILLISECONDS);
            listener.addDatagramChannel(null, 20000, 32 << 20);
            LOGGER.info("Listening for UDP data on port 20000");
            listener.addServerSocket(null, 20001, 64 << 20);
            LOGGER.info("listening for TCP connections on port 20001");
            listener.addServerSocket(null, 20002, 64 << 20);
            LOGGER.info("listening for TCP connections on port 20002");
            final Calendar endTime = Calendar.getInstance();
            endTime.add(Calendar.MINUTE, 30);
            while (true) {
                processAllConsumers(consumerMap);
                if (endTime.before(Calendar.getInstance())) {
                    break; // time to shut down
                }
            }
        } finally {
            if (listener != null) {
                LOGGER.info("Shutting down server....");
                listener.shutdown(1L, TimeUnit.SECONDS);
                LOGGER.info("Consumer map size = " + consumerMap.size());
                while (consumerMap.size() > 0) {
                    processAllConsumers(consumerMap);
                }
                LOGGER.info("Consumer map size = " + consumerMap.size());
            }
            executor.shutdown();
        }
    }

    private static void processAllConsumers(final Map<StreamConsumer, ScheduledFuture<?>> consumerMap) {
        final Set<StreamConsumer> deadConsumers = new HashSet<>();
        for (final Map.Entry<StreamConsumer, ScheduledFuture<?>> entry : consumerMap.entrySet()) {
            if (entry.getKey().isConsumerFinished()) {
                entry.getValue().cancel(true);
                deadConsumers.add(entry.getKey());
            }
        }
        for (final StreamConsumer consumer : deadConsumers) {
            LOGGER.debug("removing consumer " + consumer);
            consumerMap.remove(consumer);
        }
    }

    public static final class ConsumerRunner implements Runnable {

        private final StreamConsumer consumer;

        public ConsumerRunner(final StreamConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            if (consumer.isConsumerFinished()) {
                return;
            }
            try {
                consumer.process();
            } catch (IOException ex) {
                LOGGER.error("", ex);
            }
        }
    }

    public static final class ExampleStreamConsumerFactory implements StreamConsumerFactory {

        final ScheduledExecutorService executor;
        final Map<StreamConsumer, ScheduledFuture<?>> consumerMap;

        public ExampleStreamConsumerFactory(final ScheduledExecutorService executor, final Map<StreamConsumer, ScheduledFuture<?>> consumerMap) {
            this.executor = executor;
            this.consumerMap = consumerMap;
        }

        @Override
        public StreamConsumer newInstance(final String streamId) {
            final StreamConsumer consumer = new UselessStreamConsumer(streamId);
            final ScheduledFuture<?> future = executor.scheduleWithFixedDelay(new ConsumerRunner(consumer), 0L, 10L, TimeUnit.MILLISECONDS);
            consumerMap.put(consumer, future);
            LOGGER.info("Added consumer: " + consumer);
            return consumer;
        }
    }

}
