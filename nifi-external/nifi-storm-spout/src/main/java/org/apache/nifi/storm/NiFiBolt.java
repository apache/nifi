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
package org.apache.nifi.storm;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Storm bolt that can send tuples back to NiFi. This bolt provides a micro-batching approach for higher
 * through put scenarios. The bolt will queue tuples until the number of tuples reaches the provided batch size, or
 * until the provided batch interval in seconds has been exceeded. Setting the batch size to 1 will send each tuple
 * immediately in a single transaction.
 */
public class NiFiBolt extends BaseRichBolt {

    private static final long serialVersionUID = 3067274587595578836L;
    public static final Logger LOGGER = LoggerFactory.getLogger(NiFiBolt.class);

    private final SiteToSiteClientConfig clientConfig;
    private final NiFiDataPacketBuilder builder;
    private final int tickFrequencySeconds;

    private SiteToSiteClient client;
    private OutputCollector collector;
    private BlockingQueue<Tuple> queue = new LinkedBlockingQueue<>();

    private int batchSize = 10;
    private int batchIntervalInSec = 10;
    private long lastBatchProcessTimeSeconds = 0;

    public NiFiBolt(final SiteToSiteClientConfig clientConfig, final NiFiDataPacketBuilder builder, final int tickFrequencySeconds) {
        Validate.notNull(clientConfig);
        Validate.notNull(builder);
        Validate.isTrue(tickFrequencySeconds > 0);
        this.clientConfig = clientConfig;
        this.builder = builder;
        this.tickFrequencySeconds = tickFrequencySeconds;
    }

    public NiFiBolt withBatchSize(int batchSize) {
        Validate.isTrue(batchSize > 0);
        this.batchSize = batchSize;
        return this;
    }

    public NiFiBolt withBatchInterval(int batchIntervalInSec) {
        Validate.isTrue(batchIntervalInSec > 0);
        this.batchIntervalInSec = batchIntervalInSec;
        return this;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.client = createSiteToSiteClient();
        this.collector = outputCollector;
        this.lastBatchProcessTimeSeconds = System.currentTimeMillis() / 1000;

        LOGGER.info("Bolt is prepared with Batch Size " + batchSize
                + ", Batch Interval " + batchIntervalInSec
                + ", Tick Frequency is " + tickFrequencySeconds);
    }

    protected SiteToSiteClient createSiteToSiteClient() {
        return new SiteToSiteClient.Builder().fromConfig(clientConfig).build();
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            // if we have a tick tuple then lets see if enough time has passed since our last batch was processed
            if ((System.currentTimeMillis() / 1000 - lastBatchProcessTimeSeconds) >= batchIntervalInSec) {
                LOGGER.debug("Received tick tuple and reached batch interval, executing batch");
                finishBatch();
            } else {
                LOGGER.debug("Received tick tuple, but haven't reached batch interval, nothing to do");
            }
        } else {
            // for a regular tuple we add it to the queue and then see if our queue size exceeds batch size
            this.queue.add(tuple);

            int queueSize = this.queue.size();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Current queue size is " + queueSize + ", and batch size is " + batchSize);
            }

            if (queueSize >= batchSize) {
                LOGGER.debug("Queue Size is greater than or equal to batch size, executing batch");
                finishBatch();
            }
        }
    }

    private void finishBatch() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Finishing batch of size " + queue.size());
        }

        lastBatchProcessTimeSeconds = System.currentTimeMillis() / 1000;

        final List<Tuple> tuples = new ArrayList<>();
        queue.drainTo(tuples);

        if (tuples.size() == 0) {
            LOGGER.debug("Finishing batch, but no tuples so returning...");
            return;
        }

        try {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);
            if (transaction == null) {
                throw new IllegalStateException("Unable to create a NiFi Transaction to send data");
            }

            // convert each tuple to a NiFiDataPacket and send it as part of the transaction
            for (Tuple tuple : tuples) {
                final NiFiDataPacket dataPacket = builder.createNiFiDataPacket(tuple);
                transaction.send(dataPacket.getContent(), dataPacket.getAttributes());
            }

            transaction.confirm();
            transaction.complete();

            // ack the tuples after successfully completing the transaction
            for (Tuple tuple : tuples) {
                collector.ack(tuple);
            }

        } catch(Exception e){
            LOGGER.warn("Unable to process tuples due to: " + e.getMessage(), e);
            for (Tuple tuple : tuples) {
                collector.fail(tuple);
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (client != null) {
            try {
                client.close();
            } catch (final IOException ioe) {
                LOGGER.error("Failed to close client", ioe);
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencySeconds);
        return conf;
    }

}
