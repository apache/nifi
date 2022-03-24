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
package org.apache.nifi.cdc.mysql.event;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An event listener wrapper for MYSQL binlog events generated from the mysql-binlog-connector.
 */
public class BinlogEventListener implements BinaryLogClient.EventListener {

    protected final AtomicBoolean stopNow = new AtomicBoolean(false);
    private static final int QUEUE_OFFER_TIMEOUT_MSEC = 100;

    private final BlockingQueue<RawBinlogEvent> queue;
    private final BinaryLogClient client;

    public BinlogEventListener(BinaryLogClient client, BlockingQueue<RawBinlogEvent> q) {
        this.client = client;
        this.queue = q;
    }

    public void start() {
        stopNow.set(false);
    }

    public void stop() {
        stopNow.set(true);
    }

    @Override
    public void onEvent(Event event) {
        while (!stopNow.get()) {
            RawBinlogEvent ep = new RawBinlogEvent(event, client.getBinlogFilename());
            try {
                if (queue.offer(ep, QUEUE_OFFER_TIMEOUT_MSEC, TimeUnit.MILLISECONDS)) {
                    return;
                } else {
                    throw new RuntimeException("Unable to add event to the queue");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while adding event to the queue");
            }
        }
    }
}
