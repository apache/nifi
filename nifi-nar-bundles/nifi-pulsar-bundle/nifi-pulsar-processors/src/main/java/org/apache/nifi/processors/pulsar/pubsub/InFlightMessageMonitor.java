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
package org.apache.nifi.processors.pulsar.pubsub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class to monitor the asynchronous submission of a large number
 * of records to Apache Pulsar.
 *
 * @author david
 *
 */
public class InFlightMessageMonitor {

    private List<byte[]> records;
    private AtomicInteger successCounter;
    private AtomicInteger failureCounter;
    private List<byte[]> failures;
    private CountDownLatch latch;

    public InFlightMessageMonitor(List<byte[]> records) {
        this.records = records;
        this.successCounter = new AtomicInteger(0);
        this.failureCounter = new AtomicInteger(0);
        this.failures = new ArrayList<byte[]>();
        this.latch = new CountDownLatch(records.size());
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public List<byte[]> getRecords() {
        return records;
    }

    public List<byte[]> getFailures() {
        return failures;
    }

    public AtomicInteger getSuccessCounter() {
        return successCounter;
    }

    public AtomicInteger getFailureCounter() {
        return failureCounter;
    }

}
