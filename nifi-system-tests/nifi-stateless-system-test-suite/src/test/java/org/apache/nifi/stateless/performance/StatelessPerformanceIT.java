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

package org.apache.nifi.stateless.performance;

import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StatelessPerformanceIT extends StatelessSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(StatelessPerformanceIT.class);

    @Test
    @EnabledIfSystemProperty(named = "nifi.test.performance", matches = "true")
    public void testCreateDestroyPerf() throws InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "WARN");
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessor generate = builder.createSimpleProcessor("GenerateFlowFile");
        final VersionedProcessor terminateFlowFile = builder.createSimpleProcessor("TerminateFlowFile");

        builder.createConnection(generate, terminateFlowFile, "success");

        final List<Thread> threads = new ArrayList<>();
        final int numThreads = 6;
        for (int threadIndex = 0; threadIndex < numThreads; threadIndex++) {
            final Thread t = new Thread(() -> {
                try {
                    final StatelessDataflow dataflow = loadDataflow(builder.getFlowSnapshot());

                    final int iterations = 5_000_000;
                    final long start = System.currentTimeMillis();
                    for (int i = 0; i < iterations; i++) {
                        final DataflowTrigger trigger = dataflow.trigger();
                        final TriggerResult result = trigger.getResult();

                        result.acknowledge();
                    }
                    final long millis = System.currentTimeMillis() - start;
                    logger.info("Took {} millis to run {} iterations", millis, iterations);
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            });

            threads.add(t);
        }

        threads.forEach(Thread::start);
        for (final Thread t : threads) {
            t.join();
        }
    }

}
