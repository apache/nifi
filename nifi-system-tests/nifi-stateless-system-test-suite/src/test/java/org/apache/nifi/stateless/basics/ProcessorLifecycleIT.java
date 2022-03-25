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

package org.apache.nifi.stateless.basics;

import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProcessorLifecycleIT extends StatelessSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorLifecycleIT.class);

    @Test
    public void testRunProcessorShutdown() throws StatelessConfigurationException, IOException, InterruptedException {
        final File eventsFile = new File("target/events.txt");
        Files.deleteIfExists(eventsFile.toPath());

        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile");
        final VersionedProcessor writeLifecycleEvents = flowBuilder.createSimpleProcessor("WriteLifecycleEvents");

        flowBuilder.createConnection(generate, writeLifecycleEvents, "success");

        writeLifecycleEvents.setAutoTerminatedRelationships(Collections.singleton("success"));
        writeLifecycleEvents.setProperties(Collections.singletonMap("Event File", eventsFile.getAbsolutePath()));

        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());

        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        result.acknowledge();

        dataflow.shutdown();

        List<String> events = Files.readAllLines(eventsFile.toPath());

        // Because the processors may be stopped in the background, we want to wait until we receive the events that we expect.
        while (events.size() < 4) {
            logger.info("Expecting to find 4 events written to {} but currently found only {}; will wait 100 milliseconds and check again", eventsFile.getAbsolutePath(), events.size());

            Thread.sleep(100L);
            events = Files.readAllLines(eventsFile.toPath());
        }

        assertEquals(Arrays.asList("OnScheduled", "OnTrigger", "OnUnscheduled", "OnStopped"), events);
    }
}
