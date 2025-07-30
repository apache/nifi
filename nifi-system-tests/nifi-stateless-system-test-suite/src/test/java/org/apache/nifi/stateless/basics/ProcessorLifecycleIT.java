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

import org.apache.nifi.components.PortFunction;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessorLifecycleIT extends StatelessSystemIT {
    public static final String STOPPED_AND_SUCCESS = "Stopped and Success";
    public static final String STOPPED_AND_FAILURE = "Stopped and Failure";
    public static final String RUNNING_AND_SUCCESS = "Running and Success";
    public static final String RUNNING_AND_FAILURE = "Running and Failure";

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
        writeLifecycleEvents.setProperties(Map.of("Event File", eventsFile.getAbsolutePath()));

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

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSessionCommitCallbacksCalledBeforeStopOnFailure() throws StatelessConfigurationException, IOException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateAndCountCallbacks");
        final VersionedPort port = flowBuilder.createOutputPort("Out");
        port.setPortFunction(PortFunction.FAILURE);

        flowBuilder.createConnection(generate, port, "success");
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();

        assertFalse(result.isSuccessful());

        assertCounters(dataflow, generate.getIdentifier(), RUNNING_AND_FAILURE);
    }

    private void assertCounters(final StatelessDataflow dataflow, final String componentId, final String positiveCounterName) {
        for (final String counterName : new String[] {RUNNING_AND_SUCCESS, RUNNING_AND_FAILURE, STOPPED_AND_SUCCESS, STOPPED_AND_FAILURE}) {
            final OptionalLong counterValue = dataflow.getCounter(componentId, counterName);

            if (Objects.equals(counterName, positiveCounterName)) {
                assertTrue(counterValue.isPresent(), "Expected Counter '" + counterName + "' to be present but it was not. Counters: " + dataflow.getCounters(Pattern.compile(".+")));
                assertEquals(1, counterValue.getAsLong());
            } else {
                assertTrue(counterValue.isEmpty(), "Counter '" + counterName + "' has a value of " + counterValue);
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSessionCommitCallbacksCalledBeforeStopOnTimeout() throws StatelessConfigurationException, IOException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateAndCountCallbacks");
        final VersionedProcessor sleep = flowBuilder.createSimpleProcessor("Sleep");
        sleep.setProperties(Map.of("onTrigger Sleep Time", "1 min"));
        sleep.setAutoTerminatedRelationships(Set.of("success"));

        flowBuilder.createConnection(generate, sleep, "success");
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());
        final DataflowTrigger trigger = dataflow.trigger();
        final Optional<TriggerResult> optionalTriggerResult = trigger.getResult(100, TimeUnit.MILLISECONDS);
        assertTrue(optionalTriggerResult.isEmpty());
        trigger.cancel();

        // We expect Failure counter to get incremented because the processor the processor is being terminated
        assertCounters(dataflow, generate.getIdentifier(), RUNNING_AND_FAILURE);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSessionCommitCallbacksCalledBeforeStopOnSuccess() throws StatelessConfigurationException, IOException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateAndCountCallbacks");
        final VersionedPort port = flowBuilder.createOutputPort("Out");
        port.setPortFunction(PortFunction.STANDARD); // Being explicit that port should not be a failure port.

        flowBuilder.createConnection(generate, port, "success");
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();

        assertTrue(result.isSuccessful());
        result.acknowledge();

        assertCounters(dataflow, generate.getIdentifier(), RUNNING_AND_SUCCESS);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSessionCommitCallbacksCalledBeforeStopOnShutdownWhenProcessorDoesNotGracefullyFinish() throws StatelessConfigurationException, IOException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateAndCountCallbacks");
        final VersionedProcessor sleep = flowBuilder.createSimpleProcessor("Sleep");
        sleep.setProperties(Map.of("onTrigger Sleep Time", "1 min", "Stop Sleeping When Unscheduled", "false"));
        sleep.setAutoTerminatedRelationships(Set.of("success"));

        flowBuilder.createConnection(generate, sleep, "success");
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());
        final DataflowTrigger trigger = dataflow.trigger();
        final Optional<TriggerResult> optionalTriggerResult = trigger.getResult(100, TimeUnit.MILLISECONDS);
        assertTrue(optionalTriggerResult.isEmpty());
        dataflow.shutdown(false, true, Duration.ofMillis(1));

        while (dataflow.getCounters(Pattern.compile(".+")).isEmpty()) {
            Thread.sleep(100L);
        }

        assertCounters(dataflow, generate.getIdentifier(), RUNNING_AND_FAILURE);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSessionCommitCallbacksCalledBeforeStopOnShutdownWhenProcessorFinishes() throws StatelessConfigurationException, IOException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateAndCountCallbacks");
        final VersionedProcessor sleep = flowBuilder.createSimpleProcessor("Sleep");
        sleep.setProperties(Map.of("onTrigger Sleep Time", "1 min", "Stop Sleeping When Unscheduled", "true"));
        sleep.setAutoTerminatedRelationships(Set.of("success"));

        flowBuilder.createConnection(generate, sleep, "success");
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());
        final DataflowTrigger trigger = dataflow.trigger();
        final Optional<TriggerResult> optionalTriggerResult = trigger.getResult(100, TimeUnit.MILLISECONDS);
        assertTrue(optionalTriggerResult.isEmpty());
        dataflow.shutdown(false, true, Duration.ofSeconds(5));

        while (dataflow.getCounters(Pattern.compile(".+")).isEmpty()) {
            Thread.sleep(100L);
        }

        assertCounters(dataflow, generate.getIdentifier(), RUNNING_AND_SUCCESS);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSessionCommitCallbacksCalledBeforeStopOnShutdownWithTimeout() throws StatelessConfigurationException, IOException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateAndCountCallbacks");
        final VersionedProcessor sleep = flowBuilder.createSimpleProcessor("Sleep");
        sleep.setProperties(Map.of("onTrigger Sleep Time", "1 min",
            "Ignore Interrupts", "true",
            "Stop Sleeping When Unscheduled", "false"));
        sleep.setAutoTerminatedRelationships(Set.of("success"));

        flowBuilder.createConnection(generate, sleep, "success");
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());
        final DataflowTrigger trigger = dataflow.trigger();
        final Optional<TriggerResult> optionalTriggerResult = trigger.getResult(2, TimeUnit.SECONDS);
        assertTrue(optionalTriggerResult.isEmpty());
        dataflow.shutdown(false, true, Duration.ofMillis(1));

        while (dataflow.getCounters(Pattern.compile(".+")).isEmpty()) {
            Thread.sleep(100L);
        }

        assertCounters(dataflow, generate.getIdentifier(), RUNNING_AND_FAILURE);
    }

}
