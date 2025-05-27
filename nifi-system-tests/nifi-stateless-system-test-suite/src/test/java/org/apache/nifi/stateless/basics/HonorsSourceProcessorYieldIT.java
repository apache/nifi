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

import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TransactionThresholds;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HonorsSourceProcessorYieldIT extends StatelessSystemIT {

    @Test
    public void testSourceProcessorYieldIsHonored() throws StatelessConfigurationException, IOException, InterruptedException {
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessor source = builder.createSimpleProcessor("YieldSource");
        source.setProperties(Collections.singletonMap("Yield After", "5"));

        final VersionedProcessor holdInput = builder.createSimpleProcessor("HoldInput");
        holdInput.setProperties(Collections.singletonMap("Hold Time", "2 secs"));
        builder.createConnection(source, holdInput, "success");

        final VersionedPort outputPort = builder.createOutputPort("Out");
        builder.createConnection(holdInput, outputPort, "success");

        final StatelessDataflow dataflow = loadDataflow(builder.getFlowSnapshot(), Collections.emptyList(), Collections.emptySet(), TransactionThresholds.UNLIMITED);
        final DataflowTrigger trigger = dataflow.trigger();

        // Ignore input for 3 seconds. During this time, the source processor should produce 5 FlowFiles and then start yielding.
        // As a result, we should get 5 FlowFiles quickly and then 1 per second. This should result in 6-10 FlowFiles total,
        // allowing for some variability in timing.
        Thread.sleep(Duration.ofSeconds(3));

        final Optional<TriggerResult> optionalResult = trigger.getResult(10, TimeUnit.SECONDS);
        assertTrue(optionalResult.isPresent());

        final Map<String, List<FlowFile>> outputMap = optionalResult.get().getOutputFlowFiles();
        assertEquals(1, outputMap.size());

        final List<FlowFile> flowFiles = outputMap.get("Out");
        assertNotNull(flowFiles);
        final int numFlowFiles = flowFiles.size();
        assertTrue(numFlowFiles >= 6);
        assertTrue(numFlowFiles <= 10);
    }

}
