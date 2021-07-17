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

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CancelFlowExecutionIT extends StatelessSystemIT {

    @Test
    public void testCancelExecution() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort inPort = flowBuilder.createInputPort("In");
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessor sleep = flowBuilder.createSimpleProcessor("Sleep");
        sleep.setProperties(Collections.singletonMap("onTrigger Sleep Time", "5 mins"));

        flowBuilder.createConnection(inPort, sleep, Relationship.ANONYMOUS.getName());
        flowBuilder.createConnection(sleep, outPort, "success");

        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot());
        final DataflowTrigger trigger = dataflow.trigger();

        // Wait up to 250 milliseconds. At that point, the result should not be available.
        final Optional<TriggerResult> optionalResult = trigger.getResult(250, TimeUnit.MILLISECONDS);
        assertFalse(optionalResult.isPresent());

        trigger.cancel();
        final TriggerResult result = trigger.getResult();

        assertFalse(result.isSuccessful());
        assertTrue(result.isCanceled());
        assertTrue(result.getOutputFlowFiles().isEmpty());
        assertFalse(dataflow.isFlowFileQueued());
    }

}
