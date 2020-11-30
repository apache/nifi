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

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.FailurePortEncounteredException;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RollbackOnExceptionIT extends StatelessSystemIT {
    private static final String EXCEPTION_TEXT = "Intentional Exception to verify behavior in RollbackOnExceptionIT";

    @Test
    public void testFlowFileCompletelyRemovedWhenExceptionThrown() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessor generate = builder.createSimpleProcessor("GenerateFlowFile");
        final VersionedProcessor setAttribute = builder.createSimpleProcessor("SetAttribute");
        final VersionedProcessor throwException = builder.createSimpleProcessor("ThrowProcessException");

        throwException.setProperties(Collections.singletonMap("Text", EXCEPTION_TEXT));

        builder.createConnection(generate, setAttribute, "success");
        builder.createConnection(setAttribute, throwException, "success");

        final StatelessDataflow dataflow = loadDataflow(builder.getFlowSnapshot(), Collections.emptyList());
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertFalse(result.isSuccessful());
        assertTrue(result.getFailureCause().get() instanceof ProcessException);

        assertFalse(dataflow.isFlowFileQueued());
    }


    @Test
    public void testFlowFileCompletelyRemovedWhenTransferredToFailurePort() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessor generate = builder.createSimpleProcessor("GenerateFlowFile");
        final VersionedProcessor setAttribute = builder.createSimpleProcessor("SetAttribute");
        final VersionedPort out = builder.createOutputPort("Out");

        builder.createConnection(generate, setAttribute, "success");
        builder.createConnection(setAttribute, out, "success");

        final StatelessDataflow dataflow = loadDataflow(builder.getFlowSnapshot(), Collections.emptyList(), Collections.singleton("Out"));

        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertFalse(result.isSuccessful());
        assertTrue(result.getFailureCause().get() instanceof FailurePortEncounteredException);

        assertFalse(dataflow.isFlowFileQueued());
    }

}
