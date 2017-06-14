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
package org.apache.nifi.controller.tasks;

import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.scheduling.DummyScheduleState;
import org.apache.nifi.controller.scheduling.ProcessContextFactory;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class TestContinuallyRunConnectableTask {

    @Test
    public void funnelsShouldYieldWhenNoOutboundConnections() {

        // Incoming connection with FlowFile
        final FlowFileQueue flowFileQueueNonEmpty = Mockito.mock(FlowFileQueue.class);
        Mockito.when(flowFileQueueNonEmpty.isActiveQueueEmpty()).thenReturn(false);

        final Connection connectionNonEmpty = Mockito.mock(Connection.class);
        Mockito.when(connectionNonEmpty.getFlowFileQueue()).thenReturn(flowFileQueueNonEmpty);

        // Create a Funnel with an inbound connection, and no outbound connections
        final Funnel testFunnelNoOutbound = Mockito.mock(Funnel.class);
        Mockito.when(testFunnelNoOutbound.getIncomingConnections()).thenReturn(Collections.singletonList(connectionNonEmpty));
        Mockito.when(testFunnelNoOutbound.getConnections()).thenReturn(Collections.emptySet());

        // Set the Funnel to be yielding up to 5 seconds ago
        Mockito.when(testFunnelNoOutbound.getYieldExpiration()).thenReturn(System.currentTimeMillis() - 5000);
        // Set the Funnel 'isTriggeredWhenEmpty' to false (same as what 'StandardFunnel' returns)
        Mockito.when(testFunnelNoOutbound.isTriggerWhenEmpty()).thenReturn(false);
        // Set the Funnel connection type
        Mockito.when(testFunnelNoOutbound.getConnectableType()).thenReturn(ConnectableType.FUNNEL);
        // Set the Funnel relationships to Anonymous (same as what 'StandardFunnel' returns)
        Mockito.when(testFunnelNoOutbound.getRelationships()).thenReturn(Collections.singletonList(Relationship.ANONYMOUS));

        // Create Mock 'ProcessContextFactory', and 'ProcessContext'
        final ProcessContextFactory pcf = Mockito.mock(ProcessContextFactory.class);
        Mockito.when(pcf.newProcessContext(Mockito.any(), Mockito.any())).thenReturn(null);

        final ProcessContext pc = Mockito.mock(ProcessContext.class);

        // Create ContinuallyRunConnectableTask
        ContinuallyRunConnectableTask crct = new ContinuallyRunConnectableTask(pcf, testFunnelNoOutbound, new DummyScheduleState(true), pc);

        // We should yield since this Funnel has no outbound connections.
        Assert.assertTrue("Didn't yield when a Funnel has no outbound connections.", crct.call());
    }

}
