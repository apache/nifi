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

package org.apache.nifi.minifi.bootstrap.command;

import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.nifi.minifi.bootstrap.service.PeriodicStatusReporterManager;
import org.apache.nifi.minifi.commons.status.FlowStatusReport;
import org.apache.nifi.minifi.commons.status.instance.InstanceHealth;
import org.apache.nifi.minifi.commons.status.instance.InstanceStatus;
import org.apache.nifi.minifi.commons.status.processor.ProcessorStatusBean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FlowStatusRunnerTest {

    protected static final String STATUS_REQUEST = "processor:TailFile:health,stats,bulletins";

    @Mock
    private PeriodicStatusReporterManager periodicStatusReporterManager;

    @InjectMocks
    private FlowStatusRunner flowStatusRunner;

    @Test
    void testRunCommandShouldReturnErrorCodeWhenArgsLengthIsNotTwo() {
        int statusCode = flowStatusRunner.runCommand(new String[0]);

        assertEquals(ERROR.getStatusCode(), statusCode);
        verifyNoInteractions(periodicStatusReporterManager);
    }

    @Test
    void testRunCommandShouldReturnOkCodeWhenArgsLengthIsTwo() {
        FlowStatusReport flowStatusReport = aFlowStatusReport();
        when(periodicStatusReporterManager.statusReport(STATUS_REQUEST)).thenReturn(flowStatusReport);

        int statusCode = flowStatusRunner.runCommand(new String[] {"flowStatus", STATUS_REQUEST});

        assertEquals(OK.getStatusCode(), statusCode);
    }

    private FlowStatusReport aFlowStatusReport() {
        FlowStatusReport flowStatusReport = new FlowStatusReport();
        InstanceStatus instanceStatus = new InstanceStatus();
        InstanceHealth instanceHealth = new InstanceHealth();
        instanceHealth.setQueuedCount(2);
        instanceHealth.setActiveThreads(3);
        instanceStatus.setInstanceHealth(instanceHealth);
        flowStatusReport.setInstanceStatus(instanceStatus);
        ProcessorStatusBean processorStatusBean = new ProcessorStatusBean();
        processorStatusBean.setId("processorId");
        flowStatusReport.setProcessorStatusList(Collections.singletonList(processorStatusBean));
        return flowStatusReport;
    }

}