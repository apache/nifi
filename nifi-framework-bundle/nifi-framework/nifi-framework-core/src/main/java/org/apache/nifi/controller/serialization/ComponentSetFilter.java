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

package org.apache.nifi.controller.serialization;

import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.remote.RemoteGroupPort;

public interface ComponentSetFilter {
    boolean testProcessor(ProcessorNode processor);

    boolean testReportingTask(ReportingTaskNode reportingTask);

    boolean testFlowAnalysisRule(FlowAnalysisRuleNode flowAnalysisRule);

    boolean testControllerService(ControllerServiceNode controllerService);

    boolean testInputPort(Port port);

    boolean testOutputPort(Port port);

    boolean testRemoteInputPort(RemoteGroupPort port);

    boolean testRemoteOutputPort(RemoteGroupPort port);

    boolean testFlowRegistryClient(FlowRegistryClientNode flowRegistryClient);

    boolean testStatelessGroup(ProcessGroup group);


    default ComponentSetFilter reverse() {
        final ComponentSetFilter original = this;

        return new ComponentSetFilter() {
            @Override
            public boolean testProcessor(final ProcessorNode processor) {
                return !original.testProcessor(processor);
            }

            @Override
            public boolean testReportingTask(final ReportingTaskNode reportingTask) {
                return !original.testReportingTask(reportingTask);
            }

            @Override
            public boolean testFlowAnalysisRule(FlowAnalysisRuleNode flowAnalysisRule) {
                return !original.testFlowAnalysisRule(flowAnalysisRule);
            }

            @Override
            public boolean testControllerService(final ControllerServiceNode controllerService) {
                return !original.testControllerService(controllerService);
            }

            @Override
            public boolean testInputPort(final Port port) {
                return !original.testInputPort(port);
            }

            @Override
            public boolean testOutputPort(final Port port) {
                return !original.testOutputPort(port);
            }

            @Override
            public boolean testRemoteInputPort(final RemoteGroupPort port) {
                return !original.testRemoteInputPort(port);
            }

            @Override
            public boolean testRemoteOutputPort(final RemoteGroupPort port) {
                return !original.testRemoteOutputPort(port);
            }

            @Override
            public boolean testFlowRegistryClient(final FlowRegistryClientNode flowRegistryClient) {
                return !original.testFlowRegistryClient(flowRegistryClient);
            }

            @Override
            public boolean testStatelessGroup(final ProcessGroup group) {
                return !original.testStatelessGroup(group);
            }
        };
    }
}
