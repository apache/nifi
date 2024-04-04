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

package org.apache.nifi.registry.flow.mapping;

import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.StatelessGroupScheduledState;

public interface VersionedComponentStateLookup {
    ScheduledState getState(ProcessorNode processorNode);

    ScheduledState getState(Port port);

    ScheduledState getState(ReportingTaskNode taskNode);

    ScheduledState getState(FlowAnalysisRuleNode ruleNode);

    ScheduledState getState(ControllerServiceNode serviceNode);

    ScheduledState getState(ProcessGroup processGroup);


    /**
     * Returns a Scheduled State of ENABLED or DISABLED for every component. No component will be mapped to RUNNING.
     */
    VersionedComponentStateLookup ENABLED_OR_DISABLED = new VersionedComponentStateLookup() {
        @Override
        public ScheduledState getState(final ProcessorNode processorNode) {
            return processorNode.getScheduledState() == org.apache.nifi.controller.ScheduledState.DISABLED ? ScheduledState.DISABLED : ScheduledState.ENABLED;
        }

        @Override
        public ScheduledState getState(final Port port) {
            return port.getScheduledState() == org.apache.nifi.controller.ScheduledState.DISABLED ? ScheduledState.DISABLED : ScheduledState.ENABLED;
        }

        @Override
        public ScheduledState getState(final ReportingTaskNode taskNode) {
            return taskNode.getScheduledState() == org.apache.nifi.controller.ScheduledState.DISABLED ? ScheduledState.DISABLED : ScheduledState.ENABLED;
        }

        @Override
        public ScheduledState getState(final FlowAnalysisRuleNode ruleNode) {
            return ruleNode.getState() == FlowAnalysisRuleState.DISABLED ? ScheduledState.DISABLED : ScheduledState.ENABLED;
        }

        @Override
        public ScheduledState getState(final ControllerServiceNode serviceNode) {
            return ScheduledState.DISABLED;
        }

        @Override
        public ScheduledState getState(final ProcessGroup group) {
            return ScheduledState.ENABLED;
        }
    };

    /**
     * Returns the Scheduled State according to whatever is currently set for the component
     */
    VersionedComponentStateLookup IDENTITY_LOOKUP = new VersionedComponentStateLookup() {
        @Override
        public ScheduledState getState(final ProcessorNode processorNode) {
            return map(processorNode.getDesiredState());
        }

        @Override
        public ScheduledState getState(final Port port) {
            return map(port.getScheduledState());
        }

        @Override
        public ScheduledState getState(final ReportingTaskNode taskNode) {
            return map(taskNode.getScheduledState());
        }

        @Override
        public ScheduledState getState(final FlowAnalysisRuleNode ruleNode) {
            switch (ruleNode.getState()) {
                case DISABLED:
                    return ScheduledState.DISABLED;
                case ENABLED:
                default:
                    return ScheduledState.ENABLED;
            }
        }

        @Override
        public ScheduledState getState(final ControllerServiceNode serviceNode) {
            switch (serviceNode.getState()) {
                case ENABLED:
                case ENABLING:
                    return ScheduledState.ENABLED;
                case DISABLED:
                case DISABLING:
                default:
                    return ScheduledState.DISABLED;
            }
        }

        @Override
        public ScheduledState getState(final ProcessGroup group) {
            if (group.getDesiredStatelessScheduledState() == StatelessGroupScheduledState.RUNNING) {
                return ScheduledState.RUNNING;
            }

            return ScheduledState.ENABLED;
        }

        private ScheduledState map(final org.apache.nifi.controller.ScheduledState componentState) {
            if (componentState == null) {
                return null;
            }

            switch (componentState) {
                case DISABLED:
                    return ScheduledState.DISABLED;
                case RUNNING:
                case STARTING:
                    return ScheduledState.RUNNING;
                case RUN_ONCE:
                case STOPPED:
                case STOPPING:
                default:
                    return ScheduledState.ENABLED;
            }
        }
    };
}
