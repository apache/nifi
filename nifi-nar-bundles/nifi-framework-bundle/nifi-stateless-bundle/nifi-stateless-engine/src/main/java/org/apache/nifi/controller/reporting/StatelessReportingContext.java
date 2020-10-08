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

package org.apache.nifi.controller.reporting;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.stateless.engine.StatelessEngine;

import java.util.Map;

public class StatelessReportingContext extends AbstractReportingContext implements ReportingContext {
    private final StatelessEngine<?> statelessEngine;
    private final FlowManager flowManager;

    public StatelessReportingContext(final StatelessEngine<?> statelessEngine, final FlowManager flowManager,
                                     final Map<PropertyDescriptor, String> properties, final ReportingTask reportingTask,
                                     final VariableRegistry variableRegistry, final ParameterLookup parameterLookup) {
        super(reportingTask, statelessEngine.getBulletinRepository(), properties, statelessEngine.getControllerServiceProvider(), parameterLookup, variableRegistry);
        this.statelessEngine = statelessEngine;
        this.flowManager = flowManager;
    }

    @Override
    public EventAccess getEventAccess() {
        return new StatelessEventAccess(statelessEngine.getProcessScheduler(), null, flowManager, statelessEngine.getFlowFileEventRepository(), statelessEngine.getProvenanceRepository());
    }

    @Override
    public FlowManager getFlowManager() {
        return flowManager;
    }

    @Override
    public StateManager getStateManager() {
        return statelessEngine.getStateManagerProvider().getStateManager(getReportingTask().getIdentifier());
    }

    @Override
    public boolean isClustered() {
        return false;
    }

    @Override
    public String getClusterNodeIdentifier() {
        return null;
    }
}
