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
package org.apache.nifi.controller.flowanalysis;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.registry.VariableRegistry;
import java.util.Map;

public class StandardFlowAnalysisRuleContext extends AbstractFlowAnalysisRuleContext implements FlowAnalysisRuleContext {
    private final FlowController flowController;

    public StandardFlowAnalysisRuleContext(
        FlowAnalysisRule flowAnalysisRule,
        Map<PropertyDescriptor, String> properties,
        FlowController flowController,
        ParameterLookup parameterLookup,
        VariableRegistry variableRegistry
    ) {
        super(flowAnalysisRule, properties, flowController.getControllerServiceProvider(), parameterLookup, variableRegistry);
        this.flowController = flowController;
    }

    @Override
    public StateManager getStateManager() {
        return flowController.getStateManagerProvider().getStateManager(getFlowAnalysisRule().getIdentifier());
    }

    @Override
    protected FlowManager getFlowManager() {
        return flowController.getFlowManager();
    }

    @Override
    public boolean isClustered() {
        return flowController.isConfiguredForClustering();
    }

    @Override
    public String getClusterNodeIdentifier() {
        final NodeIdentifier nodeId = flowController.getNodeId();
        return nodeId == null ? null : nodeId.getId();
    }
}
