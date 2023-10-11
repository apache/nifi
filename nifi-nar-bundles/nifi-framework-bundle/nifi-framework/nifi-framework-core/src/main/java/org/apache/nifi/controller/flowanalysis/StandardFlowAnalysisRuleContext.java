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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.flowanalysis.FlowAnalysisContext;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.parameter.ParameterLookup;

import java.util.Map;

public class StandardFlowAnalysisRuleContext extends AbstractFlowAnalysisRuleContext implements FlowAnalysisRuleContext {
    private final String ruleName;
    private final FlowController flowController;
    private final FlowAnalysisContext flowAnalysisContext;

    public StandardFlowAnalysisRuleContext(
            final String ruleName,
            final StandardFlowAnalysisRuleNode flowAnalysisRuleNode,
            final Map<PropertyDescriptor, String> properties,
            final FlowController flowController,
            final ParameterLookup parameterLookup
    ) {
        super(flowAnalysisRuleNode, properties, flowController.getControllerServiceProvider(), parameterLookup);
        this.ruleName = ruleName;
        this.flowController = flowController;
        this.flowAnalysisContext = new StandardFlowAnalysisContext(flowController);
    }

    @Override
    public String getRuleName() {
        return ruleName;
    }

    @Override
    public StateManager getStateManager() {
        return flowController.getStateManagerProvider().getStateManager(getFlowAnalysisRule().getIdentifier());
    }

    @Override
    public FlowAnalysisContext getFlowAnalysisContext() {
        return flowAnalysisContext;
    }
}
