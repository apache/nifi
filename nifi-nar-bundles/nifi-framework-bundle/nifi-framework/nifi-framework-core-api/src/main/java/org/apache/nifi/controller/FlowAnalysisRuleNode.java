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
package org.apache.nifi.controller;

import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleState;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleType;

import java.util.Set;

public interface FlowAnalysisRuleNode extends ComponentNode {
    FlowAnalysisRule getFlowAnalysisRule();

    void setRuleType(FlowAnalysisRuleType ruleType);

    /**
     * @return the type of the flow analysis rule
     */
    FlowAnalysisRuleType getRuleType();

    void setFlowAnalysisRule(LoggableComponent<FlowAnalysisRule> flowAnalysisRule);

    FlowAnalysisRuleContext getFlowAnalysisRuleContext();

    ConfigurationContext getConfigurationContext();

    /**
     * @return the current state of the flow analysis rule
     */
    FlowAnalysisRuleState getState();

    boolean isEnabled();

    String getComments();

    void setComments(String comment);

    void verifyCanDisable();

    void verifyCanEnable();

    void verifyCanEnable(Set<ControllerServiceNode> ignoredServices);

    void verifyCanDelete();

    void verifyCanUpdate();

    void verifyCanClearState();

    void enable();

    void disable();
}
