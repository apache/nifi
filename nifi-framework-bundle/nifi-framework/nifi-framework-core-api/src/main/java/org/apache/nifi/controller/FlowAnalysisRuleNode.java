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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleState;
import org.apache.nifi.flowanalysis.EnforcementPolicy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;

import java.util.List;
import java.util.Set;

public interface FlowAnalysisRuleNode extends ComponentNode {
    FlowAnalysisRule getFlowAnalysisRule();

    void setEnforcementPolicy(EnforcementPolicy enforcementPolicy);

    /**
     * @return the enforcement policy of the flow analysis rule
     */
    EnforcementPolicy getEnforcementPolicy();

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

    /**
     * Verifies that the Flow Analysis Rule is in a state in which it can verify a configuration by calling
     * {@link #verifyConfiguration(ConfigurationContext, ComponentLog, ExtensionManager)}.
     *
     * @throws IllegalStateException if not in a state in which configuration can be verified
     */
    void verifyCanPerformVerification();

    /**
     * Verifies that the given configuration is valid for the Flow Analysis Rule
     *
     * @param context the configuration to verify
     * @param logger a logger that can be used when performing verification
     * @param extensionManager extension manager that is used for obtaining appropriate NAR ClassLoaders
     * @return a list of results indicating whether or not the given configuration is valid
     */
    List<ConfigVerificationResult> verifyConfiguration(ConfigurationContext context, ComponentLog logger, ExtensionManager extensionManager);

    void enable();

    void disable();
}
