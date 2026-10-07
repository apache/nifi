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

package org.apache.nifi.components.connector;

import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;

/**
 * The view of the Active flow handed to prepareForFlowEvolution. It delegates to the Active context, except that
 * getBundle() returns the version that built the flow rather than the loaded code. It deliberately does not
 * implement FrameworkFlowContext, so ConnectorInitializationContext.updateFlow rejects it and the hook cannot
 * restructure the pre-upgrade flow.
 */
final class PreUpgradeFlowContext implements FlowContext {
    private final FlowContext activeFlowContext;
    private final Bundle preUpgradeBundle;

    /**
     * @param activeFlowContext the live Active context; supplies getRootGroup, getParameterContext,
     *                          getConfigurationContext, and getType
     * @param preUpgradeBundle the bundle from the authoritative snapshot, returned by getBundle
     */
    PreUpgradeFlowContext(final FlowContext activeFlowContext, final Bundle preUpgradeBundle) {
        this.activeFlowContext = activeFlowContext;
        this.preUpgradeBundle = preUpgradeBundle;
    }

    @Override
    public ProcessGroupFacade getRootGroup() {
        return activeFlowContext.getRootGroup();
    }

    @Override
    public ParameterContextFacade getParameterContext() {
        return activeFlowContext.getParameterContext();
    }

    @Override
    public ConnectorConfigurationContext getConfigurationContext() {
        return activeFlowContext.getConfigurationContext();
    }

    @Override
    public FlowContextType getType() {
        return activeFlowContext.getType();
    }

    @Override
    public Bundle getBundle() {
        return preUpgradeBundle;
    }
}
