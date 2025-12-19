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

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;

import java.util.Collections;

public class StandardFlowContext implements FrameworkFlowContext {
    private final ProcessGroup managedProcessGroup;
    private final MutableConnectorConfigurationContext configurationContext;
    private final ProcessGroupFacadeFactory groupFacadeFactory;
    private final ParameterContextFacadeFactory parameterContextFacadeFactory;
    private final ComponentLog connectorLog;
    private final FlowContextType flowContextType;
    private final Bundle bundle;

    private volatile ProcessGroupFacade rootGroup;
    private volatile ParameterContextFacade parameterContext;


    public StandardFlowContext(final ProcessGroup managedProcessGroup, final MutableConnectorConfigurationContext configurationContext,
                final ProcessGroupFacadeFactory groupFacadeFactory, final ParameterContextFacadeFactory parameterContextFacadeFactory,
                final ComponentLog connectorLog, final FlowContextType flowContextType,
                final Bundle bundle) {

        this.managedProcessGroup = managedProcessGroup;
        this.groupFacadeFactory = groupFacadeFactory;
        this.parameterContextFacadeFactory = parameterContextFacadeFactory;
        this.connectorLog = connectorLog;
        this.configurationContext = configurationContext;
        this.flowContextType = flowContextType;
        this.bundle = bundle;

        this.rootGroup = groupFacadeFactory.create(managedProcessGroup, connectorLog);
        this.parameterContext = parameterContextFacadeFactory.create(managedProcessGroup);
    }

    @Override
    public ProcessGroupFacade getRootGroup() {
        return rootGroup;
    }

    @Override
    public ParameterContextFacade getParameterContext() {
        return parameterContext;
    }

    @Override
    public MutableConnectorConfigurationContext getConfigurationContext() {
        return configurationContext;
    }

    @Override
    public void updateFlow(final VersionedExternalFlow versionedExternalFlow, final AssetManager assetManager) throws FlowUpdateException {
        final String parameterContextName = managedProcessGroup.getParameterContext().getName();
        updateParameterContext(versionedExternalFlow.getFlowContents(), parameterContextName);

        try {
            managedProcessGroup.verifyCanUpdate(versionedExternalFlow, true, false);
        } catch (final IllegalStateException e) {
            throw new FlowUpdateException("Flow is not in a state that allows the requested updated", e);
        }

        final VersionedExternalFlow withoutParameterContext = new VersionedExternalFlow();
        withoutParameterContext.setFlowContents(versionedExternalFlow.getFlowContents());
        withoutParameterContext.setParameterContexts(Collections.emptyMap());
        managedProcessGroup.updateFlow(withoutParameterContext, managedProcessGroup.getIdentifier(), false, true, true);

        final ConnectorParameterLookup parameterLookup = new ConnectorParameterLookup(versionedExternalFlow.getParameterContexts().values(), assetManager);
        getParameterContext().updateParameters(parameterLookup.getParameterValues());

        rootGroup = groupFacadeFactory.create(managedProcessGroup, connectorLog);
        parameterContext = parameterContextFacadeFactory.create(managedProcessGroup);
    }

    private void updateParameterContext(final VersionedProcessGroup group, final String parameterContextName) {
        group.setParameterContextName(parameterContextName);
        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
                updateParameterContext(childGroup, parameterContextName);
            }
        }
    }

    @Override
    public Bundle getBundle() {
        return bundle;
    }

    @Override
    public FlowContextType getType() {
        return flowContextType;
    }

    @Override
    public ProcessGroup getManagedProcessGroup() {
        return managedProcessGroup;
    }
}
