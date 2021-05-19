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

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.stateless.engine.StatelessEngine;

public class StatelessReportingTaskNode extends AbstractReportingTaskNode implements ReportingTaskNode {
    private final FlowManager flowManager;
    private final StatelessEngine<?> statelessEngine;

    public StatelessReportingTaskNode(final LoggableComponent<ReportingTask> reportingTask, final String id, final StatelessEngine<?> statelessEngine,
                                      final FlowManager flowManager, final ProcessScheduler processScheduler, final ValidationContextFactory validationContextFactory,
                                      final ComponentVariableRegistry variableRegistry, final ReloadComponent reloadComponent, final ExtensionManager extensionManager,
                                      final ValidationTrigger validationTrigger) {
        super(reportingTask, id, statelessEngine.getControllerServiceProvider(), processScheduler, validationContextFactory, variableRegistry, reloadComponent, extensionManager, validationTrigger);
        this.flowManager = flowManager;
        this.statelessEngine = statelessEngine;
    }

    @Override
    protected ParameterContext getParameterContext() {
        return null;
    }

    @Override
    public ReportingContext getReportingContext() {
        return new StatelessReportingContext(statelessEngine, flowManager, getEffectivePropertyValues(), getReportingTask(), getVariableRegistry(), getParameterLookup());
    }

    @Override
    public Class<?> getComponentClass() {
        return null;
    }

    @Override
    public boolean isRestricted() {
        return false;
    }

    @Override
    public boolean isDeprecated() {
        return false;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return null;
    }

    @Override
    public Resource getResource() {
        return null;
    }
}
