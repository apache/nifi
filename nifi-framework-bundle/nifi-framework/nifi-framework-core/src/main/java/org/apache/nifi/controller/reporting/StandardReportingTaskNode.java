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

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingTask;

import java.util.Collections;
import java.util.List;

public class StandardReportingTaskNode extends AbstractReportingTaskNode implements ReportingTaskNode {

    private final FlowController flowController;

    public StandardReportingTaskNode(final LoggableComponent<ReportingTask> reportingTask, final String id, final FlowController controller,
                                     final ProcessScheduler processScheduler, final ValidationContextFactory validationContextFactory,
                                     final ReloadComponent reloadComponent, final ExtensionManager extensionManager,
                                     final ValidationTrigger validationTrigger) {
        super(reportingTask, id, controller.getControllerServiceProvider(), processScheduler, validationContextFactory, reloadComponent, extensionManager, validationTrigger);
        this.flowController = controller;
    }

    public StandardReportingTaskNode(final LoggableComponent<ReportingTask> reportingTask, final String id, final FlowController controller,
                                     final ProcessScheduler processScheduler, final ValidationContextFactory validationContextFactory,
                                     final String componentType, final String canonicalClassName, final ReloadComponent reloadComponent,
                                     final ExtensionManager extensionManager, final ValidationTrigger validationTrigger, final boolean isExtensionMissing) {
        super(reportingTask, id, controller.getControllerServiceProvider(), processScheduler, validationContextFactory, componentType, canonicalClassName,
            reloadComponent, extensionManager, validationTrigger, isExtensionMissing);
        this.flowController = controller;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return flowController;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.ReportingTask, getIdentifier(), getName());
    }

    @Override
    public boolean isRestricted() {
        return getReportingTask().getClass().isAnnotationPresent(Restricted.class);
    }

    @Override
    public Class<?> getComponentClass() {
        return getReportingTask().getClass();
    }

    @Override
    public boolean isDeprecated() {
        return getReportingTask().getClass().isAnnotationPresent(DeprecationNotice.class);
    }

    @Override
    public ReportingContext getReportingContext() {
        return new StandardReportingContext(flowController, flowController.getBulletinRepository(), getEffectivePropertyValues(), this, ParameterLookup.EMPTY);
    }

    @Override
    protected List<ValidationResult> validateConfig() {
        return Collections.emptyList();
    }

    @Override
    protected ParameterContext getParameterContext() {
        return null;
    }

    @Override
    public void start() {
        verifyCanStart();
        flowController.startReportingTask(this);
    }

    @Override
    public void stop() {
        verifyCanStop();
        flowController.stopReportingTask(this);
    }

    @Override
    public void enable() {
        verifyCanEnable();
        flowController.enableReportingTask(this);
    }

    @Override
    public void disable() {
        verifyCanDisable();
        flowController.disableReportingTask(this);
    }
}
