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

package org.apache.nifi.groups;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;

import java.util.Collection;

/**
 * A {@link ComponentScheduler} that synchronizes a flow's structure while leaving every component stopped or disabled
 * rather than resuming the running or enabled state recorded in the proposed flow. Components that the proposed flow marks
 * as DISABLED remain DISABLED; components marked ENABLED or RUNNING are left in a stopped (but enabled) state. No
 * Processor, Port, Reporting Task, stateless group, or Controller Service is started or enabled.
 *
 * <p>This differs from {@link ComponentScheduler#NOP_SCHEDULER}, which performs no scheduling action at all: because a
 * newly created component defaults to a stopped state, the no-op scheduler silently turns a component that the proposed
 * flow marks DISABLED into a stopped (enabled) component. This scheduler still applies the enabled/disabled transition so
 * that a DISABLED component is faithfully restored as DISABLED, while suppressing only the start or enable that would
 * resume a component to a running or active state.</p>
 *
 * <p>It is used wherever a flow's components must be synchronized without resuming their running state, such as restoring
 * a Connector's managed flow during NiFi startup when {@code nifi.flowcontroller.autoResumeState} is {@code false}.</p>
 */
public class NonStartingComponentScheduler extends AbstractComponentScheduler {

    public NonStartingComponentScheduler(final ControllerServiceProvider controllerServiceProvider, final VersionedComponentStateLookup stateLookup) {
        super(controllerServiceProvider, stateLookup);
    }

    @Override
    public void startComponent(final Connectable component) {
    }

    @Override
    public void startReportingTask(final ReportingTaskNode reportingTask) {
    }

    @Override
    public void startStatelessGroup(final ProcessGroup group) {
    }

    @Override
    public void enableControllerServicesAsync(final Collection<ControllerServiceNode> controllerServices) {
    }

    @Override
    protected void startNow(final Connectable component) {
    }

    @Override
    protected void startNow(final ReportingTaskNode reportingTask) {
    }

    @Override
    protected void startNow(final ProcessGroup statelessGroup) {
    }
}
