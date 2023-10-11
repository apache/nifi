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
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flow.ExecutionEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RetainExistingStateComponentScheduler implements ComponentScheduler {
    private static final Logger logger = LoggerFactory.getLogger(RetainExistingStateComponentScheduler.class);

    private final ComponentScheduler delegate;
    private final Map<String, ScheduledState> componentStates;
    private final Map<String, ControllerServiceState> controllerServiceStates;

    public RetainExistingStateComponentScheduler(final ProcessGroup processGroup, final ComponentScheduler delegate) {
        this.delegate = delegate;
        this.componentStates = mapComponentStates(processGroup);
        this.controllerServiceStates = mapControllerServiceStates(processGroup);
    }

    @Override
    public void startComponent(final Connectable component) {
        final ScheduledState existingState = componentStates.get(component.getIdentifier());
        if (existingState == null) {
            logger.debug("Will not start {} because it was not previously known in this Process Group", component);
            return;
        }

        if (existingState != ScheduledState.RUNNING && existingState != ScheduledState.STARTING) {
            logger.debug("Will not start {} because its previous state was {}", component, existingState);
            return;
        }

        logger.debug("Starting {}", component);
        delegate.startComponent(component);
    }

    @Override
    public void startStatelessGroup(final ProcessGroup group) {
        final ScheduledState existingState = componentStates.get(group.getIdentifier());
        if (existingState == null) {
            logger.debug("Will not start {} because it was not previously known in this Process Group", group);
            return;
        }

        if (existingState != ScheduledState.RUNNING && existingState != ScheduledState.STARTING) {
            logger.debug("Will not start {} because its previous state was {}", group, existingState);
            return;
        }

        logger.debug("Starting {}", group);
        delegate.startStatelessGroup(group);
    }

    @Override
    public void stopStatelessGroup(final ProcessGroup group) {
        delegate.stopStatelessGroup(group);
    }

    @Override
    public void stopComponent(final Connectable component) {
        delegate.stopComponent(component);
    }

    @Override
    public void transitionComponentState(final Connectable component, final org.apache.nifi.flow.ScheduledState desiredState) {
        delegate.transitionComponentState(component, desiredState);
    }

    @Override
    public void enableControllerServicesAsync(final Collection<ControllerServiceNode> controllerServices) {
        final Set<ControllerServiceNode> toEnable = new HashSet<>();

        for (final ControllerServiceNode service : controllerServices) {
            final ControllerServiceState existingState = controllerServiceStates.get(service.getIdentifier());

            if (existingState == null) {
                logger.debug("Will not enable {} because it was not previously known in this Process Group", service);
                continue;
            }

            if (existingState != ControllerServiceState.ENABLED && existingState != ControllerServiceState.ENABLING) {
                logger.debug("Will not enable {} because its previously state was {}", service, existingState);
                continue;
            }

            toEnable.add(service);
        }

        logger.debug("Enabling {}", toEnable);
        delegate.enableControllerServicesAsync(toEnable);
    }

    @Override
    public void disableControllerServicesAsync(final Collection<ControllerServiceNode> controllerServices) {
        delegate.disableControllerServicesAsync(controllerServices);
    }

    @Override
    public void startReportingTask(final ReportingTaskNode reportingTask) {
        delegate.startReportingTask(reportingTask);
    }

    @Override
    public void pause() {
        delegate.pause();
    }

    @Override
    public void resume() {
        delegate.resume();
    }

    private Map<String, ControllerServiceState> mapControllerServiceStates(final ProcessGroup group) {
        final Set<ControllerServiceNode> services = group.findAllControllerServices();
        final Map<String, ControllerServiceState> serviceStates = services.stream()
            .collect(Collectors.toMap(ControllerServiceNode::getIdentifier, ControllerServiceNode::getState));

        return serviceStates;
    }

    private Map<String, ScheduledState> mapComponentStates(final ProcessGroup group) {
        final Set<Connectable> connectables = new HashSet<>();
        findAllConnectables(group, connectables);

        final Map<String, ScheduledState> componentStates = new HashMap<>();
        for (final Connectable connectable : connectables) {
            componentStates.put(connectable.getIdentifier(), connectable.getScheduledState());
        }

        final Set<ProcessGroup> statelessGroups = new HashSet<>();
        findAllStatelessGroups(group, statelessGroups);
        for (final ProcessGroup statelessGroup : statelessGroups) {
            final StatelessGroupScheduledState state = statelessGroup.getStatelessScheduledState();
            final ScheduledState scheduledState = state == StatelessGroupScheduledState.RUNNING ? ScheduledState.RUNNING : ScheduledState.STOPPED;
            componentStates.put(statelessGroup.getIdentifier(), scheduledState);
        }

        return componentStates;
    }

    private void findAllConnectables(final ProcessGroup group, final Set<Connectable> connectables) {
        connectables.addAll(group.getInputPorts());
        connectables.addAll(group.getOutputPorts());
        connectables.addAll(group.getFunnels());
        connectables.addAll(group.getProcessors());
        for (final RemoteProcessGroup remoteGroup : group.getRemoteProcessGroups()) {
            connectables.addAll(remoteGroup.getInputPorts());
            connectables.addAll(remoteGroup.getOutputPorts());
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            findAllConnectables(childGroup, connectables);
        }
    }

    private void findAllStatelessGroups(final ProcessGroup start, final Set<ProcessGroup> statelessGroups) {
        if (start.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
            statelessGroups.add(start);
            return; // No need to go further, as the top-level stateless group is all we need.
        }

        for (final ProcessGroup childGroup : start.getProcessGroups()) {
            findAllStatelessGroups(childGroup, statelessGroups);
        }
    }
}
