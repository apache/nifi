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

    /**
     * Returns true if the Process Group (including nested contents) has any component running or starting, any stateless group running,
     * or any controller service enabled or enabling. Used to decide whether a version update should start or enable newly added components.
     *
     * @param group the process group to inspect
     * @return true if the group had active runtime state at inspection time
     */
    public static boolean processGroupHadActiveRuntimeState(final ProcessGroup group) {
        final Set<Connectable> connectables = new HashSet<>();
        findAllConnectables(group, connectables);
        for (final Connectable connectable : connectables) {
            final ScheduledState state = connectable.getScheduledState();
            if (state == ScheduledState.RUNNING || state == ScheduledState.STARTING) {
                return true;
            }
        }

        final Set<ProcessGroup> statelessGroups = new HashSet<>();
        findAllStatelessGroups(group, statelessGroups);
        for (final ProcessGroup statelessGroup : statelessGroups) {
            if (statelessGroup.getStatelessScheduledState() == StatelessGroupScheduledState.RUNNING) {
                return true;
            }
        }

        for (final ControllerServiceNode service : group.findAllControllerServices()) {
            final ControllerServiceState state = service.getState();
            if (state == ControllerServiceState.ENABLED || state == ControllerServiceState.ENABLING) {
                return true;
            }
        }

        return false;
    }

    private final ComponentScheduler delegate;
    private final Map<String, ScheduledState> componentStates;
    private final Map<String, ControllerServiceState> controllerServiceStates;
    private final boolean processGroupActive;

    public RetainExistingStateComponentScheduler(final ProcessGroup processGroup, final ComponentScheduler delegate) {
        this(processGroup, delegate, false);
    }

    public RetainExistingStateComponentScheduler(final ProcessGroup processGroup, final ComponentScheduler delegate, final boolean processGroupActive) {
        this.delegate = delegate;
        this.componentStates = mapComponentStates(processGroup);
        this.controllerServiceStates = mapControllerServiceStates(processGroup);
        this.processGroupActive = processGroupActive;
    }

    @Override
    public void startComponent(final Connectable component) {
        final ScheduledState existingState = componentStates.get(component.getIdentifier());
        if (existingState == null) {
            if (processGroupActive) {
                logger.debug("Starting new component {} because the Process Group is active", component);
                delegate.startComponent(component);
            } else {
                logger.debug("Will not start {} because it was not previously known in this Process Group", component);
            }
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
            if (processGroupActive) {
                logger.debug("Starting new stateless group {} because the Process Group is active", group);
                delegate.startStatelessGroup(group);
            } else {
                logger.debug("Will not start {} because it was not previously known in this Process Group", group);
            }
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
        final ScheduledState existingState = componentStates.get(component.getIdentifier());
        if (existingState == null && processGroupActive && desiredState != org.apache.nifi.flow.ScheduledState.DISABLED) {
            logger.debug("Starting new component {} because the Process Group is active", component);
            delegate.startComponent(component);
        }
        delegate.transitionComponentState(component, desiredState);
    }

    @Override
    public void enableControllerServicesAsync(final Collection<ControllerServiceNode> controllerServices) {
        final Set<ControllerServiceNode> toEnable = new HashSet<>();

        for (final ControllerServiceNode service : controllerServices) {
            final ControllerServiceState existingState = controllerServiceStates.get(service.getIdentifier());

            if (existingState == null) {
                if (processGroupActive) {
                    logger.debug("Enabling new service {} because the Process Group is active", service);
                    toEnable.add(service);
                } else {
                    logger.debug("Will not enable {} because it was not previously known in this Process Group", service);
                }
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

    boolean isProcessGroupActive() {
        return processGroupActive;
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

    private static void findAllConnectables(final ProcessGroup group, final Set<Connectable> connectables) {
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

    private static void findAllStatelessGroups(final ProcessGroup start, final Set<ProcessGroup> statelessGroups) {
        if (start.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
            statelessGroups.add(start);
            return;
        }

        for (final ProcessGroup childGroup : start.getProcessGroups()) {
            findAllStatelessGroups(childGroup, statelessGroups);
        }
    }
}
