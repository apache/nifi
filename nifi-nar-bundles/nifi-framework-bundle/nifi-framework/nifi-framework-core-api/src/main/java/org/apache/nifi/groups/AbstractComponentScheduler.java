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
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class AbstractComponentScheduler implements ComponentScheduler {
    private static final Logger logger = LoggerFactory.getLogger(AbstractComponentScheduler.class);

    private final ControllerServiceProvider serviceProvider;
    private final VersionedComponentStateLookup stateLookup;

    private final AtomicLong pauseCount = new AtomicLong(0L);
    private final Queue<Connectable> connectablesToStart = new LinkedBlockingQueue<>();
    private final Queue<ReportingTaskNode> reportingTasksToStart = new LinkedBlockingQueue<>();
    private final Queue<ControllerServiceNode> toEnable = new LinkedBlockingQueue<>();
    private final Queue<ProcessGroup> statelessGroupsToStart = new LinkedBlockingQueue<>();

    public AbstractComponentScheduler(final ControllerServiceProvider controllerServiceProvider, final VersionedComponentStateLookup stateLookup) {
        this.serviceProvider = controllerServiceProvider;
        this.stateLookup = stateLookup;
    }

    @Override
    public void pause() {
        final long count = pauseCount.incrementAndGet();
        logger.debug("{} paused; count = {}", this, count);
    }

    @Override
    public void resume() {
        final long updatedCount = pauseCount.decrementAndGet();
        logger.debug("{} resumed; count = {}", this, updatedCount);

        if (updatedCount > 0) {
            return;
        }

        logger.debug("{} enabling {}", this, toEnable);
        enableNow(toEnable);

        Connectable connectable;
        while ((connectable = connectablesToStart.poll()) != null) {
            logger.debug("{} starting {}", this, connectable);
            startNow(connectable);
        }

        ReportingTaskNode taskNode;
        while ((taskNode = reportingTasksToStart.poll()) != null) {
            logger.debug("{} starting {}", this, taskNode);
            startNow(taskNode);
        }

        ProcessGroup processGroup;
        while ((processGroup = statelessGroupsToStart.poll()) != null) {
            logger.debug("{} starting {}", this, processGroup);
            startNow(processGroup);
        }
    }

    private boolean isPaused() {
        return pauseCount.get() > 0;
    }


    @Override
    public void transitionComponentState(final Connectable component, final ScheduledState desiredState) {
        final ScheduledState scheduledState = getScheduledState(component);
        final ScheduledState finalState = desiredState == null ? ScheduledState.ENABLED : desiredState;

        switch (finalState) {
            case DISABLED:
                if (scheduledState == ScheduledState.RUNNING) {
                    logger.debug("Stopping {}", component);
                    stopComponent(component);
                }

                logger.debug("Disabling {}", component);
                disable(component);
                break;
            case ENABLED:
                if (scheduledState == ScheduledState.DISABLED) {
                    logger.debug("Enabling {}", component);
                    enable(component);
                } else if (scheduledState == ScheduledState.RUNNING) {
                    logger.debug("Stopping {}", component);
                    stopComponent(component);
                }

                break;
            case RUNNING:
                if (scheduledState == ScheduledState.DISABLED) {
                    logger.debug("Enabling {}", component);
                    enable(component);
                }

                logger.debug("Starting {}", component);
                startComponent(component);
                break;
        }
    }

    private ScheduledState getScheduledState(final Connectable component) {
        // Use the State Lookup to get the state, if possible. If, for some reason, it doesn't
        // provide us a state (which should never happen) just fall back to the component's scheduled state.
        return switch (component.getConnectableType()) {
            case INPUT_PORT, OUTPUT_PORT, REMOTE_INPUT_PORT, REMOTE_OUTPUT_PORT -> stateLookup.getState((Port) component);
            case PROCESSOR -> stateLookup.getState((ProcessorNode) component);
            case FUNNEL -> ScheduledState.RUNNING;
            default -> switch (component.getScheduledState()) {
                case DISABLED -> ScheduledState.DISABLED;
                case RUN_ONCE, STOPPED, STOPPING -> ScheduledState.ENABLED;
                default -> ScheduledState.RUNNING;
            };
        };

    }


    private void enable(final Connectable component) {
        final ProcessGroup group = component.getProcessGroup();
        switch (component.getConnectableType()) {
            case INPUT_PORT -> group.enableInputPort((Port) component);
            case OUTPUT_PORT -> group.enableOutputPort((Port) component);
            case PROCESSOR -> group.enableProcessor((ProcessorNode) component);
        }
    }

    private void disable(final Connectable component) {
        final ProcessGroup group = component.getProcessGroup();
        switch (component.getConnectableType()) {
            case INPUT_PORT -> group.disableInputPort((Port) component);
            case OUTPUT_PORT -> group.disableOutputPort((Port) component);
            case PROCESSOR -> group.disableProcessor((ProcessorNode) component);
        }
    }


    @Override
    public void startComponent(final Connectable component) {
        if (isPaused()) {
            logger.debug("{} called to start {} but paused so will queue it for start later", this, component);
            connectablesToStart.offer(component);
        } else {
            logger.debug("{} starting {} now", this, component);
            startNow(component);
        }
    }

    @Override
    public void stopComponent(final Connectable component) {
        final ProcessGroup processGroup = component.getProcessGroup();
        switch (component.getConnectableType()) {
            case INPUT_PORT -> processGroup.stopInputPort((Port) component);
            case OUTPUT_PORT -> processGroup.stopOutputPort((Port) component);
            case PROCESSOR -> processGroup.stopProcessor((ProcessorNode) component);
            case REMOTE_INPUT_PORT, REMOTE_OUTPUT_PORT -> {
                final RemoteGroupPort port = (RemoteGroupPort) component;
                port.getRemoteProcessGroup().stopTransmitting(port);
            }
        }
    }

    @Override
    public void enableControllerServicesAsync(final Collection<ControllerServiceNode> controllerServices) {
        if (isPaused()) {
            logger.debug("{} called to enable {} but paused so will queue them for start later", this, controllerServices);
            toEnable.addAll(controllerServices);
        } else {
            logger.debug("{} enabling {} now", this, controllerServices);
            enableNow(controllerServices);
        }
    }

    @Override
    public void disableControllerServicesAsync(final Collection<ControllerServiceNode> controllerServices) {
        serviceProvider.disableControllerServicesAsync(controllerServices);
    }

    protected ControllerServiceProvider getControllerServiceProvider() {
        return serviceProvider;
    }

    public void startReportingTask(final ReportingTaskNode reportingTask) {
        if (isPaused()) {
            logger.debug("{} called to start {} but paused so will queue it for start later", this, reportingTask);
            reportingTasksToStart.offer(reportingTask);
        } else {
            logger.debug("{} starting {} now", this, reportingTask);
            startNow(reportingTask);
        }
    }

    @Override
    public void startStatelessGroup(final ProcessGroup group) {
        if (isPaused()) {
            logger.debug("{} called to start {} but paused so will queue it for start later", this, group);
            statelessGroupsToStart.offer(group);
        } else {
            logger.debug("{} starting {} now", this, group);
            startNow(group);
        }
    }

    @Override
    public void stopStatelessGroup(final ProcessGroup group) {
        group.stopProcessing();
    }

    protected abstract void startNow(Connectable component);

    protected abstract void startNow(ReportingTaskNode reportingTask);

    protected abstract void startNow(ProcessGroup statelessGroup);

    protected void enableNow(final Collection<ControllerServiceNode> controllerServices) {
        final Map<ExecutionEngine, List<ControllerServiceNode>> servicesByExecutionEngine = controllerServices.stream()
            .collect(Collectors.groupingBy(this::getExecutionEngine));

        final List<ControllerServiceNode> statelessServices = servicesByExecutionEngine.get(ExecutionEngine.STATELESS);
        if (statelessServices != null && !statelessServices.isEmpty()) {
            logger.debug("Will not enable {} Controller Services because their Execution Engine is Stateless: {}", statelessServices.size(), statelessServices);
        }

        final List<ControllerServiceNode> servicesToEnable = servicesByExecutionEngine.get(ExecutionEngine.STANDARD);
        if (servicesToEnable != null && !servicesToEnable.isEmpty()) {
            getControllerServiceProvider().enableControllerServices(servicesToEnable);
        }
    }

    private ExecutionEngine getExecutionEngine(final ControllerServiceNode service) {
        final ProcessGroup group = service.getProcessGroup();
        if (group == null) {
            return ExecutionEngine.STANDARD;
        }

        return group.resolveExecutionEngine();
    }

}
