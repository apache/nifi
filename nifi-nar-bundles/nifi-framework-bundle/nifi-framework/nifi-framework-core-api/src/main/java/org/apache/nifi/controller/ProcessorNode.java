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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.scheduling.ScheduleState;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ProcessorNode extends AbstractConfiguredComponent implements Connectable {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorNode.class);

    protected final AtomicReference<ScheduledState> scheduledState;

    public ProcessorNode(final String id,
                         final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider,
                         final String componentType, final String componentCanonicalClass, final ComponentVariableRegistry variableRegistry,
                         final ReloadComponent reloadComponent, final boolean isExtensionMissing) {
        super(id, validationContextFactory, serviceProvider, componentType, componentCanonicalClass, variableRegistry, reloadComponent, isExtensionMissing);
        this.scheduledState = new AtomicReference<>(ScheduledState.STOPPED);
    }

    public abstract boolean isIsolated();

    public abstract boolean isTriggerWhenAnyDestinationAvailable();

    @Override
    public abstract boolean isSideEffectFree();

    public abstract boolean isTriggeredSerially();

    public abstract boolean isEventDrivenSupported();

    public abstract boolean isHighThroughputSupported();

    public abstract Requirement getInputRequirement();

    @Override
    public abstract boolean isValid();

    public abstract void setBulletinLevel(LogLevel bulletinLevel);

    public abstract LogLevel getBulletinLevel();

    public abstract Processor getProcessor();

    public abstract void setProcessor(LoggableComponent<Processor> processor);

    public abstract void yield(long period, TimeUnit timeUnit);

    public abstract void setAutoTerminatedRelationships(Set<Relationship> relationships);

    public abstract Set<Relationship> getAutoTerminatedRelationships();

    public abstract void setSchedulingStrategy(SchedulingStrategy schedulingStrategy);

    @Override
    public abstract SchedulingStrategy getSchedulingStrategy();

    public abstract void setExecutionNode(ExecutionNode executionNode);

    public abstract ExecutionNode getExecutionNode();

    public abstract void setRunDuration(long duration, TimeUnit timeUnit);

    public abstract long getRunDuration(TimeUnit timeUnit);

    public abstract Map<String, String> getStyle();

    public abstract void setStyle(Map<String, String> style);

    /**
     * @return the number of threads (concurrent tasks) currently being used by
     *         this Processor
     */
    public abstract int getActiveThreadCount();

    /**
     * Verifies that this Processor can be started if the provided set of
     * services are enabled. This is introduced because we need to verify that
     * all components can be started before starting any of them. In order to do
     * that, we need to know that this component can be started if the given
     * services are enabled, as we will then enable the given services before
     * starting this component.
     *
     * @param ignoredReferences to ignore
     */
    public abstract void verifyCanStart(Set<ControllerServiceNode> ignoredReferences);

    /**
     *
     */
    @Override
    public ScheduledState getScheduledState() {
        ScheduledState sc = this.scheduledState.get();
        if (sc == ScheduledState.STARTING) {
            return ScheduledState.RUNNING;
        } else if (sc == ScheduledState.STOPPING) {
            return ScheduledState.STOPPED;
        }
        return sc;
    }

    /**
     * Returns the physical state of this processor which includes transition
     * states such as STOPPING and STARTING.
     *
     * @return the physical state of this processor [DISABLED, STOPPED, RUNNING,
     *         STARTIING, STOPIING]
     */
    public ScheduledState getPhysicalScheduledState() {
        return this.scheduledState.get();
    }

    /**
     * Will start the {@link Processor} represented by this
     * {@link ProcessorNode}. Starting processor typically means invoking its
     * operation that is annotated with @OnScheduled and then executing a
     * callback provided by the {@link ProcessScheduler} to which typically
     * initiates
     * {@link Processor#onTrigger(ProcessContext, org.apache.nifi.processor.ProcessSessionFactory)}
     * cycle.
     *
     * @param scheduler
     *            implementation of {@link ScheduledExecutorService} used to
     *            initiate processor <i>start</i> task
     * @param administrativeYieldMillis
     *            the amount of milliseconds to wait for administrative yield
     * @param processContext
     *            the instance of {@link ProcessContext} and
     *            {@link ControllerServiceLookup}
     * @param schedulingAgentCallback
     *            the callback provided by the {@link ProcessScheduler} to
     *            execute upon successful start of the Processor
     */
    public abstract <T extends ProcessContext & ControllerServiceLookup> void start(ScheduledExecutorService scheduler,
            long administrativeYieldMillis, T processContext, SchedulingAgentCallback schedulingAgentCallback);

    /**
     * Will stop the {@link Processor} represented by this {@link ProcessorNode}.
     * Stopping processor typically means invoking its operation that is
     * annotated with @OnUnschedule and then @OnStopped.
     *
     * @param scheduler
     *            implementation of {@link ScheduledExecutorService} used to
     *            initiate processor <i>stop</i> task
     * @param processContext
     *            the instance of {@link ProcessContext} and
     *            {@link ControllerServiceLookup}
     * @param schedulingAgent
     *            the SchedulingAgent that is responsible for managing the scheduling of the ProcessorNode
     * @param scheduleState
     *            the ScheduleState that can be used to ensure that the running state (STOPPED, RUNNING, etc.)
     *            as well as the active thread counts are kept in sync
     */
    public abstract <T extends ProcessContext & ControllerServiceLookup> CompletableFuture<Void> stop(ScheduledExecutorService scheduler,
        T processContext, SchedulingAgent schedulingAgent, ScheduleState scheduleState);

    /**
     * Will set the state of the processor to STOPPED which essentially implies
     * that this processor can be started. This is idempotent operation and will
     * result in the WARN message if processor can not be enabled.
     */
    public void enable() {
        if (!this.scheduledState.compareAndSet(ScheduledState.DISABLED, ScheduledState.STOPPED)) {
            logger.warn("Processor cannot be enabled because it is not disabled");
        }
    }

    /**
     * Will set the state of the processor to DISABLED which essentially implies
     * that this processor can NOT be started. This is idempotent operation and
     * will result in the WARN message if processor can not be enabled.
     */
    public void disable() {
        if (!this.scheduledState.compareAndSet(ScheduledState.STOPPED, ScheduledState.DISABLED)) {
            logger.warn("Processor cannot be disabled because its state is set to " + this.scheduledState);
        }
    }
}
