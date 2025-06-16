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

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.lifecycle.ProcessorStopLifecycleMethods;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.migration.ControllerServiceFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public abstract class ProcessorNode extends AbstractComponentNode implements Connectable {
    public static final int DEFAULT_RETRY_COUNT = 10;
    public static final BackoffMechanism DEFAULT_BACKOFF_MECHANISM = BackoffMechanism.PENALIZE_FLOWFILE;
    public static final String DEFAULT_MAX_BACKOFF_PERIOD = "10 mins";

    protected final AtomicReference<ScheduledState> scheduledState;

    public ProcessorNode(final String id,
                         final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider,
                         final String componentType, final String componentCanonicalClass, final ReloadComponent reloadComponent,
                         final ExtensionManager extensionManager, final ValidationTrigger validationTrigger,
                         final boolean isExtensionMissing) {
        super(id, validationContextFactory, serviceProvider, componentType, componentCanonicalClass, reloadComponent,
                extensionManager, validationTrigger, isExtensionMissing);
        this.scheduledState = new AtomicReference<>(ScheduledState.STOPPED);
    }

    @Override
    public abstract boolean isIsolated();

    @Override
    public abstract boolean isTriggerWhenAnyDestinationAvailable();

    @Override
    public abstract boolean isSideEffectFree();

    public abstract boolean isTriggeredSerially();

    public abstract boolean isExecutionNodeRestricted();

    public abstract Requirement getInputRequirement();

    public abstract List<ActiveThreadInfo> getActiveThreads(ThreadDetails threadDetails);

    /**
     * Returns the number of threads that are still 'active' in this Processor but have been terminated
     * via {@link #terminate()}. To understand more about these threads, such as their stack traces and
     * how long they have been active, one can use {@link #getActiveThreads(ThreadDetails)} and then filter the results
     * to include only those {@link ActiveThreadInfo} objects for which the thread is terminated. For example:
     * {@code getActiveThreads().stream().filter(ActiveThreadInfo::isTerminated).collect(Collectors.toList());}
     *
     * @return the number of threads that are still 'active' in this Processor but have been terminated.
     */
    public abstract int getTerminatedThreadCount();

    public abstract void setBulletinLevel(LogLevel bulletinLevel);

    public abstract LogLevel getBulletinLevel();

    public abstract Processor getProcessor();

    public abstract void setProcessor(LoggableComponent<Processor> processor);

    @Override
    public abstract void yield(long period, TimeUnit timeUnit);

    public abstract void setAutoTerminatedRelationships(Set<Relationship> relationships);

    public abstract Set<Relationship> getAutoTerminatedRelationships();

    public abstract void setSchedulingStrategy(SchedulingStrategy schedulingStrategy);

    @Override
    public abstract SchedulingStrategy getSchedulingStrategy();

    public abstract void setExecutionNode(ExecutionNode executionNode);

    public abstract ExecutionNode getExecutionNode();

    public abstract void setRunDuration(long duration, TimeUnit timeUnit);

    @Override
    public abstract long getRunDuration(TimeUnit timeUnit);

    public abstract Map<String, String> getStyle();

    public abstract void setStyle(Map<String, String> style);

    /**
     * @return the number of threads (concurrent tasks) currently being used by
     *         this Processor
     */
    public abstract int getActiveThreadCount();

    public void verifyCanPerformVerification() {
        if (isRunning()) {
            throw new IllegalStateException("Cannot perform verification because the Processor is not stopped");
        }
    }

    public abstract List<ConfigVerificationResult> verifyConfiguration(ProcessContext processContext, ComponentLog logger, Map<String, String> attributes, ExtensionManager extensionManager);

    public abstract void verifyCanTerminate();

    @Override
    public ScheduledState getScheduledState() {
        ScheduledState sc = this.scheduledState.get();
        if (sc == ScheduledState.STARTING) {
            final ValidationStatus validationStatus = getValidationStatus();

            if (validationStatus == ValidationStatus.INVALID) {
                return ScheduledState.STOPPED;
            } else {
                return ScheduledState.RUNNING;
            }
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
     *         STARTING, STOPPING]
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
     * @param timeoutMillis the number of milliseconds to wait after triggering the Processor's @OnScheduled methods before timing out and considering
     * the startup a failure. This will result in the thread being interrupted and trying again.
     * @param processContextFactory
     *            a factory for creating instances of {@link ProcessContext}
     * @param schedulingAgentCallback
     *            the callback provided by the {@link ProcessScheduler} to
     *            execute upon successful start of the Processor
     * @param failIfStopping If <code>false</code>, and the Processor is in the 'STOPPING' state,
     *            then the Processor will automatically restart itself as soon as its last thread finishes. If this
     *            value is <code>true</code> or if the Processor is in any state other than 'STOPPING' or 'RUNNING', then this method
     *            will throw an {@link IllegalStateException}.
     * @param triggerLifecycleMethods Whether or not the lifecycle methods (@OnScheduled, @OnUnscheduled, @OnStopped, etc.) should be called.
     */
    public abstract void start(ScheduledExecutorService scheduler, long administrativeYieldMillis, long timeoutMillis, Supplier<ProcessContext> processContextFactory,
                               SchedulingAgentCallback schedulingAgentCallback, boolean failIfStopping, boolean triggerLifecycleMethods);

    /**
     * Will run the {@link Processor} represented by this
     * {@link ProcessorNode} once. This typically means invoking its
     * operation that is annotated with @OnScheduled and then executing a
     * callback provided by the {@link ProcessScheduler} to which typically
     * initiates
     * {@link Processor#onTrigger(ProcessContext, org.apache.nifi.processor.ProcessSessionFactory)}
     * cycle and schedules the stopping of the processor right away.
     * @param scheduler
     *            implementation of {@link ScheduledExecutorService} used to
     *            initiate processor <i>start</i> task
     * @param administrativeYieldMillis
     *            the amount of milliseconds to wait for administrative yield
     * @param timeoutMillis the number of milliseconds to wait after triggering the Processor's @OnScheduled methods before timing out and considering
* the startup a failure. This will result in the thread being interrupted and trying again.
     * @param processContextFactory
*            a factory for creating instances of {@link ProcessContext}
     * @param schedulingAgentCallback
*            the callback provided by the {@link ProcessScheduler} to
*            execute upon successful start of the Processor
     */
    public abstract void runOnce(ScheduledExecutorService scheduler, long administrativeYieldMillis, long timeoutMillis, Supplier<ProcessContext> processContextFactory,
                                 SchedulingAgentCallback schedulingAgentCallback);

    /**
     * Will stop the {@link Processor} represented by this {@link ProcessorNode}.
     * Stopping processor typically means invoking its operation that is
     * annotated with @OnUnschedule and then @OnStopped.
     *
     * @param processScheduler the ProcessScheduler that can be used to re-schedule the processor if need be
     * @param executor
     *            implementation of {@link ScheduledExecutorService} used to
     *            initiate processor <i>stop</i> task
     * @param processContext
     *            the instance of {@link ProcessContext}
     * @param schedulingAgent
     *            the SchedulingAgent that is responsible for managing the scheduling of the ProcessorNode
     * @param scheduleState
     *            the ScheduleState that can be used to ensure that the running state (STOPPED, RUNNING, etc.)
     *            as well as the active thread counts are kept in sync
     * @param lifecycleMethods
     *            Indicates which lifecycle methods should be triggered to run
     */
    public abstract CompletableFuture<Void> stop(ProcessScheduler processScheduler, ScheduledExecutorService executor,
        ProcessContext processContext, SchedulingAgent schedulingAgent, LifecycleState scheduleState, ProcessorStopLifecycleMethods lifecycleMethods);

    /**
     * Triggers any method with the {@link OnUnscheduled} annotation on the Processor. This does not stop scheduling the Processor, change any state
     * of the ProcessorNode, or invoke any other lifecycle methods. This method should be used only in order to signal to a Processor that it may
     * perform any necessary duties that are associated with no longer being scheduled, such as proactively ending an onTrigger cycle.
     *
     * @param processContext the ProcessContext that may be provided to the methods annotated with {@link OnUnscheduled}.
     */
    public abstract void triggerOnUnscheduled(ProcessContext processContext);

    /**
     * Marks all active tasks as terminated and interrupts all active threads
     *
     * @return the number of active tasks that were terminated
     */
    public abstract int terminate();

    /**
     * Determines whether or not the task associated with the given thread is terminated
     *
     * @param thread the thread
     * @return <code>true</code> if there is a task associated with the given thread and that task was terminated, <code>false</code> if
     *         the given thread is not an active thread, or if the active thread has not been terminated
     */
    public abstract boolean isTerminated(Thread thread);

    /**
     * Will set the state of the processor to STOPPED which essentially implies
     * that this processor can be started. This is idempotent operation and will
     * result in the WARN message if processor can not be enabled.
     */
    public abstract void enable();

    /**
     * Will set the state of the processor to DISABLED which essentially implies
     * that this processor can NOT be started. This is idempotent operation and
     * will result in the WARN message if processor can not be enabled.
     */
    public abstract void disable();

    /**
     * Returns the Scheduled State that is desired for this Processor. This may vary from the current state if the Processor is not
     * currently valid, is in the process of stopping but should then transition to Running, etc.
     *
     * @return the desired state for this Processor
     */
    public abstract ScheduledState getDesiredState();

    @Override
    protected void performFlowAnalysisOnThis() {
        getValidationContextFactory().getFlowAnalyzer().ifPresent(flowAnalyzer -> flowAnalyzer.analyzeProcessor(this));
    }

    /**
     * This method will be called once the processor's configuration has been restored (on startup, reload, e.g.)
     *
     * @param context The ProcessContext associated with the Processor configuration
     */
    public abstract void onConfigurationRestored(ProcessContext context);

    public abstract void notifyPrimaryNodeChanged(PrimaryNodeState primaryNodeState, LifecycleState lifecycleState);

    public abstract void migrateConfiguration(Map<String, String> originalPropertyValues, ControllerServiceFactory serviceFactory);

}
