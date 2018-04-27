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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.scheduling.SchedulingStrategy;

public interface ProcessScheduler {

    /**
     * Shuts down the scheduler, stopping all components
     */
    void shutdown();

    /**
     * Starts scheduling the given processor to run after invoking all methods
     * on the underlying {@link org.apache.nifi.processor.Processor FlowFileProcessor} that
     * are annotated with the {@link org.apache.nifi.annotation.lifecycle.OnScheduled} annotation. If the Processor
     * is already scheduled to run, does nothing.
     *
     * @param procNode to start
     * @param failIfStopping If <code>false</code>, and the Processor is in the 'STOPPING' state,
     *            then the Processor will automatically restart itself as soon as its last thread finishes. If this
     *            value is <code>true</code> or if the Processor is in any state other than 'STOPPING' or 'RUNNING', then this method
     *            will throw an {@link IllegalStateException}.
     * @throws IllegalStateException if the Processor is disabled
     */
    Future<Void> startProcessor(ProcessorNode procNode, boolean failIfStopping);

    /**
     * Stops scheduling the given processor to run and invokes all methods on
     * the underlying {@link org.apache.nifi.processor.Processor FlowFileProcessor} that
     * are annotated with the {@link org.apache.nifi.annotation.lifecycle.OnUnscheduled} annotation. This does not
     * interrupt any threads that are currently running within the given
     * Processor. If the Processor is not scheduled to run, does nothing.
     *
     * @param procNode to stop
     */
    Future<Void> stopProcessor(ProcessorNode procNode);

    /**
     * Interrupts all threads that are currently active in the Processor in an attempt to
     * regain the threads and stop running the tasks in the Processor. All instances of
     * {@link ProcessSession}, {@link ProcessSessionFactory}, {@link ProcessContext}, and
     * the {@link InputStream}s and {@link OutputStream}s that were generated from the associated
     * Process Sessions will also be poisoned, meaning that any calls to those objects will result
     * in a {@link TerminatedTaskException} being thrown. In addition, the number of active threads
     * will immediately be set to 0 so that the Processor can be modified. Note, however, that if
     * the threads do not return, they cannot be returned to the Thread Pool. As such, invoking this
     * method many times or when many threads are active and deadlocked/livelocked can result in
     * thread pool exhaustion. If the given Processor is not in a Scheduled State of STOPPED, then this
     * method does nothing.
     *
     * @param procNode the Processor to terminate
     *
     * @throws IllegalStateException if the Processor's Scheduled State is not currently STOPPED
     */
    void terminateProcessor(ProcessorNode procNode);

    /*
     * Notifies the schedule that the given processor is being removed so the scheduler may clean up any resources
     * related to the given processor.
     *
     * @param procNode the processor node being removed
     */
    void onProcessorRemoved(ProcessorNode procNode);

    /**
     * Starts scheduling the given Port to run. If the Port is already scheduled
     * to run, does nothing.
     *
     * @param port to start
     *
     * @throws IllegalStateException if the Port is disabled
     */
    void startPort(Port port);

    /**
     * Stops scheduling the given Port to run. This does not interrupt any
     * threads that are currently running within the given port. This does not
     * interrupt any threads that are currently running within the given Port.
     * If the Port is not scheduled to run, does nothing.
     *
     * @param port to stop
     */
    void stopPort(Port port);

    /**
     * Starts scheduling the given Funnel to run. If the funnel is already
     * scheduled to run, does nothing.
     *
     * @param funnel to start
     *
     * @throws IllegalStateException if the Funnel is disabled
     */
    void startFunnel(Funnel funnel);

    /**
     * Stops scheduling the given Funnel to run. This does not interrupt any
     * threads that are currently running within the given funnel. If the funnel
     * is not scheduled to run, does nothing.
     *
     * @param funnel to stop
     */
    void stopFunnel(Funnel funnel);

    void enableFunnel(Funnel funnel);

    void enablePort(Port port);

    void enableProcessor(ProcessorNode procNode);

    void disableFunnel(Funnel funnel);

    void disablePort(Port port);

    void disableProcessor(ProcessorNode procNode);

    /**
     * @param scheduled scheduled component
     * @return the number of threads currently active for the given
     * <code>Connectable</code>
     */
    int getActiveThreadCount(Object scheduled);

    /**
     * @param scheduled component to test
     * @return a boolean indicating whether or not the given object is scheduled
     * to run
     */
    boolean isScheduled(Object scheduled);

    /**
     * Registers a relevant event for an Event-Driven worker
     *
     * @param worker to register
     */
    void registerEvent(Connectable worker);

    /**
     * Notifies the ProcessScheduler of how many threads are available to use
     * for the given {@link SchedulingStrategy}
     *
     * @param strategy scheduling strategy
     * @param maxThreadCount max threads
     */
    void setMaxThreadCount(SchedulingStrategy strategy, int maxThreadCount);

    /**
     * Notifies the Scheduler that it should stop scheduling the given component
     * until its yield duration has expired
     *
     * @param procNode processor
     */
    void yield(ProcessorNode procNode);

    /**
     * Stops scheduling the given Reporting Task to run
     *
     * @param taskNode to unschedule
     */
    void unschedule(ReportingTaskNode taskNode);

    /**
     * Begins scheduling the given Reporting Task to run
     *
     * @param taskNode to schedule
     */
    void schedule(ReportingTaskNode taskNode);

    /**
     * Enables the Controller Service so that it can be used by Reporting Tasks
     * and Processors
     *
     * @param service to enable
     */
    CompletableFuture<Void> enableControllerService(ControllerServiceNode service);

    /**
     * Disables all of the given Controller Services in the order provided by the List
     * @param services the controller services to disable
     */
    CompletableFuture<Void> disableControllerServices(List<ControllerServiceNode> services);

    /**
     * Disables the Controller Service so that it can be updated
     *
     * @param service to disable
     */
    CompletableFuture<Void> disableControllerService(ControllerServiceNode service);
}
