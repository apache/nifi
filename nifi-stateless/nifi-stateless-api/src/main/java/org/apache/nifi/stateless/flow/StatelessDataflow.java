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

package org.apache.nifi.stateless.flow;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.reporting.BulletinRepository;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

public interface StatelessDataflow {
    /**
     * Triggers the dataflow to run, returning a DataflowTrigger that can be used to wait for the result. Uses the {@link DataflowTriggerContext#IMPLICIT_CONTEXT}.
     * @return a DataflowTrigger that can be used to wait for the result
     *
     * @throws IllegalStateException if called before {@link #initialize(StatelessDataflowInitializationContext)} is called.
     */
    default DataflowTrigger trigger() {
        return trigger(DataflowTriggerContext.IMPLICIT_CONTEXT);
    }

    /**
     * Triggers the dataflow to run, returning a DataflowTrigger that can be used to wait for the result
     *
     * @param triggerContext the trigger context to use
     * @return a DataflowTrigger that can be used to wait for the result
     *
     * @throws IllegalStateException if called before {@link #initialize(StatelessDataflowInitializationContext)} is called.
     */
    DataflowTrigger trigger(DataflowTriggerContext triggerContext);



    /**
     * <p>
     * Performs initialization necessary for triggering dataflows. These activities include, but are not limited to:
     * </p>
     *
     * <ul>
     *     <li>Component validation</li>
     *     <li>Enabling Controller Services</li>
     *     <li>Initializing processors (i.e., invoking @OnScheduled methods, etc.), but not triggering any Processors</li>
     *     <li>Initializing Remote Process Groups so that they can be triggered</li>
     *     <li>Scheduling Reporting Tasks to run</li>
     * </ul>
     *
     * <p>
     *     This method MUST be called prior to calling {@link #trigger()}.
     * </p>
     */
    void initialize(StatelessDataflowInitializationContext initializationContext);

    default void shutdown() {
        shutdown(true, false);
    }

    void shutdown(boolean triggerComponentShutdown, boolean interruptProcessors);

    StatelessDataflowValidation performValidation();

    Set<String> getInputPortNames();

    Set<String> getOutputPortNames();

    QueueSize enqueue(byte[] flowFileContents, Map<String, String> attributes, String portName);

    QueueSize enqueue(InputStream flowFileContents, Map<String, String> attributes, String portName);

    boolean isFlowFileQueued();

    /**
     *
     * @return True if there are any processors in the dataflow with the {@link org.apache.nifi.annotation.behavior.Stateful} annotation
     */
    boolean isStateful();

    void purge();

    Map<String, String> getComponentStates(Scope scope);

    void setComponentStates(Map<String, String> componentStates, Scope scope);

    boolean isSourcePrimaryNodeOnly();

    long getSourceYieldExpiration();

    BulletinRepository getBulletinRepository();

}
