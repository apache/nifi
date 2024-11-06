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

package org.apache.nifi.flow.synchronization;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisRuleInstantiationException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedFlowAnalysisRule;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.groups.ComponentAdditions;
import org.apache.nifi.groups.FlowSynchronizationOptions;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.VersionedComponentAdditions;
import org.apache.nifi.parameter.ParameterContext;

import java.util.concurrent.TimeoutException;

public interface VersionedComponentSynchronizer {

    /**
     * Adds versioned components to the specified Process Group.
     *
     * @param group the Process Group to append to
     * @param additions the component additions to add to the Process Group
     * @param options sync options
     * @return the component additions
     */
    ComponentAdditions addVersionedComponentsToProcessGroup(ProcessGroup group, VersionedComponentAdditions additions, FlowSynchronizationOptions options);

    /**
     * Verifies that the given additions can be applied to the specified process group.
     *
     * @param group the Process Group that will be appended to
     * @param additions the component additions that will be added to the Process Group
     */
    void verifyCanAddVersionedComponents(ProcessGroup group, VersionedComponentAdditions additions);

    /**
     * Synchronize the given Process Group to match the proposed flow
     * @param group the Process Group to update
     * @param proposedFlow the proposed/desired state for the process group
     * @param synchronizationOptions options for how to synchronize the group
     */
    void synchronize(ProcessGroup group, VersionedExternalFlow proposedFlow, FlowSynchronizationOptions synchronizationOptions) throws ProcessorInstantiationException;

    /**
     * Verifies that the given Process Group can be updated to match the proposed flow
     * @param group the group to update
     * @param proposed the proposed updated version
     * @param verifyConnectionRemoval if <code>true</code> and synchronizing the Process Group would result in any Connection being removed, an IllegalStateException will be thrown if that
     * Connection has data in it. If <code>false</code>, the presence of data in removed queues will be ignored.
     */
    void verifyCanSynchronize(ProcessGroup group, VersionedProcessGroup proposed, boolean verifyConnectionRemoval);


    /**
     * Synchronizes the given Processor to match the proposed snapshot, or deletes the Processor if the proposed snapshot is <code>null</code>. If the given processor is <code>null</code>,
     * adds the processor to the given ProcessGroup
     *
     * @param processor the processor to synchronize
     * @param proposedProcessor the proposed/desired state for the processor
     * @param group the ProcessGroup to which the ProcessorNode should belong.
     * @param synchronizationOptions options for how to synchronize the flow
     *
     * @throws FlowSynchronizationException if unable to synchronize the processor with the proposed version
     * @throws TimeoutException if the processor must be stopped in order to synchronize it with the proposed version and stopping takes longer than the timeout allowed by the
     * {@link FlowSynchronizationOptions synchronization options}.
     * @throws InterruptedException if interrupted while waiting for processor to stop or outbound connections to empty if processor is being removed
     */
    void synchronize(ProcessorNode processor, VersionedProcessor proposedProcessor, ProcessGroup group, FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException, InterruptedException;

    /**
     * Synchronizes the given Controller Service to match the proposed version, or deletes the Controller Service if the proposed snapshot is <code>null</code>. If the given Controller Service is
     * <code>null</code>, adds the Controller Service to the given Process Group
     *
     * @param controllerService the Controller Service to synchronize
     * @param proposed the proposed/desired state for the controller service
     * @param group the ProcessGroup to which the Controller Service should belong
     * @param synchronizationOptions options for how to synchronize the flow
     *
     * @throws FlowSynchronizationException if unable to synchronize the Controller Service with the proposed version
     * @throws TimeoutException if the Controller Service must be disabled in order to synchronize it with the proposed version and disabling takes longer than the timeout allowed by the
     * {@link FlowSynchronizationOptions synchronization options}.
     * @throws InterruptedException if interrupted while waiting for Controller Service to disable
     */
    void synchronize(ControllerServiceNode controllerService, VersionedControllerService proposed, ProcessGroup group, FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException, InterruptedException;

    /**
     * Synchronizes the given Connection to match the proposed one, or deletes the Connection if the proposed is <code>null</code>. If the given connection is <code>null</code>, adds the connection
     * to the given ProcessGroup
     * @param connection the connection to synchronize
     * @param proposedConnection the proposed/desired state for the connection
     * @param group the ProcessGroup to which the connection should belong
     * @param synchronizationOptions options for how to synchronize the flow
     *
     * @throws IllegalStateException if the connection cannot be updated due to the state of the flow
     * @throws FlowSynchronizationException if unable to synchronize the connection with the proposed version
     * @throws TimeoutException if the source or destination of the connection must be stopped in order to perform the synchronization and stopping it takes longer than the timeout allowed by the
     * {@link FlowSynchronizationOptions synchronization options}.
     */
    void synchronize(Connection connection, VersionedConnection proposedConnection, ProcessGroup group, FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException;


    /**
     * Synchronizes the given Port to match the proposed one, or deletes the Port if the proposed is <code>null</code>. If the given Port is <code>null</code>, creates it and adds it to the given
     * ProcessGroup
     *
     * @param port the port to synchronize
     * @param proposed the proposed/desired state for the port
     * @param group the ProcessGroup to which the port should belong
     * @param synchronizationOptions options for how to synchronize the flow
     *
     * @throws IllegalStateException if the port cannot be updated due to the state of the flow
     * @throws FlowSynchronizationException if unable to synchronize the port with the proposed version
     * @throws TimeoutException if the port is running and takes longer to stop than the timeout allowed by the {@link FlowSynchronizationOptions synchronization options}.
     */
    void synchronize(Port port, VersionedPort proposed, ProcessGroup group, FlowSynchronizationOptions synchronizationOptions) throws FlowSynchronizationException, TimeoutException,
        InterruptedException;


    /**
     * Synchronizes the given Funnel to match the proposed one, or deletes the Funnel if the proposed is <code>null</code>. If the given Funnel is <code>null</code>, creates it and adds it to the
     * given ProcessGroup
     *
     * @param funnel the funnel to synchronize
     * @param proposed the proposed/desired state for the funnel
     * @param group the ProcessGroup to which the funnel should belong
     * @param synchronizationOptions options for how to synchronize the flow
     *
     * @throws IllegalStateException if the funnel cannot be updated due to the state of the flow
     * @throws FlowSynchronizationException if unable to synchronize the funnel with the proposed version
     * @throws TimeoutException if the funnel is being removed and downstream components take longer to stop than the timeout allowed by the {@link FlowSynchronizationOptions synchronization options}.
     */
    void synchronize(Funnel funnel, VersionedFunnel proposed, ProcessGroup group, FlowSynchronizationOptions synchronizationOptions) throws FlowSynchronizationException, TimeoutException,
        InterruptedException;

    /**
     * Synchronizes the given Label to match the proposed one, or deletes the Label if the proposed is <code>null</code>. If the given Label is <code>null</code>, creates it and adds it to the
     * given ProcessGroup
     *
     * @param label the label to synchronize
     * @param proposed the proposed/desired state for the label
     * @param group the ProcessGroup to which the label should belong
     * @param synchronizationOptions options for how to synchronize the flow
     */
    void synchronize(Label label, VersionedLabel proposed, ProcessGroup group, FlowSynchronizationOptions synchronizationOptions);

    /**
     * Synchronizes the given Reporting Task to match the proposed one, or deletes the Reporting Task if the proposed is <code>null</code>. If the given Reporting Task is <code>null</code>,
     * creates it
     *
     * @param reportingTask the reporting task to synchronize
     * @param proposed the proposed/desired state
     * @param synchronizationOptions options for how to synchronize the flow
     *
     * @throws IllegalStateException if the reporting task cannot be updated due to the state of the flow
     * @throws FlowSynchronizationException if unable to synchronize the reporting task with the proposed version
     * @throws TimeoutException if the reporting task is being removed and takes longer to stop than the timeout allowed by the {@link FlowSynchronizationOptions synchronization options}.
     */
    void synchronize(ReportingTaskNode reportingTask, VersionedReportingTask proposed, FlowSynchronizationOptions synchronizationOptions)
            throws FlowSynchronizationException, TimeoutException, InterruptedException, ReportingTaskInstantiationException;

    /**
     * Synchronizes the given Flow Analysis Rule to match the proposed one, or deletes the Flow Analysis Rule if the proposed is <code>null</code>.
     * If the given Flow Analysis Rule is <code>null</code>, creates it.
     *
     * @param flowAnalysisRule the flow analysis rule to synchronize
     * @param proposed the proposed/desired state
     * @param synchronizationOptions options for how to synchronize the flow
     *
     * @throws IllegalStateException if the flow analysis rule cannot be updated due to the state of the flow
     * @throws FlowSynchronizationException if unable to synchronize the flow analysis rule with the proposed version
     * @throws TimeoutException if the flow analysis rule is being removed and takes longer to stop than the timeout allowed by the {@link FlowSynchronizationOptions synchronization options}.
     */
    void synchronize(FlowAnalysisRuleNode flowAnalysisRule, VersionedFlowAnalysisRule proposed, FlowSynchronizationOptions synchronizationOptions)
            throws FlowSynchronizationException, TimeoutException, InterruptedException, FlowAnalysisRuleInstantiationException;

    /**
     * Synchronizes the given Remote Process Group to match the proposed one, or deletes the rpg if the proposed is <code>null</code>. If the given rpg is <code>null</code>, creates it and adds
     * it to the given ProcessGroup
     *
     * @param rpg the rpg to synchronize
     * @param proposed the proposed/desired state for the rpg
     * @param group the ProcessGroup to which the rpg should belong
     * @param synchronizationOptions options for how to synchronize the flow
     *
     * @throws IllegalStateException if the rpg cannot be updated due to the state of the flow
     * @throws FlowSynchronizationException if unable to synchronize the rpg with the proposed version
     * @throws TimeoutException if the rpg is being removed and takes longer to stop than the timeout allowed by the {@link FlowSynchronizationOptions synchronization options}.
     */
    void synchronize(RemoteProcessGroup rpg, VersionedRemoteProcessGroup proposed, ProcessGroup group, FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException, InterruptedException;


    void synchronize(ParameterContext parameterContext, VersionedParameterContext proposed, FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException, InterruptedException;

    void synchronizeProcessGroupSettings(ProcessGroup processGroup, VersionedProcessGroup proposed, ProcessGroup parentGroup, FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException, InterruptedException, ProcessorInstantiationException;


}
