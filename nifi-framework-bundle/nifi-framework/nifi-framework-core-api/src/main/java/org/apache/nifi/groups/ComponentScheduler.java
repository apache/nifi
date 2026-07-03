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
import org.apache.nifi.flow.ScheduledState;

import java.util.Collection;

public interface ComponentScheduler {
    void startComponent(Connectable component);

    void stopComponent(Connectable component);

    void transitionComponentState(Connectable component, ScheduledState desiredState);

    void enableControllerServicesAsync(Collection<ControllerServiceNode> controllerServices);

    /**
     * Called during a flow synchronization with the Controller Services that are newly added by the synchronization
     * (i.e., that did not exist in the flow before). Because Controller Services are always persisted in a versioned
     * flow as DISABLED, they are never included in the "proposed state is ENABLED" set that is handed to
     * {@link #enableControllerServicesAsync(Collection)}. This method gives a scheduler the opportunity to enable
     * newly added services based on its own policy.
     *
     * <p>The default implementation is a no-op: schedulers that restore components to a persisted state (startup,
     * cluster reconnect) enable services through their normal proposed-state handling and must not enable a service
     * that was not previously enabled. Only a scheduler that retains the existing runtime state during an update
     * (a versioned-flow upgrade) overrides this to enable newly added services when the process group is active,
     * mirroring how newly added processors are started.</p>
     *
     * @param controllerServices the Controller Services that were newly added by the synchronization
     */
    default void enableAddedControllerServicesAsync(Collection<ControllerServiceNode> controllerServices) {
    }

    void disableControllerServicesAsync(Collection<ControllerServiceNode> controllerServices);

    void startReportingTask(ReportingTaskNode reportingTask);

    void pause();

    void resume();

    void startStatelessGroup(ProcessGroup group);

    void stopStatelessGroup(ProcessGroup group);

    ComponentScheduler NOP_SCHEDULER = new ComponentScheduler() {
        @Override
        public void startComponent(final Connectable component) {
        }

        @Override
        public void stopComponent(final Connectable component) {
        }

        @Override
        public void transitionComponentState(final Connectable component, final ScheduledState desiredState) {
        }

        @Override
        public void enableControllerServicesAsync(final Collection<ControllerServiceNode> controllerServices) {
        }

        @Override
        public void disableControllerServicesAsync(final Collection<ControllerServiceNode> controllerServices) {
        }

        @Override
        public void startReportingTask(final ReportingTaskNode reportingTask) {
        }

        @Override
        public void pause() {
        }

        @Override
        public void resume() {
        }

        @Override
        public void startStatelessGroup(final ProcessGroup group) {
        }

        @Override
        public void stopStatelessGroup(final ProcessGroup group) {
        }
    };
}
