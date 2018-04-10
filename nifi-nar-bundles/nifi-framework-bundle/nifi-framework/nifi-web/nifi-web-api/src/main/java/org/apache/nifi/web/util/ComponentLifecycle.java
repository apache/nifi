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

package org.apache.nifi.web.util;

import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;

import java.net.URI;
import java.util.Set;

public interface ComponentLifecycle {
    /**
     * Updates the scheduled state of all components that are given, to match the desired ScheduledState
     *
     * @param exampleUri an URI to use as a base for the REST API.
     * @param groupId the ID of the process group
     * @param components the components to schedule or unschedule
     * @param desiredState the desired state of the components
     * @param pause a pause that can be used to determine how long to wait between polling for task completion and that can also be used to cancel the operation
     *
     * @return the set of all AffectedComponents that are updated by the request, including the new Revisions
     *
     * @throws IllegalStateException if any of the components given do not have a state that can be transitioned to the given desired state
     */
    Set<AffectedComponentEntity> scheduleComponents(URI exampleUri, String groupId, Set<AffectedComponentEntity> components,
        ScheduledState desiredState, Pause pause) throws LifecycleManagementException;

    /**
     * Updates the Controller Service State state of all controller services that are given, to match the desired ControllerServiceState
     *
     * @param exampleUri an URI to use as a base for the REST API
     * @param groupId the ID of the process group
     * @param services the controller services to enable or disable
     * @param desiredState the desired state of the components
     * @param pause a pause that can be used to determine how long to wait between polling for task completion and that can also be used to cancel the operation
     *
     * @return the set of all AffectedComponents that are updated by the request, including the new Revisions
     *
     * @throws IllegalStateException if any of the components given do not have a state that can be transitioned to the given desired state
     */
    Set<AffectedComponentEntity> activateControllerServices(URI exampleUri, String groupId, Set<AffectedComponentEntity> services,
        ControllerServiceState desiredState, Pause pause) throws LifecycleManagementException;
}
