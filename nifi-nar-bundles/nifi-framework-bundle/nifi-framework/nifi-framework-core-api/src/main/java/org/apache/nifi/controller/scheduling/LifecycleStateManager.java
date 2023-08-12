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

package org.apache.nifi.controller.scheduling;

import java.util.Optional;

public interface LifecycleStateManager {
    /**
     * Returns the LifecycleState that is registered for the given component; if
     * no LifecycleState currently is registered, one is created and registered
     * atomically, and then that value is returned.
     *
     * @param componentId the ID of the component that the lifecycle state is associated with
     * @param replaceTerminatedState if there exists a LifecycleState for the given component but that component has been terminated,
     * indicates whether it should be replaced with a new one or not
     * @param replaceUnscheduledState if there exists a LifecycleState for the given component and that component has been unscheduled,
     * indicates whether it should be replaced with a new one or not
     * @return the lifecycle state for the given schedulable component
     */
    LifecycleState getOrRegisterLifecycleState(String componentId, boolean replaceTerminatedState, boolean replaceUnscheduledState);

    /**
     * Returns the LifecycleState that is registered for the given component; if no LifecycleState
     * is registered, returns an empty Optional.
     * @param componentId the ID of the component that the lifecycle state is associated with
     * @return the lifecycle state associated with the given component, or an empty Optional
     */
    Optional<LifecycleState> getLifecycleState(String componentId);

    /**
     * Removes the LifecycleState for the given component and returns the value that was removed, or an empty Optional
     * if there was no state registered
     * @param componentId the ID of the component that the lifecycle state is associated with
     * @return the removed state or an empty optional
     */
    Optional<LifecycleState> removeLifecycleState(String componentId);
}
