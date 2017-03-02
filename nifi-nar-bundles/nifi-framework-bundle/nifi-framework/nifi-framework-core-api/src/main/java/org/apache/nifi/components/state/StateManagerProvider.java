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

package org.apache.nifi.components.state;

/**
 * <p>
 * Interface that provides a mechanism for obtaining the {@link StateManager} for a particular component
 * </p>
 */
public interface StateManagerProvider {
    /**
     * Returns the StateManager for the component with the given ID, or <code>null</code> if no State Manager
     * exists for the component with the given ID
     *
     * @param componentId the id of the component for which the StateManager should be returned
     *
     * @return the StateManager for the component with the given ID, or <code>null</code> if no State Manager
     *         exists for the component with the given ID
     */
    StateManager getStateManager(String componentId);

    /**
     * Notifies the State Manager Provider that the component with the given ID has been removed from the NiFi instance
     * and will no longer be needed, so the appropriate resource cleanup can take place.
     *
     * @param componentId the ID of the component that has been removed
     */
    void onComponentRemoved(String componentId);

    /**
     * Shuts down the state managers, cleaning up any resources that they occupy
     */
    void shutdown();

    /**
     * Initializes the Cluster State Provider and enables it for use
     */
    void enableClusterProvider();

    /**
     * Disables the Cluster State Provider and begins using the Local State Provider to persist and retrieve
     * state, even when components request a clustered provider
     */
    void disableClusterProvider();
}
