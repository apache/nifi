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

import java.io.IOException;
import java.util.Map;

import org.apache.nifi.components.ConfigurableComponent;

/**
 * <p>
 * Provides a mechanism by which components can store and retrieve state. Depending on the Provider, the state
 * may be stored locally, or it may be stored on a remote resource.
 * </p>
 *
 * <p>
 * Which implementation should be used for local and clustered state is configured in the NiFi properties file.
 * It is therefore possible to provide custom implementations of this interface. Note, however, that this interface
 * is new as of version 0.5.0 of Apache NiFi and may not be considered "stable" as of yet. Therefore, it is subject
 * to change without notice, so providing custom implementations is cautioned against until the API becomes more stable.
 * </p>
 *
 * @since 0.5.0
 */
public interface StateProvider extends ConfigurableComponent {

    /**
     * Initializes the StateProvider so that it is capable of being used. This method will be called
     * once before any of the other methods are called and will not be called again until the {@link #shutdown()}
     * method has been called
     *
     * @param context the initialization context that can be used to prepare the state provider for use
     */
    void initialize(StateProviderInitializationContext context) throws IOException;

    /**
     * Shuts down the StateProvider and cleans up any resources held by it. Once this method has returned, the
     * StateProvider may be initialized once again via the {@link #initialize(StateProviderInitializationContext)} method.
     */
    void shutdown();

    /**
     * Updates the value of the component's state, setting the new value to the
     * given state
     *
     * @param state the value to change the state to
     * @param componentId the id of the component for which state is being set
     *
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    void setState(Map<String, String> state, String componentId) throws IOException;


    /**
     * Returns the currently configured state for the component. The returned StateMap will never be null.
     * The version of the StateMap will be -1 and the state will contain no key/value pairs if the state has never been set.
     *
     * @param componentId the id of the component for which state is to be retrieved
     * @return the currently configured value for the component's state
     *
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    StateMap getState(String componentId) throws IOException;


    /**
     * Updates the value of the component's state to the new value if and only if the value currently
     * is the same as the given oldValue.
     *
     * @param oldValue the old value to compare against
     * @param newValue the new value to use if and only if the state's value is the same as the given oldValue
     * @param componentId the id of the component for which state is being retrieved
     * @return <code>true</code> if the state was updated to the new value, <code>false</code> if the state's value was not
     *         equal to oldValue
     *
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    boolean replace(StateMap oldValue, Map<String, String> newValue, String componentId) throws IOException;

    /**
     * Removes all values from the component's state that is stored using the given scope
     *
     * @param componentId the id of the component for which state is being cleared
     *
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    void clear(String componentId) throws IOException;

    /**
     * This method is called whenever a component is removed from the NiFi instance. This allows the State Provider to
     * perform tasks when a component is removed in order to clean up resources that may be associated with that component
     *
     * @param componentId the ID of the component that was added to the NiFi instance
     * @throws IOException if unable to perform the necessary cleanup
     */
    void onComponentRemoved(String componentId) throws IOException;

    /**
     * Notifies the state provider that it should begin servicing requests to store and retrieve state
     */
    void enable();

    /**
     * Notifies the state provider that it should stop servicing requests to store and retrieve state and instead throw a ProviderDisabledException if any request is made to do so
     */
    void disable();

    /**
     * @return <code>true</code> if the provider is enabled, <code>false</code> otherwise.
     */
    boolean isEnabled();
}
