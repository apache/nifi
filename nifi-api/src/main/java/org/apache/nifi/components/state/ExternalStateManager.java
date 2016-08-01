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

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;

import java.io.IOException;
import java.util.Collection;

/**
 * <p>
 * The ExternalStateManager is responsible for providing NiFi a mechanism for retrieving
 * and clearing state stored in an external system a NiFi component interact with.
 * </p>
 *
 * <p>
 * This mechanism is designed to allow Data Flow Managers to easily view and clear
 * component state stored externally.
 * It is advisable to implement this interface for components those have such state.
 * </p>
 *
 * <p>
 * When calling methods in this class, the state is always retrieved/cleared from external system
 * regardless NiFi instance is a part of a cluster or standalone.
 * </p>
 *
 * <p>
 * Any component that wishes to implement ExternalStateManager should also use the {@link Stateful} annotation
 * with {@link Scope#EXTERNAL} to provide a description of what state is being stored.
 * If this annotation is not present, the UI will not expose such information or allow DFMs to clear the state.
 * </p>
 *
 */
public interface ExternalStateManager {

    /**
     * To access an external system from those method implementations, configured property values of the component will be needed,
     * such as Database connection URL or Kafka broker address.
     * NiFi framework will call {@link #validateExternalStateAccess} method, to determine if required properties are set
     * to access an external system, before {@link #getExternalState} or {@link #clearExternalState} is called.
     * In this method, implementations has to validate configured properties,
     * and also capture configured property values to use at getExternalState and clearExternalState.
     * @param context validation context
     * @return validation error result
     */
    Collection<ValidationResult> validateExternalStateAccess(final ValidationContext context);

    /**
     * Returns the current state for the component. This return value may be <code>null</code>.
     *
     * @return the current state for the component or null if there is no state is retrieved
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    StateMap getExternalState() throws IOException;

    /**
     * Clears all keys and values from the component's state
     *
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    void clearExternalState() throws IOException;

    /**
     * <p>
     * In a clustered environment, implementations of {@link ExternalStateManager} interface is called based on the
     * {@link ExternalStateScope} value returned by its {@link #getExternalStateScope()} method.
     * </p>
     */
    enum ExternalStateScope {
        /**
         * State is to be treated as "global" across the cluster. I.e., the same component on all nodes will
         * have access to the same state.
         */
        CLUSTER,

        /**
         * State is to be treated per node. I.e., the same component will have different state in the external system for
         * each node in the cluster.
         */
        NODE
    }

    /**
     * @return An external state scope defining how this state manager should behave.
     */
    ExternalStateScope getExternalStateScope();

}
