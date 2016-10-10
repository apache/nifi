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

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.kerberos.KerberosContext;
import org.apache.nifi.logging.ComponentLog;

public interface ControllerServiceInitializationContext extends KerberosContext {

    /**
     * @return the identifier associated with the {@link ControllerService} with
     * which this context is associated
     */
    String getIdentifier();

    /**
     * @return the {@link ControllerServiceLookup} which can be used to obtain
     * Controller Services
     */
    ControllerServiceLookup getControllerServiceLookup();

    /**
     * @return a logger that can be used to log important events in a standard
     * way and generate bulletins when appropriate
     */
    ComponentLog getLogger();

    /**
     * @return the StateManager that can be used to store and retrieve state for this component
     */
    StateManager getStateManager();
}
