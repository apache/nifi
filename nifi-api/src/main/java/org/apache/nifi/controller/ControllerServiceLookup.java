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

import java.util.Set;

public interface ControllerServiceLookup {

    /**
     * @param serviceIdentifier of controller service
     * @return the ControllerService that is registered with the given
     * identifier
     */
    ControllerService getControllerService(String serviceIdentifier);

    /**
     * @param serviceIdentifier identifier of service to check
     * @return <code>true</code> if the Controller Service with the given
     * identifier is enabled, <code>false</code> otherwise. If the given
     * identifier is not known by this ControllerServiceLookup, returns
     * <code>false</code>
     */
    boolean isControllerServiceEnabled(String serviceIdentifier);

    /**
     * @param serviceIdentifier identifier of service to check
     * @return <code>true</code> if the Controller Service with the given
     * identifier has been enabled but is still in the transitioning state,
     * otherwise returns <code>false</code>. If the given identifier is not
     * known by this ControllerServiceLookup, returns <code>false</code>
     */
    boolean isControllerServiceEnabling(String serviceIdentifier);

    /**
     * @param service service to check
     * @return <code>true</code> if the given Controller Service is enabled,
     * <code>false</code> otherwise. If the given Controller Service is not
     * known by this ControllerServiceLookup, returns <code>false</code>
     */
    boolean isControllerServiceEnabled(ControllerService service);

    /**
     *
     * @param serviceType type of service to get identifiers for
     *
     * @return the set of all Controller Service Identifiers whose Controller
     *         Service is of the given type.
     * @throws IllegalArgumentException if the given class is not an interface
     */
    Set<String> getControllerServiceIdentifiers(Class<? extends ControllerService> serviceType) throws IllegalArgumentException;

    /**
     * @param serviceIdentifier identifier to look up
     * @return the name of the Controller service with the given identifier. If
     * no service can be found with this identifier, returns {@code null}
     */
    String getControllerServiceName(String serviceIdentifier);
}
