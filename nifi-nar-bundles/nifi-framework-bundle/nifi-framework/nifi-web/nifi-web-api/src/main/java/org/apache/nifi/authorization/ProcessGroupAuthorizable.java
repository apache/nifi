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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.resource.Authorizable;

import java.util.Set;

/**
 * Authorizable for a ProcessGroup and its encapsulated components.
 */
public interface ProcessGroupAuthorizable {
    /**
     * Returns the authorizable for this ProcessGroup. Non null
     *
     * @return authorizable
     */
    Authorizable getAuthorizable();

    /**
     * The authorizables for all encapsulated processors. Non null
     *
     * @return all encapsulated processors
     */
    Set<ComponentAuthorizable> getEncapsulatedProcessors();

    /**
     * The authorizables for all encapsulated connections. Non null
     *
     * @return all encapsulated connections
     */
    Set<ConnectionAuthorizable> getEncapsulatedConnections();

    /**
     * The authorizables for all encapsulated input ports. Non null
     *
     * @return all encapsulated input ports
     */
    Set<Authorizable> getEncapsulatedInputPorts();

    /**
     * The authorizables for all encapsulated output ports. Non null
     *
     * @return all encapsulated output ports
     */
    Set<Authorizable> getEncapsulatedOutputPorts();

    /**
     * The authorizables for all encapsulated funnels. Non null
     *
     * @return all encapsulated funnels
     */
    Set<Authorizable> getEncapsulatedFunnels();

    /**
     * The authorizables for all encapsulated labels. Non null
     *
     * @return all encapsulated labels
     */
    Set<Authorizable> getEncapsulatedLabels();

    /**
     * The authorizables for all encapsulated process groups. Non null
     *
     * @return all encapsulated process groups
     */
    Set<ProcessGroupAuthorizable> getEncapsulatedProcessGroups();

    /**
     * The authorizables for all encapsulated remote process groups. Non null
     *
     * @return all encapsulated remote process groups
     */
    Set<Authorizable> getEncapsulatedRemoteProcessGroups();

    /**
     * The authorizables for all encapsulated templates. Non null
     *
     * @return all encapsulated templates
     */
    Set<Authorizable> getEncapsulatedTemplates();

    /**
     * The authorizables for all encapsulated input ports. Non null
     *
     * @return all encapsulated input ports
     */
    Set<ComponentAuthorizable> getEncapsulatedControllerServices();

}
