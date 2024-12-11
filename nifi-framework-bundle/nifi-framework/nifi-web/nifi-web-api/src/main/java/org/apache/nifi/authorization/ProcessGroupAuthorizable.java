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
import org.apache.nifi.groups.ProcessGroup;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Authorizable for a ProcessGroup and its encapsulated components.
 */
public interface ProcessGroupAuthorizable extends AuthorizableHolder {
    /**
     * Returns the Process Group that this Authorizable represents. Non null
     *
     * @return the Process Group
     */
    ProcessGroup getProcessGroup();

    /**
     * Returns the optional Parameter Context Authorizable.
     *
     * @return the Parameter Context authorizable
     */
    Optional<Authorizable> getParameterContextAuthorizable();

    /**
     * The authorizables for all encapsulated processors. Non null
     *
     * @return all encapsulated processors
     */
    Set<ComponentAuthorizable> getEncapsulatedProcessors();

    /**
     * The authorizables for all encapsulated processors that meet the specified predicate. Non null
     *
     * @param filter predicate to filter which processors should be included
     * @return all encapsulated processors that meet the specified predicate
     */
    Set<ComponentAuthorizable> getEncapsulatedProcessors(Predicate<org.apache.nifi.authorization.resource.ComponentAuthorizable> filter);

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
     * The authorizables for all encapsulated controller services. Non null
     *
     * @return all encapsulated controller services
     */
    Set<ComponentAuthorizable> getEncapsulatedControllerServices();

    /**
     * The authorizables for all encapsulated controller services that meet the specified predicate. Non null
     *
     * @param filter predicate to filter which controller services should be included
     * @return all controller services that meet the specified predicate
     */
    Set<ComponentAuthorizable> getEncapsulatedControllerServices(Predicate<org.apache.nifi.authorization.resource.ComponentAuthorizable> filter);

}
