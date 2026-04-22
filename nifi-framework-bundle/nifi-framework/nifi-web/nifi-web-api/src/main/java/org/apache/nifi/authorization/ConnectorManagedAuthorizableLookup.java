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

/**
 * Facade exposing authorizable lookups that include components within Connector-managed Process Groups.
 *
 * <p>The standard {@link AuthorizableLookup} intentionally hides components inside Connector-managed Process Group
 * hierarchies unless the owning Connector is in Troubleshooting mode. The few Connector-scoped REST endpoints that
 * must authorize access to components inside a managed flow regardless of the Connector's state obtain those
 * authorizables through this facade, so the standard {@link AuthorizableLookup} surface stays free of the
 * "include connector managed" flag.</p>
 *
 * <p>Obtain an instance via {@link AuthorizableLookup#forConnectorManagedFlow()}.</p>
 */
public interface ConnectorManagedAuthorizableLookup {

    /**
     * Get the authorizable Processor located within a Connector-managed Process Group.
     */
    ComponentAuthorizable getProcessor(String id);

    /**
     * Get the authorizable input Port located within a Connector-managed Process Group.
     */
    Authorizable getInputPort(String id);

    /**
     * Get the authorizable output Port located within a Connector-managed Process Group.
     */
    Authorizable getOutputPort(String id);

    /**
     * Get the authorizable Connection located within a Connector-managed Process Group.
     */
    ConnectionAuthorizable getConnection(String id);

    /**
     * Get the authorizable Process Group, including Connector-managed Process Groups.
     */
    ProcessGroupAuthorizable getProcessGroup(String id);

    /**
     * Get the authorizable Remote Process Group located within a Connector-managed Process Group.
     */
    Authorizable getRemoteProcessGroup(String id);

    /**
     * Get the authorizable Controller Service located within a Connector-managed Process Group.
     */
    ComponentAuthorizable getControllerService(String id);
}
