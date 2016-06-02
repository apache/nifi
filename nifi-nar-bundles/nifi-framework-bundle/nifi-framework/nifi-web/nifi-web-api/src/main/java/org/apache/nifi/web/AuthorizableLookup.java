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
package org.apache.nifi.web;

import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.controller.Snippet;

public interface AuthorizableLookup {

    /**
     * Get the authorizable Processor.
     *
     * @param id processor id
     * @return authorizable
     */
    Authorizable getProcessor(String id);

    /**
     * Get the authorizable InputPort.
     *
     * @param id input port id
     * @return authorizable
     */
    Authorizable getInputPort(String id);

    /**
     * Get the authorizable OutputPort.
     *
     * @param id output port id
     * @return authorizable
     */
    Authorizable getOutputPort(String id);

    /**
     * Get the authorizable Connection.
     *
     * @param id connection id
     * @return authorizable
     */
    Authorizable getConnection(String id);

    /**
     * Get the authorizable ProcessGroup.
     *
     * @param id process group id
     * @return authorizable
     */
    Authorizable getProcessGroup(String id);

    /**
     * Get the authorizable RemoteProcessGroup.
     *
     * @param id remote process group id
     * @return authorizable
     */
    Authorizable getRemoteProcessGroup(String id);

    /**
     * Get the authorizable RemoteProcessGroup input port.
     *
     * @param remoteProcessGroupId remote process group id
     * @param id input port id
     * @return authorizable
     */
    Authorizable getRemoteProcessGroupInputPort(String remoteProcessGroupId, String id);

    /**
     * Get the authorizable RemoteProcessGroup output port.
     *
     * @param remoteProcessGroupId remote process group id
     * @param id output port id
     * @return authorizable
     */
    Authorizable getRemoteProcessGroupOutputPort(String remoteProcessGroupId, String id);

    /**
     * Get the authorizable Label.
     *
     * @param id label id
     * @return authorizable
     */
    Authorizable getLabel(String id);

    /**
     * Get the authorizable Funnel.
     *
     * @param id funnel id
     * @return authorizable
     */
    Authorizable getFunnel(String id);

    /**
     * Get the authorizable ControllerService.
     *
     * @param id controller service id
     * @return authorizable
     */
    Authorizable getControllerService(String id);

    /**
     * Get the authorizable referencing component.
     *
     * @param controllerSeriveId controller service id
     * @param id component id
     * @return authorizable
     */
    Authorizable getControllerServiceReferencingComponent(String controllerSeriveId, String id);

    /**
     * Get the authorizable ReportingTask.
     *
     * @param id reporting task id
     * @return authorizable
     */
    Authorizable getReportingTask(String id);

    /**
     * Get the authorizable Template.
     *
     * @param id template id
     * @return authorizable
     */
    Authorizable getTemplate(String id);

    /**
     * Get the authorizable connectable.
     *
     * @param id connectable id
     * @return authorizable
     */
    Authorizable getConnectable(String id);

    /**
     * Get the snippet of authorizable's.
     *
     * @param id snippet id
     * @return snippet of authorizable's
     */
    Snippet getSnippet(String id);

    /**
     * Get the {@link Authorizable} that represents the resource of {@link User}s.
     * @return authorizable
     */
    Authorizable getUsersAuthorizable();

    /**
     * Get the {@link Authorizable} that represents the resource of {@link Group}s.
     * @return authorizable
     */
    Authorizable getUserGroupsAuthorizable();

    /**
     * Get the {@link Authorizable} the represents the parent resource of {@link AccessPolicy} resources.
     * @return authorizable
     */
    Authorizable getAccessPoliciesAuthorizable();

    /**
     * Get the {@link Authorizable} the represents the {@link AccessPolicy} with the given ID.
     * @param id access policy ID
     * @return authorizable
     */
    Authorizable getAccessPolicyAuthorizable(String id);
}
