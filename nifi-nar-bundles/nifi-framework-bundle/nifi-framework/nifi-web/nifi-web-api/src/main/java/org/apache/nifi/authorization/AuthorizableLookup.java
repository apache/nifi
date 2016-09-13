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
import org.apache.nifi.controller.Snippet;

public interface AuthorizableLookup {

    /**
     * Get the authorizable Controller.
     *
     * @return authorizable
     */
    Authorizable getController();

    /**
     * Get the authorizable Processor.
     *
     * @param id processor id
     * @return authorizable
     */
    ControllerServiceReferencingComponentAuthorizable getProcessor(String id);

    /**
     * Get the authorizable for this Processor. This will create a dummy instance of the
     * processor. The intent of this method is to provide access to the PropertyDescriptors
     * prior to the component being created.
     *
     * @param type processor type
     * @return authorizable
     */
    ControllerServiceReferencingComponentAuthorizable getProcessorByType(String type);

    /**
     * Get the authorizable for querying Provenance.
     *
     * @return authorizable
     */
    Authorizable getProvenance();

    /**
     * Get the authorizable for viewing/reseting Counters.
     *
     * @return authorizable
     */
    Authorizable getCounters();

    /**
     * Get the authorizable RootGroup InputPort.
     *
     * @param id input port id
     * @return authorizable
     */
    RootGroupPortAuthorizable getRootGroupInputPort(String id);

    /**
     * Get the authorizable RootGroup OutputPort.
     *
     * @param id output port id
     * @return authorizable
     */
    RootGroupPortAuthorizable getRootGroupOutputPort(String id);

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
    ConnectionAuthorizable getConnection(String id);

    /**
     * Get the authorizable ProcessGroup.
     *
     * @param id process group id
     * @return authorizable
     */
    ProcessGroupAuthorizable getProcessGroup(String id);

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
    ControllerServiceReferencingComponentAuthorizable getControllerService(String id);

    /**
     * Get the authorizable for this Controller Service. This will create a dummy instance of the
     * controller service. The intent of this method is to provide access to the PropertyDescriptors
     * prior to the component being created.
     *
     * @param type processor type
     * @return authorizable
     */
    ControllerServiceReferencingComponentAuthorizable getControllerServiceByType(String type);

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
    ControllerServiceReferencingComponentAuthorizable getReportingTask(String id);

    /**
     * Get the authorizable for this Reporting Task. This will create a dummy instance of the
     * reporting task. The intent of this method is to provide access to the PropertyDescriptors
     * prior to the component being created.
     *
     * @param type processor type
     * @return authorizable
     */
    ControllerServiceReferencingComponentAuthorizable getReportingTaskByType(String type);

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
     * Get the {@link Authorizable} that represents the resource of users and user groups.
     * @return authorizable
     */
    Authorizable getTenant();

    /**
     * Get the authorizable for data of a specified component.
     *
     * @return authorizable
     */
    Authorizable getData(String id);

    /**
     * Get the authorizable for access all policies.
     *
     * @return authorizable
     */
    Authorizable getPolicies();

    /**
     * Get the authorizable for the policy of the policy id.
     *
     * @param id id
     * @return authorizable
     */
    Authorizable getAccessPolicyById(String id);

    /**
     * Get the authorizable for the policy of the specified resource.
     *
     * @param resource resource
     * @return authorizable
     */
    Authorizable getAccessPolicyByResource(String resource);

    /**
     * Get the authorizable of the specified resource.
     *
     * @param resource resource
     * @return authorizable
     */
    Authorizable getAuthorizableFromResource(final String resource);
}
