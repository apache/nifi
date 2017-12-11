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
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;

public interface AuthorizableLookup {

    /**
     * Get the authorizable Controller.
     *
     * @return authorizable
     */
    Authorizable getController();

    /**
     * Get the authorizable for the given type and bundle. This will use a dummy instance of the
     * component. The intent of this method is to provide access to the PropertyDescriptors
     * prior to the component being created.
     *
     * @param type component type
     * @param bundle the bundle for the component
     * @return authorizable
     */
    ComponentAuthorizable getConfigurableComponent(String type, BundleDTO bundle);

    /**
     * Get the authorizable Processor.
     *
     * @param id processor id
     * @return authorizable
     */
    ComponentAuthorizable getProcessor(String id);

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
     * Get the authorizable for retrieving resources.
     *
     * @return authorizable
     */
    Authorizable getResource();

    /**
     * Get the authorizable for site to site.
     *
     * @return authorizable
     */
    Authorizable getSiteToSite();

    /**
     * Get the authorizable for the flow.
     *
     * @return authorizable
     */
    Authorizable getFlow();

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
    ComponentAuthorizable getControllerService(String id);

    /**
     * Get the authorizable referencing component.
     *
     * @param controllerServiceId controller service id
     * @param id component id
     * @return authorizable
     */
    Authorizable getControllerServiceReferencingComponent(String controllerServiceId, String id);

    /**
     * Get the authorizable ReportingTask.
     *
     * @param id reporting task id
     * @return authorizable
     */
    ComponentAuthorizable getReportingTask(String id);

    /**
     * Get the authorizable Template.
     *
     * @param id template id
     * @return authorizable
     */
    Authorizable getTemplate(String id);

    /**
     * Get the authorizable Template contents.
     *
     * @param snippet the template contents
     * @return authorizable
     */
    TemplateContentsAuthorizable getTemplateContents(FlowSnippetDTO snippet);

    /**
     * Get the authorizable connectable. Note this does not include RemoteGroupPorts.
     *
     * @param id connectable id
     * @return authorizable
     */
    Authorizable getLocalConnectable(String id);

    /**
     * Get the snippet of authorizable's.
     *
     * @param id snippet id
     * @return snippet of authorizable's
     */
    SnippetAuthorizable getSnippet(String id);

    /**
     * Get the {@link Authorizable} that represents the resource of users and user groups.
     * @return authorizable
     */
    Authorizable getTenant();

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


    /**
     * Get the authorizable for access to the System resource.
     *
     * @return authorizable
     */
    Authorizable getSystem();

    /**
     * Get the authorizable for accessing restricted components.
     *
     * @return authorizable
     */
    Authorizable getRestrictedComponents();
}
