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

import org.apache.nifi.authorization.resource.AccessPoliciesAuthorizable;
import org.apache.nifi.authorization.resource.AccessPolicyAuthorizable;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.UserGroupsAuthorizable;
import org.apache.nifi.authorization.resource.UsersAuthorizable;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.apache.nifi.web.dao.ControllerServiceDAO;
import org.apache.nifi.web.dao.FunnelDAO;
import org.apache.nifi.web.dao.LabelDAO;
import org.apache.nifi.web.dao.PortDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.apache.nifi.web.dao.ReportingTaskDAO;
import org.apache.nifi.web.dao.SnippetDAO;
import org.apache.nifi.web.dao.TemplateDAO;


class StandardAuthorizableLookup implements AuthorizableLookup {

    private static final UsersAuthorizable USERS_AUTHORIZABLE = new UsersAuthorizable();
    private static final UserGroupsAuthorizable USER_GROUPS_AUTHORIZABLE = new UserGroupsAuthorizable();
    private static final Authorizable ACCESS_POLICIES_AUTHORIZABLE = new AccessPoliciesAuthorizable();

    // nifi core components
    private ControllerFacade controllerFacade;

    // data access objects
    private ProcessorDAO processorDAO;
    private ProcessGroupDAO processGroupDAO;
    private RemoteProcessGroupDAO remoteProcessGroupDAO;
    private LabelDAO labelDAO;
    private FunnelDAO funnelDAO;
    private SnippetDAO snippetDAO;
    private PortDAO inputPortDAO;
    private PortDAO outputPortDAO;
    private ConnectionDAO connectionDAO;
    private ControllerServiceDAO controllerServiceDAO;
    private ReportingTaskDAO reportingTaskDAO;
    private TemplateDAO templateDAO;
    private AccessPolicyDAO accessPolicyDAO;

    @Override
    public Authorizable getProcessor(final String id) {
        return processorDAO.getProcessor(id);
    }

    @Override
    public Authorizable getInputPort(final String id) {
        return inputPortDAO.getPort(id);
    }

    @Override
    public Authorizable getOutputPort(final String id) {
        return outputPortDAO.getPort(id);
    }

    @Override
    public Authorizable getConnection(final String id) {
        return connectionDAO.getConnection(id);
    }

    @Override
    public Authorizable getProcessGroup(final String id) {
        return processGroupDAO.getProcessGroup(id);
    }

    @Override
    public Authorizable getRemoteProcessGroup(final String id) {
        return remoteProcessGroupDAO.getRemoteProcessGroup(id);
    }

    @Override
    public Authorizable getRemoteProcessGroupInputPort(final String remoteProcessGroupId, final String id) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        return remoteProcessGroup.getInputPort(id);
    }

    @Override
    public Authorizable getRemoteProcessGroupOutputPort(final String remoteProcessGroupId, final String id) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        return remoteProcessGroup.getOutputPort(id);
    }

    @Override
    public Authorizable getLabel(final String id) {
        return labelDAO.getLabel(id);
    }

    @Override
    public Authorizable getFunnel(final String id) {
        return funnelDAO.getFunnel(id);
    }

    @Override
    public Authorizable getControllerService(final String id) {
        return controllerServiceDAO.getControllerService(id);
    }

    @Override
    public Authorizable getControllerServiceReferencingComponent(String controllerSeriveId, String id) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerSeriveId);
        final ControllerServiceReference referencingComponents = controllerService.getReferences();

        ConfiguredComponent reference = null;
        for (final ConfiguredComponent component : referencingComponents.getReferencingComponents()) {
            if (component.getIdentifier().equals(id)) {
                reference = component;
                break;
            }
        }

        if (reference == null) {
            throw new ResourceNotFoundException("Unable to find referencing component with id " + id);
        }

        return reference;
    }

    @Override
    public Authorizable getReportingTask(final String id) {
        return reportingTaskDAO.getReportingTask(id);
    }

    @Override
    public Snippet getSnippet(final String id) {
        return snippetDAO.getSnippet(id);
    }

    @Override
    public Authorizable getUsersAuthorizable() {
        return USERS_AUTHORIZABLE;
    }

    @Override
    public Authorizable getUserGroupsAuthorizable() {
        return USER_GROUPS_AUTHORIZABLE;
    }

    @Override
    public Authorizable getAccessPoliciesAuthorizable() {
        return ACCESS_POLICIES_AUTHORIZABLE;
    }

    @Override
    public Authorizable getAccessPolicyAuthorizable(String id) {
        return new AccessPolicyAuthorizable(accessPolicyDAO.getAccessPolicy(id));
    }

    @Override
    public Authorizable getTemplate(final String id) {
        return templateDAO.getTemplate(id);
    }

    @Override
    public Authorizable getConnectable(String id) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(controllerFacade.getRootGroupId());
        return group.findConnectable(id);
    }

    public void setProcessorDAO(ProcessorDAO processorDAO) {
        this.processorDAO = processorDAO;
    }

    public void setProcessGroupDAO(ProcessGroupDAO processGroupDAO) {
        this.processGroupDAO = processGroupDAO;
    }

    public void setRemoteProcessGroupDAO(RemoteProcessGroupDAO remoteProcessGroupDAO) {
        this.remoteProcessGroupDAO = remoteProcessGroupDAO;
    }

    public void setLabelDAO(LabelDAO labelDAO) {
        this.labelDAO = labelDAO;
    }

    public void setFunnelDAO(FunnelDAO funnelDAO) {
        this.funnelDAO = funnelDAO;
    }

    public void setSnippetDAO(SnippetDAO snippetDAO) {
        this.snippetDAO = snippetDAO;
    }

    public void setInputPortDAO(PortDAO inputPortDAO) {
        this.inputPortDAO = inputPortDAO;
    }

    public void setOutputPortDAO(PortDAO outputPortDAO) {
        this.outputPortDAO = outputPortDAO;
    }

    public void setConnectionDAO(ConnectionDAO connectionDAO) {
        this.connectionDAO = connectionDAO;
    }

    public void setControllerServiceDAO(ControllerServiceDAO controllerServiceDAO) {
        this.controllerServiceDAO = controllerServiceDAO;
    }

    public void setReportingTaskDAO(ReportingTaskDAO reportingTaskDAO) {
        this.reportingTaskDAO = reportingTaskDAO;
    }

    public void setTemplateDAO(TemplateDAO templateDAO) {
        this.templateDAO = templateDAO;
    }

    public void setAccessPolicyDAO(AccessPolicyDAO accessPolicyDAO) {
        this.accessPolicyDAO = accessPolicyDAO;
    }

    public void setControllerFacade(ControllerFacade controllerFacade) {
        this.controllerFacade = controllerFacade;
    }
}
