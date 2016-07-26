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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.resource.AccessPolicyAuthorizable;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.DataTransferAuthorizable;
import org.apache.nifi.authorization.resource.DataAuthorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.resource.TenantAuthorizable;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.web.ResourceNotFoundException;
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

    private static final TenantAuthorizable TENANT_AUTHORIZABLE = new TenantAuthorizable();
    private static final Authorizable POLICIES_AUTHORIZABLE = new Authorizable() {
        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getPoliciesResource();
        }
    };

    private static final Authorizable PROVENANCE_AUTHORIZABLE = new Authorizable() {
        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getProvenanceResource();
        }
    };

    private static final Authorizable COUNTERS_AUTHORIZABLE = new Authorizable() {
        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getCountersResource();
        }
    };

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
    public Authorizable getController() {
        return controllerFacade;
    }

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
    public ConnectionAuthorizable getConnection(final String id) {
        final Connection connection = connectionDAO.getConnection(id);
        return new ConnectionAuthorizable() {
            @Override
            public Authorizable getAuthorizable() {
                return connection;
            }

            @Override
            public Connectable getSource() {
                return connection.getSource();
            }

            @Override
            public Connectable getDestination() {
                return connection.getDestination();
            }

            @Override
            public ProcessGroup getParentGroup() {
                return connection.getProcessGroup();
            }
        };
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
    public Authorizable getProvenance() {
        return PROVENANCE_AUTHORIZABLE;
    }

    @Override
    public Authorizable getCounters() {
        return COUNTERS_AUTHORIZABLE;
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
    public Authorizable getTenant() {
        return TENANT_AUTHORIZABLE;
    }

    @Override
    public Authorizable getData(final String id) {
        return controllerFacade.getDataAuthorizable(id);
    }

    @Override
    public Authorizable getPolicies() {
        return POLICIES_AUTHORIZABLE;
    }

    @Override
    public Authorizable getAccessPolicyById(final String id) {
        final AccessPolicy policy = accessPolicyDAO.getAccessPolicy(id);
        return getAccessPolicyByResource(policy.getResource());
    }

    @Override
    public Authorizable getAccessPolicyByResource(final String resource) {
        try {
            return new AccessPolicyAuthorizable(getAuthorizableFromResource(resource));
        } catch (final ResourceNotFoundException e) {
            // the underlying component has been removed or resource is invalid... require /policies permissions
            return POLICIES_AUTHORIZABLE;
        }
    }

    @Override
    public Authorizable getAuthorizableFromResource(String resource) {
        // parse the resource type
        ResourceType resourceType = null;
        for (ResourceType type : ResourceType.values()) {
            if (resource.equals(type.getValue()) || resource.startsWith(type.getValue() + "/")) {
                resourceType = type;
            }
        }

        if (resourceType == null) {
            throw new ResourceNotFoundException("Unrecognized resource: " + resource);
        }

        // if this is a policy or a provenance event resource, there should be another resource type
        if (ResourceType.Policy.equals(resourceType) || ResourceType.Data.equals(resourceType) || ResourceType.DataTransfer.equals(resourceType)) {
            final ResourceType primaryResourceType = resourceType;

            // get the resource type
            resource = StringUtils.substringAfter(resource, resourceType.getValue());

            for (ResourceType type : ResourceType.values()) {
                if (resource.equals(type.getValue()) || resource.startsWith(type.getValue() + "/")) {
                    resourceType = type;
                }
            }

            if (resourceType == null) {
                throw new ResourceNotFoundException("Unrecognized resource: " + resource);
            }

            // must either be a policy, event, or data transfer
            if (ResourceType.Policy.equals(primaryResourceType)) {
                return new AccessPolicyAuthorizable(getAccessPolicy(resourceType, resource));
            } else if (ResourceType.Data.equals(primaryResourceType)) {
                return new DataAuthorizable(getAccessPolicy(resourceType, resource));
            } else {
                return new DataTransferAuthorizable(getAccessPolicy(resourceType, resource));
            }
        } else {
            return getAccessPolicy(resourceType, resource);
        }
    }

    private Authorizable getAccessPolicy(final ResourceType resourceType, final String resource) {
        final String slashComponentId = StringUtils.substringAfter(resource, resourceType.getValue());
        if (slashComponentId.startsWith("/")) {
            return getAccessPolicyByResource(resourceType, slashComponentId.substring(1));
        } else {
            return getAccessPolicyByResource(resourceType);
        }
    }

    private Authorizable getAccessPolicyByResource(final ResourceType resourceType, final String componentId) {
        Authorizable authorizable = null;
        switch (resourceType) {
            case ControllerService:
                authorizable = getControllerService(componentId);
                break;
            case Funnel:
                authorizable = getFunnel(componentId);
                break;
            case InputPort:
                authorizable = getInputPort(componentId);
                break;
            case Label:
                authorizable = getLabel(componentId);
                break;
            case OutputPort:
                authorizable = getOutputPort(componentId);
                break;
            case Processor:
                authorizable = getProcessor(componentId);
                break;
            case ProcessGroup:
                authorizable = getProcessGroup(componentId);
                break;
            case RemoteProcessGroup:
                authorizable = getRemoteProcessGroup(componentId);
                break;
            case ReportingTask:
                authorizable = getReportingTask(componentId);
                break;
            case Template:
                authorizable = getTemplate(componentId);
                break;
            case Data:
                authorizable = controllerFacade.getDataAuthorizable(componentId);
                break;
        }

        if (authorizable == null) {
            throw new IllegalArgumentException("An unexpected type of resource in this policy " + resourceType.getValue());
        }

        return authorizable;
    }

    private Authorizable getAccessPolicyByResource(final ResourceType resourceType) {
        Authorizable authorizable = null;
        switch (resourceType) {
            case Controller:
                authorizable = getController();
                break;
            case Counters:
                authorizable = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return null;
                    }

                    @Override
                    public Resource getResource() {
                        return ResourceFactory.getCountersResource();
                    }
                };
                break;
            case Flow:
                authorizable = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return null;
                    }

                    @Override
                    public Resource getResource() {
                        return ResourceFactory.getFlowResource();
                    }
                };
                break;
            case Provenance:
                authorizable = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return null;
                    }

                    @Override
                    public Resource getResource() {
                        return ResourceFactory.getProvenanceResource();
                    }
                };
                break;
            case Proxy:
                authorizable = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return null;
                    }

                    @Override
                    public Resource getResource() {
                        return ResourceFactory.getProxyResource();
                    }
                };
                break;
            case Policy:
                authorizable = POLICIES_AUTHORIZABLE;
                break;
            case Resource:
                authorizable = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return null;
                    }

                    @Override
                    public Resource getResource() {
                        return ResourceFactory.getResourceResource();
                    }
                };
                break;
            case SiteToSite:
                // TODO - new site-to-site and port specific site-to-site
                authorizable = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return null;
                    }

                    @Override
                    public Resource getResource() {
                        return ResourceFactory.getSiteToSiteResource();
                    }
                };
                break;
            case System:
                authorizable = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return null;
                    }

                    @Override
                    public Resource getResource() {
                        return ResourceFactory.getSystemResource();
                    }
                };
                break;
            case Tenant:
                authorizable = getTenant();
                break;
        }

        if (authorizable == null) {
            throw new IllegalArgumentException("An unexpected type of resource in this policy " + resourceType.getValue());
        }

        return authorizable;
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
