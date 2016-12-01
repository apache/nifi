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
import org.apache.nifi.authorization.resource.DataAuthorizable;
import org.apache.nifi.authorization.resource.DataTransferAuthorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.resource.RestrictedComponentsAuthorizable;
import org.apache.nifi.authorization.resource.TenantAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.remote.PortAuthorizationResult;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


class StandardAuthorizableLookup implements AuthorizableLookup {

    private static final TenantAuthorizable TENANT_AUTHORIZABLE = new TenantAuthorizable();
    private static final Authorizable RESTRICTED_COMPONENTS_AUTHORIZABLE = new RestrictedComponentsAuthorizable();

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

    private static final Authorizable SYSTEM_AUTHORIZABLE = new Authorizable() {
        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getSystemResource();
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
    public ConfigurableComponentAuthorizable getProcessor(final String id) {
        final ProcessorNode processorNode = processorDAO.getProcessor(id);
        return new ProcessorConfigurableComponentAuthorizable(processorNode);
    }

    @Override
    public ConfigurableComponentAuthorizable getProcessorByType(String type) {
        try {
            final ProcessorNode processorNode = controllerFacade.createTemporaryProcessor(type);
            return new ProcessorConfigurableComponentAuthorizable(processorNode);
        } catch (final Exception e) {
            throw new AccessDeniedException("Unable to create processor to verify if it references any Controller Services.");
        }
    }

    @Override
    public RootGroupPortAuthorizable getRootGroupInputPort(String id) {
        final Port inputPort = inputPortDAO.getPort(id);

        if (!(inputPort instanceof RootGroupPort)) {
            throw new IllegalArgumentException(String.format("The specified id '%s' does not represent an input port in the root group.", id));
        }

        final DataTransferAuthorizable baseAuthorizable = new DataTransferAuthorizable(inputPort);
        return new RootGroupPortAuthorizable() {
            @Override
            public Authorizable getAuthorizable() {
                return baseAuthorizable;
            }

            @Override
            public AuthorizationResult checkAuthorization(NiFiUser user) {
                // perform the authorization of the user by using the underlying component, ensures consistent authorization with raw s2s
                final PortAuthorizationResult authorizationResult = ((RootGroupPort) inputPort).checkUserAuthorization(user);
                if (authorizationResult.isAuthorized()) {
                    return AuthorizationResult.approved();
                } else {
                    return AuthorizationResult.denied(authorizationResult.getExplanation());
                }
            }
        };
    }

    @Override
    public RootGroupPortAuthorizable getRootGroupOutputPort(String id) {
        final Port outputPort = outputPortDAO.getPort(id);

        if (!(outputPort instanceof RootGroupPort)) {
            throw new IllegalArgumentException(String.format("The specified id '%s' does not represent an output port in the root group.", id));
        }

        final DataTransferAuthorizable baseAuthorizable = new DataTransferAuthorizable(outputPort);
        return new RootGroupPortAuthorizable() {
            @Override
            public Authorizable getAuthorizable() {
                return baseAuthorizable;
            }

            @Override
            public AuthorizationResult checkAuthorization(NiFiUser user) {
                // perform the authorization of the user by using the underlying component, ensures consistent authorization with raw s2s
                final PortAuthorizationResult authorizationResult = ((RootGroupPort) outputPort).checkUserAuthorization(user);
                if (authorizationResult.isAuthorized()) {
                    return AuthorizationResult.approved();
                } else {
                    return AuthorizationResult.denied(authorizationResult.getExplanation());
                }
            }
        };
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
        return new StandardConnectionAuthorizable(connection);
    }

    @Override
    public ProcessGroupAuthorizable getProcessGroup(final String id) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(id);
        return new StandardProcessGroupAuthorizable(processGroup);
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
    public ConfigurableComponentAuthorizable getControllerService(final String id) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(id);
        return new ControllerServiceConfigurableComponentAuthorizable(controllerService);
    }

    @Override
    public ConfigurableComponentAuthorizable getControllerServiceByType(String type) {
        try {
            final ControllerServiceNode controllerService = controllerFacade.createTemporaryControllerService(type);
            return new ControllerServiceConfigurableComponentAuthorizable(controllerService);
        } catch (final Exception e) {
            throw new AccessDeniedException("Unable to create controller service to verify if it references any Controller Services.");
        }
    }

    @Override
    public Authorizable getProvenance() {
        return PROVENANCE_AUTHORIZABLE;
    }

    @Override
    public Authorizable getCounters() {
        return COUNTERS_AUTHORIZABLE;
    }

    private ConfiguredComponent findControllerServiceReferencingComponent(final ControllerServiceReference referencingComponents, final String id) {
        ConfiguredComponent reference = null;
        for (final ConfiguredComponent component : referencingComponents.getReferencingComponents()) {
            if (component.getIdentifier().equals(id)) {
                reference = component;
                break;
            }

            if (component instanceof ControllerServiceNode) {
                final ControllerServiceNode refControllerService = (ControllerServiceNode) component;
                reference = findControllerServiceReferencingComponent(refControllerService.getReferences(), id);
                if (reference != null) {
                    break;
                }
            }
        }

        return reference;
    }

    @Override
    public Authorizable getControllerServiceReferencingComponent(String controllerServiceId, String id) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
        final ControllerServiceReference referencingComponents = controllerService.getReferences();
        final ConfiguredComponent reference = findControllerServiceReferencingComponent(referencingComponents, id);

        if (reference == null) {
            throw new ResourceNotFoundException("Unable to find referencing component with id " + id);
        }

        return reference;
    }

    @Override
    public ConfigurableComponentAuthorizable getReportingTask(final String id) {
        final ReportingTaskNode reportingTaskNode = reportingTaskDAO.getReportingTask(id);
        return new ReportingTaskConfigurableComponentAuthorizable(reportingTaskNode);
    }

    @Override
    public ConfigurableComponentAuthorizable getReportingTaskByType(String type) {
        try {
            final ReportingTaskNode reportingTask = controllerFacade.createTemporaryReportingTask(type);
            return new ReportingTaskConfigurableComponentAuthorizable(reportingTask);
        } catch (final Exception e) {
            throw new AccessDeniedException("Unable to create reporting to verify if it references any Controller Services.");
        }
    }

    @Override
    public SnippetAuthorizable getSnippet(final String id) {
        final Snippet snippet = snippetDAO.getSnippet(id);
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(snippet.getParentGroupId());

        return new SnippetAuthorizable() {
            @Override
            public Set<ConfigurableComponentAuthorizable> getSelectedProcessors() {
                return processGroup.getProcessors().stream()
                        .filter(processor -> snippet.getProcessors().containsKey(processor.getIdentifier()))
                        .map(processor -> getProcessor(processor.getIdentifier()))
                        .collect(Collectors.toSet());
            }

            @Override
            public Set<ConnectionAuthorizable> getSelectedConnections() {
                return processGroup.getConnections().stream()
                        .filter(connection -> snippet.getConnections().containsKey(connection.getIdentifier()))
                        .map(connection -> getConnection(connection.getIdentifier()))
                        .collect(Collectors.toSet());
            }

            @Override
            public Set<Authorizable> getSelectedInputPorts() {
                return processGroup.getInputPorts().stream()
                        .filter(inputPort -> snippet.getInputPorts().containsKey(inputPort.getIdentifier()))
                        .map(inputPort -> getInputPort(inputPort.getIdentifier()))
                        .collect(Collectors.toSet());
            }

            @Override
            public Set<Authorizable> getSelectedOutputPorts() {
                return processGroup.getOutputPorts().stream()
                        .filter(outputPort -> snippet.getOutputPorts().containsKey(outputPort.getIdentifier()))
                        .map(outputPort -> getOutputPort(outputPort.getIdentifier()))
                        .collect(Collectors.toSet());
            }

            @Override
            public Set<Authorizable> getSelectedFunnels() {
                return processGroup.getFunnels().stream()
                        .filter(funnel -> snippet.getFunnels().containsKey(funnel.getIdentifier()))
                        .map(funnel -> getFunnel(funnel.getIdentifier()))
                        .collect(Collectors.toSet());
            }

            @Override
            public Set<Authorizable> getSelectedLabels() {
                return processGroup.getLabels().stream()
                        .filter(label -> snippet.getLabels().containsKey(label.getIdentifier()))
                        .map(label -> getLabel(label.getIdentifier()))
                        .collect(Collectors.toSet());
            }

            @Override
            public Set<ProcessGroupAuthorizable> getSelectedProcessGroups() {
                return processGroup.getProcessGroups().stream()
                        .filter(processGroup -> snippet.getProcessGroups().containsKey(processGroup.getIdentifier()))
                        .map(processGroup -> getProcessGroup(processGroup.getIdentifier()))
                        .collect(Collectors.toSet());
            }

            @Override
            public Set<Authorizable> getSelectedRemoteProcessGroups() {
                return processGroup.getRemoteProcessGroups().stream()
                        .filter(remoteProcessGroup -> snippet.getRemoteProcessGroups().containsKey(remoteProcessGroup.getIdentifier()))
                        .map(remoteProcessGroup -> getRemoteProcessGroup(remoteProcessGroup.getIdentifier()))
                        .collect(Collectors.toSet());
            }
        };
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
                authorizable = getControllerService(componentId).getAuthorizable();
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
                authorizable = getProcessor(componentId).getAuthorizable();
                break;
            case ProcessGroup:
                authorizable = getProcessGroup(componentId).getAuthorizable();
                break;
            case RemoteProcessGroup:
                authorizable = getRemoteProcessGroup(componentId);
                break;
            case ReportingTask:
                authorizable = getReportingTask(componentId).getAuthorizable();
                break;
            case Template:
                authorizable = getTemplate(componentId).getAuthorizable();
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
                authorizable = getCounters();
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
                authorizable = getProvenance();
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
                authorizable = getPolicies();
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
                authorizable = getSystem();
                break;
            case Tenant:
                authorizable = getTenant();
                break;
            case RestrictedComponents:
                authorizable = getRestrictedComponents();
                break;
        }

        if (authorizable == null) {
            throw new IllegalArgumentException("An unexpected type of resource in this policy " + resourceType.getValue());
        }

        return authorizable;
    }

    /**
     * Creates temporary instances of all processors and controller services found in the specified snippet.
     *
     * @param snippet               snippet
     * @param processors            processors
     * @param controllerServices    controller services
     */
    private void createTemporaryProcessorsAndControllerServices(final FlowSnippetDTO snippet,
                                                                final Set<ConfigurableComponentAuthorizable> processors,
                                                                final Set<ConfigurableComponentAuthorizable> controllerServices) {

        if (snippet == null) {
            return;
        }

        if (snippet.getProcessors() != null) {
            processors.addAll(snippet.getProcessors().stream().map(processor -> getProcessorByType(processor.getType())).collect(Collectors.toSet()));
        }

        if (snippet.getControllerServices() != null) {
            controllerServices.addAll(snippet.getControllerServices().stream().map(controllerService -> getControllerServiceByType(controllerService.getType())).collect(Collectors.toSet()));
        }

        if (snippet.getProcessGroups() != null) {
            snippet.getProcessGroups().stream().forEach(group -> createTemporaryProcessorsAndControllerServices(group.getContents(), processors, controllerServices));
        }
    }

    @Override
    public TemplateAuthorizable getTemplate(final String id) {
        final Template template = templateDAO.getTemplate(id);
        final TemplateDTO contents = template.getDetails();

        // templates are immutable so we can pre-compute all encapsulated processors and controller services
        final Set<ConfigurableComponentAuthorizable> processors = new HashSet<>();
        final Set<ConfigurableComponentAuthorizable> controllerServices = new HashSet<>();

        // find all processors and controller services
        createTemporaryProcessorsAndControllerServices(contents.getSnippet(), processors, controllerServices);

        return new TemplateAuthorizable() {
            @Override
            public Authorizable getAuthorizable() {
                return template;
            }

            @Override
            public Set<ConfigurableComponentAuthorizable> getEncapsulatedProcessors() {
                return processors;
            }

            @Override
            public Set<ConfigurableComponentAuthorizable> getEncapsulatedControllerServices() {
                return controllerServices;
            }
        };
    }

    @Override
    public Authorizable getConnectable(String id) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(controllerFacade.getRootGroupId());
        return group.findConnectable(id);
    }

    @Override
    public Authorizable getRestrictedComponents() {
        return RESTRICTED_COMPONENTS_AUTHORIZABLE;
    }

    @Override
    public Authorizable getSystem() {
        return SYSTEM_AUTHORIZABLE;
    }

    /**
     * ConfigurableComponentAuthorizable for a ProcessorNode.
     */
    private static class ProcessorConfigurableComponentAuthorizable implements ConfigurableComponentAuthorizable {
        private final ProcessorNode processorNode;

        public ProcessorConfigurableComponentAuthorizable(ProcessorNode processorNode) {
            this.processorNode = processorNode;
        }

        @Override
        public Authorizable getAuthorizable() {
            return processorNode;
        }

        @Override
        public boolean isRestricted() {
            return processorNode.isRestricted();
        }

        @Override
        public String getValue(PropertyDescriptor propertyDescriptor) {
            return processorNode.getProperty(propertyDescriptor);
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String propertyName) {
            return processorNode.getPropertyDescriptor(propertyName);
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return processorNode.getPropertyDescriptors();
        }
    }

    /**
     * ConfigurableComponentAuthorizable for a ControllerServiceNode.
     */
    private static class ControllerServiceConfigurableComponentAuthorizable implements ConfigurableComponentAuthorizable {
        private final ControllerServiceNode controllerServiceNode;

        public ControllerServiceConfigurableComponentAuthorizable(ControllerServiceNode controllerServiceNode) {
            this.controllerServiceNode = controllerServiceNode;
        }

        @Override
        public Authorizable getAuthorizable() {
            return controllerServiceNode;
        }

        @Override
        public boolean isRestricted() {
            return controllerServiceNode.isRestricted();
        }

        @Override
        public String getValue(PropertyDescriptor propertyDescriptor) {
            return controllerServiceNode.getProperty(propertyDescriptor);
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String propertyName) {
            return controllerServiceNode.getControllerServiceImplementation().getPropertyDescriptor(propertyName);
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return controllerServiceNode.getControllerServiceImplementation().getPropertyDescriptors();
        }
    }

    /**
     * ConfigurableComponentAuthorizable for a ProcessorNode.
     */
    private static class ReportingTaskConfigurableComponentAuthorizable implements ConfigurableComponentAuthorizable {
        private final ReportingTaskNode reportingTaskNode;

        public ReportingTaskConfigurableComponentAuthorizable(ReportingTaskNode reportingTaskNode) {
            this.reportingTaskNode = reportingTaskNode;
        }

        @Override
        public Authorizable getAuthorizable() {
            return reportingTaskNode;
        }

        @Override
        public boolean isRestricted() {
            return reportingTaskNode.isRestricted();
        }

        @Override
        public String getValue(PropertyDescriptor propertyDescriptor) {
            return reportingTaskNode.getProperty(propertyDescriptor);
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String propertyName) {
            return reportingTaskNode.getReportingTask().getPropertyDescriptor(propertyName);
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return reportingTaskNode.getReportingTask().getPropertyDescriptors();
        }
    }

    private static class StandardProcessGroupAuthorizable implements ProcessGroupAuthorizable {
        private final ProcessGroup processGroup;

        public StandardProcessGroupAuthorizable(ProcessGroup processGroup) {
            this.processGroup = processGroup;
        }

        @Override
        public Authorizable getAuthorizable() {
            return processGroup;
        }

        @Override
        public Set<ConfigurableComponentAuthorizable> getEncapsulatedProcessors() {
            return processGroup.findAllProcessors().stream().map(
                    processorNode -> new ProcessorConfigurableComponentAuthorizable(processorNode)).collect(Collectors.toSet());
        }

        @Override
        public Set<ConnectionAuthorizable> getEncapsulatedConnections() {
            return processGroup.findAllConnections().stream().map(
                    connection -> new StandardConnectionAuthorizable(connection)).collect(Collectors.toSet());
        }

        @Override
        public Set<Authorizable> getEncapsulatedInputPorts() {
            return processGroup.findAllInputPorts().stream().collect(Collectors.toSet());
        }

        @Override
        public Set<Authorizable> getEncapsulatedOutputPorts() {
            return processGroup.findAllOutputPorts().stream().collect(Collectors.toSet());
        }

        @Override
        public Set<Authorizable> getEncapsulatedFunnels() {
            return processGroup.findAllFunnels().stream().collect(Collectors.toSet());
        }

        @Override
        public Set<Authorizable> getEncapsulatedLabels() {
            return processGroup.findAllLabels().stream().collect(Collectors.toSet());
        }

        @Override
        public Set<ProcessGroupAuthorizable> getEncapsulatedProcessGroups() {
            return processGroup.findAllProcessGroups().stream().map(
                    group -> new StandardProcessGroupAuthorizable(group)).collect(Collectors.toSet());
        }

        @Override
        public Set<Authorizable> getEncapsulatedRemoteProcessGroups() {
            return processGroup.findAllRemoteProcessGroups().stream().collect(Collectors.toSet());
        }

        @Override
        public Set<Authorizable> getEncapsulatedTemplates() {
            return processGroup.findAllTemplates().stream().collect(Collectors.toSet());
        }

        @Override
        public Set<ConfigurableComponentAuthorizable> getEncapsulatedControllerServices() {
            return processGroup.findAllControllerServices().stream().map(
                    controllerServiceNode -> new ControllerServiceConfigurableComponentAuthorizable(controllerServiceNode)).collect(Collectors.toSet());
        }
    }

    private static class StandardConnectionAuthorizable implements ConnectionAuthorizable {
        private final Connection connection;

        public StandardConnectionAuthorizable(Connection connection) {
            this.connection = connection;
        }

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
