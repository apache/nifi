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
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.authorization.resource.AccessPolicyAuthorizable;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.DataAuthorizable;
import org.apache.nifi.authorization.resource.DataTransferAuthorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.resource.ProvenanceDataAuthorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.resource.RestrictedComponentsAuthorizableFactory;
import org.apache.nifi.authorization.resource.TenantAuthorizable;
import org.apache.nifi.authorization.resource.VersionedComponentAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.remote.PortAuthorizationResult;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.apache.nifi.web.dao.ControllerServiceDAO;
import org.apache.nifi.web.dao.FlowAnalysisRuleDAO;
import org.apache.nifi.web.dao.FlowRegistryDAO;
import org.apache.nifi.web.dao.FunnelDAO;
import org.apache.nifi.web.dao.LabelDAO;
import org.apache.nifi.web.dao.ParameterContextDAO;
import org.apache.nifi.web.dao.ParameterProviderDAO;
import org.apache.nifi.web.dao.PortDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.apache.nifi.web.dao.ReportingTaskDAO;
import org.apache.nifi.web.dao.SnippetDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Component
public class StandardAuthorizableLookup implements AuthorizableLookup {

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

    private static final Authorizable RESOURCE_AUTHORIZABLE = new Authorizable() {
        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getResourceResource();
        }
    };

    private static final Authorizable SITE_TO_SITE_AUTHORIZABLE = new Authorizable() {
        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getSiteToSiteResource();
        }
    };

    private static final Authorizable FLOW_AUTHORIZABLE = new Authorizable() {
        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getFlowResource();
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
    private FlowAnalysisRuleDAO flowAnalysisRuleDAO;
    private ParameterProviderDAO parameterProviderDAO;
    private FlowRegistryDAO flowRegistryDAO;
    private AccessPolicyDAO accessPolicyDAO;
    private ParameterContextDAO parameterContextDAO;

    @Override
    public Authorizable getController() {
        return controllerFacade;
    }

    @Override
    public ComponentAuthorizable getConfigurableComponent(final String type, final BundleDTO bundle) {
        final ConfigurableComponent configurableComponent = controllerFacade.getTemporaryComponent(type, bundle);
        return getConfigurableComponent(configurableComponent);
    }

    @Override
    public ComponentAuthorizable getConfigurableComponent(ConfigurableComponent configurableComponent) {
        try {
            return new ConfigurableComponentAuthorizable(configurableComponent, controllerFacade.getExtensionManager());
        } catch (final Exception e) {
            throw new AccessDeniedException("Unable to create component to verify if it references any Controller Services.");
        }
    }

    @Override
    public ComponentAuthorizable getProcessor(final String id) {
        final ProcessorNode processorNode = processorDAO.getProcessor(id);
        return new ProcessorComponentAuthorizable(processorNode, controllerFacade.getExtensionManager());
    }

    @Override
    public PublicPortAuthorizable getPublicInputPort(String id) {
        final Port inputPort = inputPortDAO.getPort(id);

        if (!(inputPort instanceof PublicPort)) {
            throw new IllegalArgumentException(String.format("The specified id '%s' does not represent an input port which can be accessed remotely.", id));
        }

        final DataTransferAuthorizable baseAuthorizable = new DataTransferAuthorizable(inputPort);
        return new PublicPortAuthorizable() {
            @Override
            public Authorizable getAuthorizable() {
                return baseAuthorizable;
            }

            @Override
            public AuthorizationResult checkAuthorization(NiFiUser user) {
                // perform the authorization of the user by using the underlying component, ensures consistent authorization with raw s2s
                final PortAuthorizationResult authorizationResult = ((PublicPort) inputPort).checkUserAuthorization(user);
                if (authorizationResult.isAuthorized()) {
                    return AuthorizationResult.approved();
                } else {
                    return AuthorizationResult.denied(authorizationResult.getExplanation());
                }
            }
        };
    }

    @Override
    public PublicPortAuthorizable getPublicOutputPort(String id) {
        final Port outputPort = outputPortDAO.getPort(id);

        if (!(outputPort instanceof PublicPort)) {
            throw new IllegalArgumentException(String.format("The specified id '%s' does not represent an output port which can be accessed remotely.", id));
        }

        final DataTransferAuthorizable baseAuthorizable = new DataTransferAuthorizable(outputPort);
        return new PublicPortAuthorizable() {
            @Override
            public Authorizable getAuthorizable() {
                return baseAuthorizable;
            }

            @Override
            public AuthorizationResult checkAuthorization(NiFiUser user) {
                // perform the authorization of the user by using the underlying component, ensures consistent authorization with raw s2s
                final PortAuthorizationResult authorizationResult = ((PublicPort) outputPort).checkUserAuthorization(user);
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
    public ParameterContext getParameterContext(final String id) {
        return parameterContextDAO.getParameterContext(id);
    }

    @Override
    public ConnectionAuthorizable getConnection(final String id) {
        final Connection connection = connectionDAO.getConnection(id);
        return new StandardConnectionAuthorizable(connection);
    }

    @Override
    public ProcessGroupAuthorizable getRootProcessGroup() {
        return getProcessGroup(controllerFacade.getRootGroupId());
    }

    @Override
    public ProcessGroupAuthorizable getProcessGroup(final String id) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(id);
        return new StandardProcessGroupAuthorizable(processGroup, controllerFacade.getExtensionManager());
    }

    @Override
    public Authorizable getRemoteProcessGroup(final String id) {
        return remoteProcessGroupDAO.getRemoteProcessGroup(id);
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
    public Set<ComponentAuthorizable> getControllerServices(final String groupId, final Predicate<VersionedComponentAuthorizable> filter) {
        return controllerServiceDAO.getControllerServices(groupId, true, false).stream()
                .filter(cs -> filter.test(new VersionedComponentAuthorizable() {
                        @Override
                        public Optional<String> getVersionedComponentId() {
                            return cs.getVersionedComponentId();
                        }

                        @Override
                        public String getIdentifier() {
                            return cs.getIdentifier();
                        }

                        @Override
                        public String getProcessGroupIdentifier() {
                            return cs.getProcessGroupIdentifier();
                        }

                        @Override
                        public Authorizable getParentAuthorizable() {
                            return cs.getParentAuthorizable();
                        }

                        @Override
                        public Resource getResource() {
                            return cs.getResource();
                        }
                    })
                )
                .map(controllerServiceNode -> new ControllerServiceComponentAuthorizable(controllerServiceNode, controllerFacade.getExtensionManager())).collect(Collectors.toSet());
    }

    @Override
    public ComponentAuthorizable getControllerService(final String id) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(id);
        return new ControllerServiceComponentAuthorizable(controllerService, controllerFacade.getExtensionManager());
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
    public Authorizable getResource() {
        return RESOURCE_AUTHORIZABLE;
    }

    @Override
    public Authorizable getSiteToSite() {
        return SITE_TO_SITE_AUTHORIZABLE;
    }

    @Override
    public Authorizable getFlow() {
        return FLOW_AUTHORIZABLE;
    }

    private ComponentNode findControllerServiceReferencingComponent(final ControllerServiceReference referencingComponents, final String id) {
        ComponentNode reference = null;
        for (final ComponentNode component : referencingComponents.getReferencingComponents()) {
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
        final ComponentNode reference = findControllerServiceReferencingComponent(referencingComponents, id);

        if (reference == null) {
            throw new ResourceNotFoundException("Unable to find referencing component with id " + id);
        }

        return reference;
    }

    @Override
    public ComponentAuthorizable getReportingTask(final String id) {
        final ReportingTaskNode reportingTaskNode = reportingTaskDAO.getReportingTask(id);
        return new ReportingTaskComponentAuthorizable(reportingTaskNode, controllerFacade.getExtensionManager());
    }

    @Override
    public ComponentAuthorizable getFlowAnalysisRule(final String id) {
        final FlowAnalysisRuleNode flowAnalysisRuleNode = flowAnalysisRuleDAO.getFlowAnalysisRule(id);
        return new FlowAnalysisRuleComponentAuthorizable(flowAnalysisRuleNode, controllerFacade.getExtensionManager());
    }

    @Override
    public Set<ComponentAuthorizable> getParameterProviders(final Predicate<org.apache.nifi.authorization.resource.ComponentAuthorizable> filter) {
        return parameterProviderDAO.getParameterProviders().stream()
                .filter(filter)
                .map(parameterProviderNode -> new ParameterProviderComponentAuthorizable(parameterProviderNode, controllerFacade.getExtensionManager())).collect(Collectors.toSet());
    }

    @Override
    public ComponentAuthorizable getParameterProvider(final String id) {
        final ParameterProviderNode parameterProviderNode = parameterProviderDAO.getParameterProvider(id);
        return new ParameterProviderComponentAuthorizable(parameterProviderNode, controllerFacade.getExtensionManager());
    }

    @Override
    public ComponentAuthorizable getFlowRegistryClient(final String id) {
        final FlowRegistryClientNode flowRegistryClientNode = flowRegistryDAO.getFlowRegistryClient(id);
        return new FlowRegistryClientComponentAuthorizable(flowRegistryClientNode, controllerFacade.getExtensionManager());
    }

    @Override
    public SnippetAuthorizable getSnippet(final String id) {
        final Snippet snippet = snippetDAO.getSnippet(id);
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(snippet.getParentGroupId());

        return new SnippetAuthorizable() {
            @Override
            public ProcessGroupAuthorizable getParentProcessGroup() {
                return new StandardProcessGroupAuthorizable(processGroup, controllerFacade.getExtensionManager());
            }

            @Override
            public Set<ComponentAuthorizable> getSelectedProcessors() {
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
    public Authorizable getAuthorizableFromResource(final String resource) {
        // parse the resource type
        final ResourceType resourceType = ResourceType.fromRawValue(resource);
        if (resourceType == null) {
            throw new ResourceNotFoundException("Unrecognized resource: " + resource);
        }

        // if this is a policy, data or a provenance event resource, there should be another resource type
        switch (resourceType) {
            case Policy:
            case Data:
            case DataTransfer:
            case ProvenanceData:
            case Operation:

                // get the resource type
                final String baseResource = StringUtils.substringAfter(resource, resourceType.getValue());
                final ResourceType baseResourceType = ResourceType.fromRawValue(baseResource);

                if (baseResourceType == null) {
                    throw new ResourceNotFoundException("Unrecognized base resource: " + resource);
                }

                switch (resourceType) {
                    case Policy:
                        return new AccessPolicyAuthorizable(getAccessPolicy(baseResourceType, resource));
                    case Data:
                        return new DataAuthorizable(getAccessPolicy(baseResourceType, resource));
                    case DataTransfer:
                        return new DataTransferAuthorizable(getAccessPolicy(baseResourceType, resource));
                    case ProvenanceData:
                        return new ProvenanceDataAuthorizable(getAccessPolicy(baseResourceType, resource));
                    case Operation:
                        return new OperationAuthorizable(getAccessPolicy(baseResourceType, resource));
                }

            case RestrictedComponents:
                final String slashRequiredPermission = StringUtils.substringAfter(resource, resourceType.getValue());

                if (slashRequiredPermission.startsWith("/")) {
                    final RequiredPermission requiredPermission = RequiredPermission.valueOfPermissionIdentifier(slashRequiredPermission.substring(1));

                    if (requiredPermission == null) {
                        throw new ResourceNotFoundException("Unrecognized resource: " + resource);
                    }

                    return getRestrictedComponents(requiredPermission);
                } else {
                    return getRestrictedComponents();
                }

            default:
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
            case RegistryClient:
                authorizable = getFlowRegistryClient(componentId).getAuthorizable();
                break;
            case RemoteProcessGroup:
                authorizable = getRemoteProcessGroup(componentId);
                break;
            case ReportingTask:
                authorizable = getReportingTask(componentId).getAuthorizable();
                break;
            case FlowAnalysisRule:
                authorizable = getFlowAnalysisRule(componentId).getAuthorizable();
                break;
            case ParameterContext:
                authorizable = getParameterContext(componentId);
                break;
            case ParameterProvider:
                authorizable = getParameterProvider(componentId).getAuthorizable();
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
            case ParameterContext:
                authorizable = getParameterContexts();
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
                                                                final Set<ComponentAuthorizable> processors,
                                                                final Set<ComponentAuthorizable> controllerServices,
                                                                final ExtensionManager extensionManager) {

        if (snippet == null) {
            return;
        }

        if (snippet.getProcessors() != null) {
            snippet.getProcessors().forEach(processor -> {
                try {
                    final BundleCoordinate bundle = BundleUtils.getCompatibleBundle(extensionManager, processor.getType(), processor.getBundle());
                    processors.add(getConfigurableComponent(processor.getType(), new BundleDTO(bundle.getGroup(), bundle.getId(), bundle.getVersion())));
                } catch (final IllegalStateException ignored) {
                    // no compatible bundles... no additional auth checks necessary... if created, will be ghosted
                }
            });
        }

        if (snippet.getControllerServices() != null) {
            snippet.getControllerServices().forEach(controllerService -> {
                try {
                    final BundleCoordinate bundle = BundleUtils.getCompatibleBundle(extensionManager, controllerService.getType(), controllerService.getBundle());
                    controllerServices.add(getConfigurableComponent(controllerService.getType(), new BundleDTO(bundle.getGroup(), bundle.getId(), bundle.getVersion())));
                } catch (final IllegalStateException ignored) {
                    // no compatible bundles... no additional auth checks necessary... if created, will be ghosted
                }
            });
        }

        if (snippet.getProcessGroups() != null) {
            snippet.getProcessGroups().forEach(group -> createTemporaryProcessorsAndControllerServices(group.getContents(), processors, controllerServices, extensionManager));
        }
    }

    @Override
    public Authorizable getLocalConnectable(String id) {
        final Connectable connectable = controllerFacade.findLocalConnectable(id);
        if (connectable == null) {
            throw new ResourceNotFoundException("Unable to find component with id " + id);
        }

        return connectable;
    }

    @Override
    public Authorizable getRestrictedComponents() {
        return RestrictedComponentsAuthorizableFactory.getRestrictedComponentsAuthorizable();
    }

    @Override
    public Authorizable getRestrictedComponents(final RequiredPermission requiredPermission) {
        return RestrictedComponentsAuthorizableFactory.getRestrictedComponentsAuthorizable(requiredPermission);
    }

    @Override
    public Authorizable getSystem() {
        return SYSTEM_AUTHORIZABLE;
    }

    @Override
    public Authorizable getParameterContexts() {
        return new Authorizable() {
            @Override
            public Authorizable getParentAuthorizable() {
                return getController();
            }

            @Override
            public Resource getResource() {
                return ResourceFactory.getParameterContextsResource();
            }
        };
    }

    /**
     * ComponentAuthorizable for a ConfigurableComponent. This authorizable is intended only to be used when
     * creating new components.
     */
    private static class ConfigurableComponentAuthorizable implements ComponentAuthorizable {
        private final ConfigurableComponent configurableComponent;
        private final ExtensionManager extensionManager;

        public ConfigurableComponentAuthorizable(final ConfigurableComponent configurableComponent, final ExtensionManager extensionManager) {
            this.configurableComponent = configurableComponent;
            this.extensionManager = extensionManager;
        }

        @Override
        public Authorizable getAuthorizable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isRestricted() {
            return configurableComponent.getClass().isAnnotationPresent(Restricted.class);
        }

        @Override
        public Set<Authorizable> getRestrictedAuthorizables() {
            return RestrictedComponentsAuthorizableFactory.getRestrictedComponentsAuthorizable(configurableComponent.getClass());
        }

        @Override
        public String getValue(PropertyDescriptor propertyDescriptor) {
            return null;
        }

        @Override
        public String getRawValue(final PropertyDescriptor propertyDescriptor) {
            return null;
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String propertyName) {
            return configurableComponent.getPropertyDescriptor(propertyName);
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return configurableComponent.getPropertyDescriptors();
        }

        @Override
        public void cleanUpResources() {
            extensionManager.removeInstanceClassLoader(configurableComponent.getIdentifier());
        }

        @Override
        public ParameterContext getParameterContext() {
            return null;
        }
    }

    /**
     * ComponentAuthorizable for a ProcessorNode.
     */
    private static class ProcessorComponentAuthorizable implements ComponentAuthorizable {
        private final ProcessorNode processorNode;
        private final ExtensionManager extensionManager;

        public ProcessorComponentAuthorizable(final ProcessorNode processorNode, final ExtensionManager extensionManager) {
            this.processorNode = processorNode;
            this.extensionManager = extensionManager;
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
        public Set<Authorizable> getRestrictedAuthorizables() {
            return RestrictedComponentsAuthorizableFactory.getRestrictedComponentsAuthorizable(processorNode.getComponentClass());
        }

        @Override
        public ParameterContext getParameterContext() {
            return processorNode.getProcessGroup().getParameterContext();
        }

        @Override
        public String getValue(PropertyDescriptor propertyDescriptor) {
            return processorNode.getEffectivePropertyValue(propertyDescriptor);
        }

        @Override
        public String getRawValue(final PropertyDescriptor propertyDescriptor) {
            return processorNode.getRawPropertyValue(propertyDescriptor);
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String propertyName) {
            return processorNode.getPropertyDescriptor(propertyName);
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return processorNode.getPropertyDescriptors();
        }

        @Override
        public void cleanUpResources() {
            extensionManager.removeInstanceClassLoader(processorNode.getIdentifier());
        }
    }

    /**
     * ComponentAuthorizable for a ControllerServiceNode.
     */
    private static class ControllerServiceComponentAuthorizable implements ComponentAuthorizable {
        private final ControllerServiceNode controllerServiceNode;
        private final ExtensionManager extensionManager;

        public ControllerServiceComponentAuthorizable(final ControllerServiceNode controllerServiceNode, final ExtensionManager extensionManager) {
            this.controllerServiceNode = controllerServiceNode;
            this.extensionManager = extensionManager;
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
        public Set<Authorizable> getRestrictedAuthorizables() {
            return RestrictedComponentsAuthorizableFactory.getRestrictedComponentsAuthorizable(controllerServiceNode.getComponentClass());
        }

        @Override
        public ParameterContext getParameterContext() {
            final ProcessGroup processGroup = controllerServiceNode.getProcessGroup();
            return processGroup == null ? null : processGroup.getParameterContext(); // will be null if Controller-level Controller Service.
        }

        @Override
        public String getValue(PropertyDescriptor propertyDescriptor) {
            return controllerServiceNode.getEffectivePropertyValue(propertyDescriptor);
        }

        @Override
        public String getRawValue(final PropertyDescriptor propertyDescriptor) {
            return controllerServiceNode.getRawPropertyValue(propertyDescriptor);
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String propertyName) {
            return controllerServiceNode.getControllerServiceImplementation().getPropertyDescriptor(propertyName);
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return controllerServiceNode.getControllerServiceImplementation().getPropertyDescriptors();
        }

        @Override
        public void cleanUpResources() {
            extensionManager.removeInstanceClassLoader(controllerServiceNode.getIdentifier());
        }
    }

    /**
     * ComponentAuthorizable for a ReportingTask.
     */
    private static class ReportingTaskComponentAuthorizable implements ComponentAuthorizable {
        private final ReportingTaskNode reportingTaskNode;
        private final ExtensionManager extensionManager;

        public ReportingTaskComponentAuthorizable(final ReportingTaskNode reportingTaskNode, final ExtensionManager extensionManager) {
            this.reportingTaskNode = reportingTaskNode;
            this.extensionManager = extensionManager;
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
        public Set<Authorizable> getRestrictedAuthorizables() {
            return RestrictedComponentsAuthorizableFactory.getRestrictedComponentsAuthorizable(reportingTaskNode.getComponentClass());
        }

        @Override
        public ParameterContext getParameterContext() {
            return null;
        }

        @Override
        public String getValue(PropertyDescriptor propertyDescriptor) {
            return reportingTaskNode.getEffectivePropertyValue(propertyDescriptor);
        }

        @Override
        public String getRawValue(final PropertyDescriptor propertyDescriptor) {
            return reportingTaskNode.getRawPropertyValue(propertyDescriptor);
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String propertyName) {
            return reportingTaskNode.getReportingTask().getPropertyDescriptor(propertyName);
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return reportingTaskNode.getReportingTask().getPropertyDescriptors();
        }

        @Override
        public void cleanUpResources() {
            extensionManager.removeInstanceClassLoader(reportingTaskNode.getIdentifier());
        }
    }

    /**
     * ComponentAuthorizable for a FlowAnalysisRuleNode
     */
    private static class FlowAnalysisRuleComponentAuthorizable implements ComponentAuthorizable {
        private final FlowAnalysisRuleNode flowAnalysisRuleNode;
        private final ExtensionManager extensionManager;

        public FlowAnalysisRuleComponentAuthorizable(final FlowAnalysisRuleNode flowAnalysisRuleNode, final ExtensionManager extensionManager) {
            this.flowAnalysisRuleNode = flowAnalysisRuleNode;
            this.extensionManager = extensionManager;
        }

        @Override
        public Authorizable getAuthorizable() {
            return flowAnalysisRuleNode;
        }

        @Override
        public boolean isRestricted() {
            return flowAnalysisRuleNode.isRestricted();
        }

        @Override
        public Set<Authorizable> getRestrictedAuthorizables() {
            return RestrictedComponentsAuthorizableFactory.getRestrictedComponentsAuthorizable(flowAnalysisRuleNode.getComponentClass());
        }

        @Override
        public ParameterContext getParameterContext() {
            return null;
        }

        @Override
        public String getValue(PropertyDescriptor propertyDescriptor) {
            return flowAnalysisRuleNode.getEffectivePropertyValue(propertyDescriptor);
        }

        @Override
        public String getRawValue(final PropertyDescriptor propertyDescriptor) {
            return flowAnalysisRuleNode.getRawPropertyValue(propertyDescriptor);
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String propertyName) {
            return flowAnalysisRuleNode.getFlowAnalysisRule().getPropertyDescriptor(propertyName);
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return flowAnalysisRuleNode.getFlowAnalysisRule().getPropertyDescriptors();
        }

        @Override
        public void cleanUpResources() {
            extensionManager.removeInstanceClassLoader(flowAnalysisRuleNode.getIdentifier());
        }
    }

    /**
     * ComponentAuthorizable for a ParameterProvider.
     */
    private static class ParameterProviderComponentAuthorizable implements ComponentAuthorizable {
        private final ParameterProviderNode parameterProviderNode;
        private final ExtensionManager extensionManager;

        public ParameterProviderComponentAuthorizable(final ParameterProviderNode parameterProviderNode, final ExtensionManager extensionManager) {
            this.parameterProviderNode = parameterProviderNode;
            this.extensionManager = extensionManager;
        }

        @Override
        public Authorizable getAuthorizable() {
            return parameterProviderNode;
        }

        @Override
        public boolean isRestricted() {
            return parameterProviderNode.isRestricted();
        }

        @Override
        public Set<Authorizable> getRestrictedAuthorizables() {
            return RestrictedComponentsAuthorizableFactory.getRestrictedComponentsAuthorizable(parameterProviderNode.getComponentClass());
        }

        @Override
        public ParameterContext getParameterContext() {
            return null;
        }

        @Override
        public String getValue(final PropertyDescriptor propertyDescriptor) {
            return parameterProviderNode.getEffectivePropertyValue(propertyDescriptor);
        }

        @Override
        public String getRawValue(final PropertyDescriptor propertyDescriptor) {
            return parameterProviderNode.getRawPropertyValue(propertyDescriptor);
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String propertyName) {
            return parameterProviderNode.getParameterProvider().getPropertyDescriptor(propertyName);
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return parameterProviderNode.getParameterProvider().getPropertyDescriptors();
        }

        @Override
        public void cleanUpResources() {
            extensionManager.removeInstanceClassLoader(parameterProviderNode.getIdentifier());
        }
    }

    /**
     * ComponentAuthorizable for a FlowRegistryClientNode.
     */
    private static class FlowRegistryClientComponentAuthorizable implements ComponentAuthorizable {
        private final FlowRegistryClientNode flowRegistryClientNode;
        private final ExtensionManager extensionManager;

        public FlowRegistryClientComponentAuthorizable(final FlowRegistryClientNode flowRegistryClientNode, final ExtensionManager extensionManager) {
            this.flowRegistryClientNode = flowRegistryClientNode;
            this.extensionManager = extensionManager;
        }

        @Override
        public Authorizable getAuthorizable() {
            return flowRegistryClientNode;
        }

        @Override
        public boolean isRestricted() {
            return flowRegistryClientNode.isRestricted();
        }

        @Override
        public Set<Authorizable> getRestrictedAuthorizables() {
            return RestrictedComponentsAuthorizableFactory.getRestrictedComponentsAuthorizable(flowRegistryClientNode.getComponentClass());
        }

        @Override
        public ParameterContext getParameterContext() {
            return null;
        }

        @Override
        public String getValue(PropertyDescriptor propertyDescriptor) {
            return flowRegistryClientNode.getEffectivePropertyValue(propertyDescriptor);
        }

        @Override
        public String getRawValue(final PropertyDescriptor propertyDescriptor) {
            return flowRegistryClientNode.getRawPropertyValue(propertyDescriptor);
        }

        @Override
        public PropertyDescriptor getPropertyDescriptor(String propertyName) {
            return flowRegistryClientNode.getComponent().getPropertyDescriptor(propertyName);
        }

        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return flowRegistryClientNode.getComponent().getPropertyDescriptors();
        }

        @Override
        public void cleanUpResources() {
            extensionManager.removeInstanceClassLoader(flowRegistryClientNode.getIdentifier());
        }
    }

    private static class StandardProcessGroupAuthorizable implements ProcessGroupAuthorizable {
        private final ProcessGroup processGroup;
        private final ExtensionManager extensionManager;

        public StandardProcessGroupAuthorizable(final ProcessGroup processGroup, final ExtensionManager extensionManager) {
            this.processGroup = processGroup;
            this.extensionManager = extensionManager;
        }

        @Override
        public Authorizable getAuthorizable() {
            return processGroup;
        }

        @Override
        public Optional<Authorizable> getParameterContextAuthorizable() {
            return Optional.ofNullable(processGroup.getParameterContext());
        }

        @Override
        public ProcessGroup getProcessGroup() {
            return processGroup;
        }

        @Override
        public Set<ComponentAuthorizable> getEncapsulatedProcessors() {
            return processGroup.findAllProcessors().stream().map(
                    processorNode -> new ProcessorComponentAuthorizable(processorNode, extensionManager)).collect(Collectors.toSet());
        }

        @Override
        public Set<ComponentAuthorizable> getEncapsulatedProcessors(Predicate<org.apache.nifi.authorization.resource.ComponentAuthorizable> processorFilter) {
            return processGroup.findAllProcessors().stream()
                    .filter(processorFilter)
                    .map(processorNode -> new ProcessorComponentAuthorizable(processorNode, extensionManager)).collect(Collectors.toSet());
        }

        @Override
        public Set<ConnectionAuthorizable> getEncapsulatedConnections() {
            return processGroup.findAllConnections().stream().map(
                StandardConnectionAuthorizable::new).collect(Collectors.toSet());
        }

        @Override
        public Set<Authorizable> getEncapsulatedInputPorts() {
            return new HashSet<>(processGroup.findAllInputPorts());
        }

        @Override
        public Set<Authorizable> getEncapsulatedOutputPorts() {
            return new HashSet<>(processGroup.findAllOutputPorts());
        }

        @Override
        public Set<Authorizable> getEncapsulatedFunnels() {
            return new HashSet<>(processGroup.findAllFunnels());
        }

        @Override
        public Set<Authorizable> getEncapsulatedLabels() {
            return new HashSet<>(processGroup.findAllLabels());
        }

        @Override
        public Set<ProcessGroupAuthorizable> getEncapsulatedProcessGroups() {
            return processGroup.findAllProcessGroups().stream().map(
                    group -> new StandardProcessGroupAuthorizable(group, extensionManager)).collect(Collectors.toSet());
        }

        @Override
        public Set<Authorizable> getEncapsulatedRemoteProcessGroups() {
            return new HashSet<>(processGroup.findAllRemoteProcessGroups());
        }

        @Override
        public Set<ComponentAuthorizable> getEncapsulatedControllerServices() {
            return processGroup.findAllControllerServices().stream().map(
                    controllerServiceNode -> new ControllerServiceComponentAuthorizable(controllerServiceNode, extensionManager)).collect(Collectors.toSet());
        }

        @Override
        public Set<ComponentAuthorizable> getEncapsulatedControllerServices(Predicate<org.apache.nifi.authorization.resource.ComponentAuthorizable> serviceFilter) {
            return processGroup.findAllControllerServices().stream()
                    .filter(serviceFilter)
                    .map(controllerServiceNode -> new ControllerServiceComponentAuthorizable(controllerServiceNode, extensionManager)).collect(Collectors.toSet());
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
        public Authorizable getSourceData() {
            return new DataAuthorizable(connection.getSourceAuthorizable());
        }

        @Override
        public Connectable getDestination() {
            return connection.getDestination();
        }

        @Override
        public Authorizable getDestinationData() {
            return new DataAuthorizable(connection.getDestinationAuthorizable());
        }

        @Override
        public ProcessGroup getParentGroup() {
            return connection.getProcessGroup();
        }
    }

    @Autowired
    public void setProcessorDAO(ProcessorDAO processorDAO) {
        this.processorDAO = processorDAO;
    }

    @Autowired
    public void setProcessGroupDAO(ProcessGroupDAO processGroupDAO) {
        this.processGroupDAO = processGroupDAO;
    }

    @Autowired
    public void setRemoteProcessGroupDAO(RemoteProcessGroupDAO remoteProcessGroupDAO) {
        this.remoteProcessGroupDAO = remoteProcessGroupDAO;
    }

    @Autowired
    public void setLabelDAO(LabelDAO labelDAO) {
        this.labelDAO = labelDAO;
    }

    @Autowired
    public void setFunnelDAO(FunnelDAO funnelDAO) {
        this.funnelDAO = funnelDAO;
    }

    @Autowired
    public void setSnippetDAO(SnippetDAO snippetDAO) {
        this.snippetDAO = snippetDAO;
    }

    @Qualifier("standardInputPortDAO")
    @Autowired
    public void setInputPortDAO(PortDAO inputPortDAO) {
        this.inputPortDAO = inputPortDAO;
    }

    @Qualifier("standardOutputPortDAO")
    @Autowired
    public void setOutputPortDAO(PortDAO outputPortDAO) {
        this.outputPortDAO = outputPortDAO;
    }

    @Autowired
    public void setConnectionDAO(ConnectionDAO connectionDAO) {
        this.connectionDAO = connectionDAO;
    }

    @Autowired
    public void setControllerServiceDAO(ControllerServiceDAO controllerServiceDAO) {
        this.controllerServiceDAO = controllerServiceDAO;
    }

    @Autowired
    public void setReportingTaskDAO(ReportingTaskDAO reportingTaskDAO) {
        this.reportingTaskDAO = reportingTaskDAO;
    }

    @Autowired
    public void setFlowAnalysisRuleDAO(FlowAnalysisRuleDAO flowAnalysisRuleDAO) {
        this.flowAnalysisRuleDAO = flowAnalysisRuleDAO;
    }

    @Autowired
    public void setParameterProviderDAO(final ParameterProviderDAO parameterProviderDAO) {
        this.parameterProviderDAO = parameterProviderDAO;
    }

    @Autowired
    public void setFlowRegistryDAO(FlowRegistryDAO flowRegistryDAO) {
        this.flowRegistryDAO = flowRegistryDAO;
    }

    @Autowired
    public void setAccessPolicyDAO(AccessPolicyDAO accessPolicyDAO) {
        this.accessPolicyDAO = accessPolicyDAO;
    }

    @Autowired
    public void setParameterContextDAO(ParameterContextDAO parameterContextDAO) {
        this.parameterContextDAO = parameterContextDAO;
    }

    @Autowired
    public void setControllerFacade(ControllerFacade controllerFacade) {
        this.controllerFacade = controllerFacade;
    }
}
