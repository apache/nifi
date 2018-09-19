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
package org.apache.nifi.authorization.resource;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.components.RequiredPermission;

import java.util.Objects;

public final class ResourceFactory {

    private final static Resource CONTROLLER_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Controller.getValue();
        }

        @Override
        public String getName() {
            return "Controller";
        }

        @Override
        public String getSafeDescription() {
            return "the controller";
        }
    };

    private final static Resource FLOW_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Flow.getValue();
        }

        @Override
        public String getName() {
            return "NiFi Flow";
        }

        @Override
        public String getSafeDescription() {
            return "the user interface";
        }
    };

    private final static Resource POLICY_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Policy.getValue();
        }

        @Override
        public String getName() {
            return "Policies for ";
        }

        @Override
        public String getSafeDescription() {
            return "the policies for ";
        }
    };

    private final static Resource COUNTERS_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Counters.getValue();
        }

        @Override
        public String getName() {
            return "Counters";
        }

        @Override
        public String getSafeDescription() {
            return "counters";
        }
    };

    private final static Resource PROVENANCE_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Provenance.getValue();
        }

        @Override
        public String getName() {
            return "Provenance";
        }

        @Override
        public String getSafeDescription() {
            return "provenance";
        }
    };

    private final static Resource PROVENANCE_DATA_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.ProvenanceData.getValue();
        }

        @Override
        public String getName() {
            return "Provenance data for ";
        }

        @Override
        public String getSafeDescription() {
            return "the provenance data for ";
        }
    };

    private final static Resource DATA_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Data.getValue();
        }

        @Override
        public String getName() {
            return "Data for ";
        }

        @Override
        public String getSafeDescription() {
            return "the data for ";
        }
    };

    private final static Resource PROXY_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Proxy.getValue();
        }

        @Override
        public String getName() {
            return "Proxy User Requests";
        }

        @Override
        public String getSafeDescription() {
            return "proxy requests on behalf of users";
        }
    };

    private final static Resource RESOURCE_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Resource.getValue();
        }

        @Override
        public String getName() {
            return "NiFi Resources";
        }

        @Override
        public String getSafeDescription() {
            return "resources";
        }
    };

    private final static Resource SITE_TO_SITE_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.SiteToSite.getValue();
        }

        @Override
        public String getName() {
            return "Site to Site";
        }

        @Override
        public String getSafeDescription() {
            return "site-to-site details";
        }
    };

    private final static Resource SYSTEM_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.System.getValue();
        }

        @Override
        public String getName() {
            return "System";
        }

        @Override
        public String getSafeDescription() {
            return "system diagnostics";
        }
    };

    private final static Resource RESTRICTED_COMPONENTS_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.RestrictedComponents.getValue();
        }

        @Override
        public String getName() {
            return "Restricted Components";
        }

        @Override
        public String getSafeDescription() {
            return "restricted components";
        }
    };

    private final static Resource TENANT_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Tenant.getValue();
        }

        @Override
        public String getName() {
            return "Tenant";
        }

        @Override
        public String getSafeDescription() {
            return "users/user groups";
        }
    };

    private final static Resource POLICIES_RESOURCE = new Resource() {

        @Override
        public String getIdentifier() {
            return "/policies";
        }

        @Override
        public String getName() {
            return "Access Policies";
        }

        @Override
        public String getSafeDescription() {
            return "policies";
        }
    };

    /**
     * Gets the Resource for accessing the Controller. This includes Controller level configuration, bulletins, reporting tasks, and the cluster.
     *
     * @return  The resource for accessing the Controller
     */
    public static Resource getControllerResource() {
        return CONTROLLER_RESOURCE;
    }

    /**
     * Gets the Resource for accessing the NiFi flow. This includes the data flow structure, component status, search results, and banner/about text.
     *
     * @return  The NiFi resource
     */
    public static Resource getFlowResource() {
        return FLOW_RESOURCE;
    }

    /**
     * Gets the Resource for accessing the Counters..
     *
     * @return  The resource for accessing the Controller
     */
    public static Resource getCountersResource() {
        return COUNTERS_RESOURCE;
    }

    /**
     * Gets the Resource for accessing provenance. Access to this Resource allows the user to access data provenance. However, additional authorization
     * is required based on the component that generated the event and the attributes of the event.
     *
     * @return  The provenance resource
     */
    public static Resource getProvenanceResource() {
        return PROVENANCE_RESOURCE;
    }

    /**
     * Gets the Resource for proxying a user request.
     *
     * @return  The resource for proxying a user request
     */
    public static Resource getProxyResource() {
        return PROXY_RESOURCE;
    }

    /**
     * Gets the Resource for detailing all available NiFi Resources.
     *
     * @return  The Resource resource
     */
    public static Resource getResourceResource() {
        return RESOURCE_RESOURCE;
    }

    /**
     * Gets the Resource for obtaining site to site details. This will allow other NiFi instances to obtain necessary configuration to initiate a
     * site to site data transfer.
     *
     * @return  The resource for obtaining site to site details
     */
    public static Resource getSiteToSiteResource() {
        return SITE_TO_SITE_RESOURCE;
    }

    /**
     * Gets the Resource for accessing details of the System NiFi is running on.
     *
     * @return  The System resource
     */
    public static Resource getSystemResource() {
        return SYSTEM_RESOURCE;
    }

    /**
     * Gets the Resource for accessing restricted components.
     *
     * @return The restricted components resource
     */
    public static Resource getRestrictedComponentsResource() {
        return RESTRICTED_COMPONENTS_RESOURCE;
    }

    /**
     * Gets a Resource for accessing certain kinds of restricted components.
     *
     * @param requiredPermission The required permission
     * @return The restricted components resource
     */
    public static Resource getRestrictedComponentsResource(final RequiredPermission requiredPermission) {
        return new Resource() {
            @Override
            public String getIdentifier() {
                return RESTRICTED_COMPONENTS_RESOURCE.getIdentifier() + "/" + requiredPermission.getPermissionIdentifier();
            }

            @Override
            public String getName() {
                return requiredPermission.getPermissionLabel();
            }

            @Override
            public String getSafeDescription() {
                return "Components requiring additional permission: " + requiredPermission.getPermissionLabel();
            }
        };
    }

    /**
     * Gets the Resource for accessing Tenants which includes creating, modifying, and deleting Users and UserGroups.
     *
     * @return The Resource for accessing Tenants
     */
    public static Resource getTenantResource() {
        return TENANT_RESOURCE;
    }

    /**
     * Gets a Resource for performing transferring data to a port.
     *
     * @param resource      The resource to transfer data to
     * @return              The resource
     */
    public static Resource getDataTransferResource(final Resource resource) {
        Objects.requireNonNull(resource, "The resource must be specified.");

        return new Resource() {
            @Override
            public String getIdentifier() {
                return ResourceType.DataTransfer.getValue() + resource.getIdentifier();
            }

            @Override
            public String getName() {
                return "Transfer data to " + resource.getName();
            }

            @Override
            public String getSafeDescription() {
                return "data transfers to " + resource.getSafeDescription();
            }
        };
    }

    /**
     * Gets the {@link Resource} for accessing access policies.
     * @return The policies resource
     */
    public static Resource getPoliciesResource() {
        return POLICIES_RESOURCE;
    }

    /**
     * Gets a Resource for accessing a resources's policies.
     *
     * @param resource      The resource being accessed
     * @return              The resource
     */
    public static Resource getPolicyResource(final Resource resource) {
        Objects.requireNonNull(resource, "The resource type must be specified.");

        return new Resource() {
            @Override
            public String getIdentifier() {
                return POLICY_RESOURCE.getIdentifier() + resource.getIdentifier();
            }

            @Override
            public String getName() {
                return POLICY_RESOURCE.getName() + resource.getName();
            }

            @Override
            public String getSafeDescription() {
                return POLICY_RESOURCE.getSafeDescription() + resource.getSafeDescription();
            }
        };
    }

    /**
     * Gets a Resource for accessing component operations.
     *
     * @param resource      The resource being accessed
     * @return              The resource
     */
    public static Resource getOperationResource(final Resource resource) {
        Objects.requireNonNull(resource, "The resource type must be specified.");

        return new Resource() {
            @Override
            public String getIdentifier() {
                return ResourceType.Operation.getValue() + resource.getIdentifier();
            }

            @Override
            public String getName() {
                return "Operations for" + resource.getName();
            }

            @Override
            public String getSafeDescription() {
                return "Operations for" + resource.getSafeDescription();
            }
        };
    }

    /**
     * Gets a Resource for accessing a component configuration.
     *
     * @param resourceType  The type of resource being accessed
     * @param identifier    The identifier of the component being accessed
     * @param name          The name of the component being accessed
     * @return              The resource
     */
    public static Resource getComponentResource(final ResourceType resourceType, final String identifier, final String name) {
        Objects.requireNonNull(resourceType, "The resource must be specified.");
        Objects.requireNonNull(identifier, "The component identifier must be specified.");

        return new Resource() {
            @Override
            public String getIdentifier() {
                return resourceType.getValue() + "/" + identifier;
            }

            @Override
            public String getName() {
                return name;
            }

            @Override
            public String getSafeDescription() {
                final String componentType;
                switch (resourceType) {
                    case ControllerService:
                        componentType = "Controller Service";
                        break;
                    case ProcessGroup:
                        componentType = "Process Group";
                        break;
                    case Template:
                        componentType = "Template";
                        break;
                    case Funnel:
                        componentType = "Funnel";
                        break;
                    case InputPort:
                        componentType = "Input Port";
                        break;
                    case OutputPort:
                        componentType = "Output Port";
                        break;
                    case Processor:
                        componentType = "Processor";
                        break;
                    case RemoteProcessGroup:
                        componentType = "Remote Process Group";
                        break;
                    case ReportingTask:
                        componentType = "Reporting Task";
                        break;
                    case Label:
                        componentType = "Label";
                        break;
                    default:
                        componentType = "Component";
                        break;
                }
                return componentType + " with ID " + identifier;
            }
        };
    }

    /**
     * Gets a Resource for accessing flowfile information
     *
     * @param resource The resource for the component being accessed
     * @return The resource for the data of the component being accessed
     */
    public static Resource getDataResource(final Resource resource) {
        return new Resource() {
            @Override
            public String getIdentifier() {
                return DATA_RESOURCE.getIdentifier() + resource.getIdentifier();
            }

            @Override
            public String getName() {
                return DATA_RESOURCE.getName() + resource.getName();
            }

            @Override
            public String getSafeDescription() {
                return DATA_RESOURCE.getSafeDescription() + resource.getSafeDescription();
            }
        };
    }

    /**
     * Gets a Resource for accessing provenance data.
     *
     * @param resource      The resource for the component being accessed
     * @return              The resource for the provenance data being accessed
     */
    public static Resource getProvenanceDataResource(final Resource resource) {
        Objects.requireNonNull(resource, "The resource must be specified.");

        return new Resource() {
            @Override
            public String getIdentifier() {
                return String.format("%s%s", PROVENANCE_DATA_RESOURCE.getIdentifier(), resource.getIdentifier());
            }

            @Override
            public String getName() {
                return PROVENANCE_DATA_RESOURCE.getName() + resource.getName();
            }

            @Override
            public String getSafeDescription() {
                return PROVENANCE_DATA_RESOURCE.getSafeDescription() + resource.getSafeDescription();
            }
        };
    }

    /**
     * Prevent outside instantiation.
     */
    private ResourceFactory() {}
}
