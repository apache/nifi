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

import java.util.Objects;

public final class ResourceFactory {

    private final static Resource CONNECTION_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Connection.getValue();
        }

        @Override
        public String getName() {
            return "Connection";
        }
    };

    private final static Resource CONTROLLER_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Controller.getValue();
        }

        @Override
        public String getName() {
            return "Controller";
        }
    };

    private final static Resource CONTROLLER_SERVICE_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.ControllerService.getValue();
        }

        @Override
        public String getName() {
            return "Controller Service";
        }
    };

    private final static Resource FUNNEL_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Funnel.getValue();
        }

        @Override
        public String getName() {
            return "Funnel";
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
    };

    private final static Resource GROUP_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Group.getValue();
        }

        @Override
        public String getName() {
            return "Group";
        }
    };

    private final static Resource INPUT_PORT_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.InputPort.getValue();
        }

        @Override
        public String getName() {
            return "Input Port";
        }
    };

    private final static Resource LABEL_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Label.getValue();
        }

        @Override
        public String getName() {
            return "Label";
        }
    };

    private final static Resource OUTPUT_PORT_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.OutputPort.getValue();
        }

        @Override
        public String getName() {
            return "Output Port";
        }
    };

    private final static Resource POLICY_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Policy.getValue();
        }

        @Override
        public String getName() {
            return "Policy";
        }
    };

    private final static Resource PROCESSOR_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Processor.getValue();
        }

        @Override
        public String getName() {
            return "Processor";
        }
    };

    private final static Resource PROCESS_GROUP_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.ProcessGroup.getValue();
        }

        @Override
        public String getName() {
            return "Process Group";
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
    };

    private final static Resource REMOTE_PROCESS_GROUP_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.RemoteProcessGroup.getValue();
        }

        @Override
        public String getName() {
            return "Remote Process Group";
        }
    };

    private final static Resource REPORTING_TASK_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.ReportingTask.getValue();
        }

        @Override
        public String getName() {
            return "Reporting Task";
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
    };

    private final static Resource TEMPLATE_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Template.getValue();
        }

        @Override
        public String getName() {
            return "Template";
        }
    };

    private final static Resource TOKEN_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Token.getValue();
        }

        @Override
        public String getName() {
            return "API access token";
        }
    };

    private final static Resource USER_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.User.getValue();
        }

        @Override
        public String getName() {
            return "User";
        }
    };

    /**
     * Gets the Resource for accessing Connections.
     *
     * @return The resource for accessing connections
     */
    public static Resource getConnectionResource() {
        return CONNECTION_RESOURCE;
    }

    /**
     * Gets the Resource for accessing the Controller. This includes Controller level configuration, bulletins, reporting tasks, and the cluster.
     *
     * @return  The resource for accessing the Controller
     */
    public static Resource getControllerResource() {
        return CONTROLLER_RESOURCE;
    }

    /**
     * Gets the Resource for accessing Controller Services.
     *
     * @return The resource for accessing Controller Services
     */
    public static Resource getControllerServiceResource() {
        return CONTROLLER_SERVICE_RESOURCE;
    }

    /**
     * Gets the Resource for accessing Funnels.
     *
     * @return The resource for accessing Funnels.
     */
    public static Resource getFunnelResource() {
        return FUNNEL_RESOURCE;
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
     * Gets the Resource for accessing Groups which allows management of user groups.
     *
     * @return The resource for accessing Groups
     */
    public static Resource getGroupResource() {
        return GROUP_RESOURCE;
    }

    /**
     * Gets the Resource for accessing Input Ports.
     *
     * @return The resource for accessing Input Ports
     */
    public static Resource getInputPortResource() {
        return INPUT_PORT_RESOURCE;
    }

    /**
     * Gets the Resource for accessing Labels.
     *
     * @return The resource for accessing Labels
     */
    public static Resource getLabelResource() {
        return LABEL_RESOURCE;
    }

    /**
     * Gets the Resource for accessing Output Ports.
     *
     * @return The resource for accessing Output Ports
     */
    public static Resource getOutputPortResource() {
        return OUTPUT_PORT_RESOURCE;
    }

    /**
     * Gets the Resource for accessing Policies which allows management of Access Policies.
     *
     * @return The resource for accessing Policies
     */
    public static Resource getPolicyResource() {
        return POLICY_RESOURCE;
    }

    /**
     * Gets the Resource for accessing Processors.
     *
     * @return The resource for accessing Processors
     */
    public static Resource getProcessorResource() {
        return PROCESSOR_RESOURCE;
    }

    /**
     * Gets the Resource for accessing Process Groups.
     *
     * @return The resource for accessing Process Groups
     */
    public static Resource getProcessGroupResource() {
        return PROCESS_GROUP_RESOURCE;
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
     * Gets the Resource for accessing Remote Process Groups.
     *
     * @return The resource accessing Remote Process Groups
     */
    public static Resource getRemoteProcessGroupResource() {
        return REMOTE_PROCESS_GROUP_RESOURCE;
    }

    /**
     * Gets the Resource for accessing Reporting Tasks.
     *
     * @return The resource for accessing Reporting Tasks
     */
    public static Resource getReportingTaskResource() {
        return REPORTING_TASK_RESOURCE;
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
     * Gets the Resource for accessing Templates.
     *
     * @return The Resource for accessing Tempaltes
     */
    public static Resource getTemplateResource() {
        return TEMPLATE_RESOURCE;
    }

    /**
     * Gets the Resource for creating API access tokens.
     *
     * @return  The token request resource
     */
    public static Resource getTokenResource() {
        return TOKEN_RESOURCE;
    }

    /**
     * Gets the Resource for accessing Users which includes creating, modifying, and deleting Users.
     *
     * @return The Resource for accessing Users
     */
    public static Resource getUserResource() {
        return USER_RESOURCE;
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
                return String.format("%s/%s", resourceType.getValue(), identifier);
            }

            @Override
            public String getName() {
                return name;
            }
        };
    }

    /**
     * Gets a Resource for accessing a component's provenance events.
     *
     * @param resourceType  The type of resource being accessed
     * @param identifier    The identifier of the component being accessed
     * @param name          The name of the component being accessed
     * @return              The resource
     */
    public static Resource getComponentProvenanceResource(final ResourceType resourceType, final String identifier, final String name) {
        final Resource componentResource = getComponentResource(resourceType, identifier, name);
        return new Resource() {
            @Override
            public String getIdentifier() {
                return String.format("%s/%s", componentResource.getIdentifier(), "provenance");
            }

            @Override
            public String getName() {
                return componentResource.getName() + " provenance";
            }
        };
    }

    /**
     * Gets a Resource fo accessing a flowfile queue for the specified connection.
     *
     * @param connectionIdentifier  The identifier of the connection
     * @param connectionName        The name of the connection
     * @return                      The resource
     */
    public static Resource getFlowFileQueueResource(final String connectionIdentifier, final String connectionName) {
        Objects.requireNonNull(connectionIdentifier, "The connection identifier must be specified.");
        Objects.requireNonNull(connectionName, "The connection name must be specified.");

        return new Resource() {
            @Override
            public String getIdentifier() {
                return String.format("/flowfile-queue/%s", connectionIdentifier);
            }

            @Override
            public String getName() {
                return connectionName + " queue";
            }
        };
    }

    /**
     * Prevent outside instantiation.
     */
    private ResourceFactory() {}
}
