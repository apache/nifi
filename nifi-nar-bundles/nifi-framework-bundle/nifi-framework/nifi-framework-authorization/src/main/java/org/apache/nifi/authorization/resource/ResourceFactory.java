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

    private final static Resource FLOW_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "/flow";
        }

        @Override
        public String getName() {
            return "NiFi Flow";
        }
    };

    private final static Resource RESOURCE_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "/resources";
        }

        @Override
        public String getName() {
            return "NiFi Resources";
        }
    };

    private final static Resource SYSTEM_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "/system";
        }

        @Override
        public String getName() {
            return "System";
        }
    };

    private final static Resource CONTROLLER_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "/controller";
        }

        @Override
        public String getName() {
            return "Controller";
        }
    };

    private final static Resource PROVENANCE_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "/provenance";
        }

        @Override
        public String getName() {
            return "Provenance";
        }
    };

    private final static Resource TOKEN_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "/token";
        }

        @Override
        public String getName() {
            return "API access token";
        }
    };

    private final static Resource SITE_TO_SITE_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "/site-to-site";
        }

        @Override
        public String getName() {
            return "Site to Site";
        }
    };

    private final static Resource PROXY_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "/proxy";
        }

        @Override
        public String getName() {
            return "Proxy User Requests";
        }
    };

    /**
     * Gets the Resource for accessing the NiFi flow. This includes the data flow structure, component status, search results, and banner/about text.
     *
     * @return  The NiFi resource
     */
    public static Resource getFlowResource() {
        return FLOW_RESOURCE;
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
     * Gets the Resource for accessing details of the System NiFi is running on.
     *
     * @return  The System resource
     */
    public static Resource getSystemResource() {
        return SYSTEM_RESOURCE;
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
     * Gets the Resource for accessing provenance. Access to this Resource allows the user to access data provenance. However, additional authorization
     * is required based on the component that generated the event and the attributes of the event.
     *
     * @return  The provenance resource
     */
    public static Resource getProvenanceResource() {
        return PROVENANCE_RESOURCE;
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
     * Gets the Resource for obtaining site to site details. This will allow other NiFi instances to obtain necessary configuration to initiate a
     * site to site data transfer.
     *
     * @return  The resource for obtaining site to site details
     */
    public static Resource getSiteToSiteResource() {
        return SITE_TO_SITE_RESOURCE;
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
