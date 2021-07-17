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
package org.apache.nifi.registry.security.authorization.resource;

import org.apache.nifi.registry.security.authorization.Resource;

import java.util.Objects;

public final class ResourceFactory {

    private final static Resource BUCKETS_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Bucket.getValue();
        }

        @Override
        public String getName() {
            return "Buckets";
        }

        @Override
        public String getSafeDescription() {
            return "buckets";
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

    private final static Resource TENANTS_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Tenant.getValue();
        }

        @Override
        public String getName() {
            return "Tenants";
        }

        @Override
        public String getSafeDescription() {
            return "users/user groups";
        }
    };

    private final static Resource POLICIES_RESOURCE = new Resource() {

        @Override
        public String getIdentifier() {
            return ResourceType.Policy.getValue();
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

    private final static Resource ACTUATOR_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Actuator.getValue();
        }

        @Override
        public String getName() {
            return "Actuator";
        }

        @Override
        public String getSafeDescription() {
            return "actuator";
        }
    };

    private final static Resource SWAGGER_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return ResourceType.Swagger.getValue();
        }

        @Override
        public String getName() {
            return "Swagger";
        }

        @Override
        public String getSafeDescription() {
            return "swagger";
        }
    };

    /**
     * Gets the Resource for actuator system management endpoints.
     *
     * @return  The resource for actuator system management endpoints.
     */
    public static Resource getActuatorResource() {
        return ACTUATOR_RESOURCE;
    }

    /**
     * Gets the Resource for swagger UI static resources.
     *
     * @return  The resource for swagger UI static resources.
     */
    public static Resource getSwaggerResource() {
        return SWAGGER_RESOURCE;
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
     * Gets the Resource for accessing Tenants which includes creating, modifying, and deleting Users and UserGroups.
     *
     * @return The Resource for accessing Tenants
     */
    public static Resource getTenantsResource() {
        return TENANTS_RESOURCE;
    }

    /**
     * Gets the {@link Resource} for accessing access policies.
     * @return The policies resource
     */
    public static Resource getPoliciesResource() {
        return POLICIES_RESOURCE;
    }

    /**
     * Gets the {@link Resource} for accessing buckets.
     * @return The buckets resource
     */
    public static Resource getBucketsResource() {
        return BUCKETS_RESOURCE;
    }

    /**
     * Gets the {@link Resource} for accessing buckets.
     * @return The buckets resource
     */
    public static Resource getBucketResource(String bucketIdentifier, String bucketName) {
        return getChildResource(ResourceType.Bucket, bucketIdentifier, bucketName);
    }

    /**
     * Get a Resource object for any object that has a base type and an identifier, ie:
     * /buckets/{uuid}
     *
     * @param parentResourceType - Required, the base resource type
     * @param childIdentifier - Required, the identity of this sub resource
     * @param name - Optional, the name of the subresource
     * @return A resource for this object
     */
    private static Resource getChildResource(final ResourceType parentResourceType, final String childIdentifier, final String name) {
        Objects.requireNonNull(parentResourceType, "The base resource type must be specified.");
        Objects.requireNonNull(childIdentifier, "The child identifier identifier must be specified.");

        return new Resource() {
            @Override
            public String getIdentifier() {
                return String.format("%s/%s", parentResourceType.getValue(), childIdentifier);
            }

            @Override
            public String getName() {
                return name;
            }

            @Override
            public String getSafeDescription() {
                final StringBuilder safeDescription = new StringBuilder();
                switch (parentResourceType) {
                    case Bucket:
                        safeDescription.append("Bucket");
                        break;
                    default:
                        safeDescription.append("Unknown resource type");
                        break;
                }
                safeDescription.append(" with ID ");
                safeDescription.append(childIdentifier);
                return safeDescription.toString();
            }
        };

    }

    /**
     * Prevent outside instantiation.
     */
    private ResourceFactory() {}
}
