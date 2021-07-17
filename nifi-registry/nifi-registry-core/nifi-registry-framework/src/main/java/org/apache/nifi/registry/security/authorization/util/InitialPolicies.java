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
package org.apache.nifi.registry.security.authorization.util;

import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.resource.ResourceFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Defines the initial policies to be created for the initial users.
 */
public final class InitialPolicies {

    // Resource-actions pairs used by initial admins and NiFi identities

    public static final ResourceAndAction TENANTS_READ = new ResourceAndAction(ResourceFactory.getTenantsResource(), RequestAction.READ);
    public static final ResourceAndAction TENANTS_WRITE = new ResourceAndAction(ResourceFactory.getTenantsResource(), RequestAction.WRITE);
    public static final ResourceAndAction TENANTS_DELETE = new ResourceAndAction(ResourceFactory.getTenantsResource(), RequestAction.DELETE);

    public static final ResourceAndAction POLICIES_READ = new ResourceAndAction(ResourceFactory.getPoliciesResource(), RequestAction.READ);
    public static final ResourceAndAction POLICIES_WRITE = new ResourceAndAction(ResourceFactory.getPoliciesResource(), RequestAction.WRITE);
    public static final ResourceAndAction POLICIES_DELETE = new ResourceAndAction(ResourceFactory.getPoliciesResource(), RequestAction.DELETE);

    public static final ResourceAndAction BUCKETS_READ = new ResourceAndAction(ResourceFactory.getBucketsResource(), RequestAction.READ);
    public static final ResourceAndAction BUCKETS_WRITE = new ResourceAndAction(ResourceFactory.getBucketsResource(), RequestAction.WRITE);
    public static final ResourceAndAction BUCKETS_DELETE = new ResourceAndAction(ResourceFactory.getBucketsResource(), RequestAction.DELETE);

    public static final ResourceAndAction ACTUATOR_READ = new ResourceAndAction(ResourceFactory.getActuatorResource(), RequestAction.READ);
    public static final ResourceAndAction ACTUATOR_WRITE = new ResourceAndAction(ResourceFactory.getActuatorResource(), RequestAction.WRITE);
    public static final ResourceAndAction ACTUATOR_DELETE = new ResourceAndAction(ResourceFactory.getActuatorResource(), RequestAction.DELETE);

    public static final ResourceAndAction SWAGGER_READ = new ResourceAndAction(ResourceFactory.getSwaggerResource(), RequestAction.READ);
    public static final ResourceAndAction SWAGGER_WRITE = new ResourceAndAction(ResourceFactory.getSwaggerResource(), RequestAction.WRITE);
    public static final ResourceAndAction SWAGGER_DELETE = new ResourceAndAction(ResourceFactory.getSwaggerResource(), RequestAction.DELETE);

    public static final ResourceAndAction PROXY_READ = new ResourceAndAction(ResourceFactory.getProxyResource(), RequestAction.READ);
    public static final ResourceAndAction PROXY_WRITE = new ResourceAndAction(ResourceFactory.getProxyResource(), RequestAction.WRITE);
    public static final ResourceAndAction PROXY_DELETE = new ResourceAndAction(ResourceFactory.getProxyResource(), RequestAction.DELETE);

    /**
     * Resource-action pairs to create policies for an initial admin.
     */
    public static final Set<ResourceAndAction> ADMIN_POLICIES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    TENANTS_READ,
                    TENANTS_WRITE,
                    TENANTS_DELETE,
                    POLICIES_READ,
                    POLICIES_WRITE,
                    POLICIES_DELETE,
                    BUCKETS_READ,
                    BUCKETS_WRITE,
                    BUCKETS_DELETE,
                    ACTUATOR_READ,
                    ACTUATOR_WRITE,
                    ACTUATOR_DELETE,
                    SWAGGER_READ,
                    SWAGGER_WRITE,
                    SWAGGER_DELETE,
                    PROXY_READ,
                    PROXY_WRITE,
                    PROXY_DELETE
            ))
    );

    /**
     * Resource-action paris to create policies for initial NiFi users.
     */
    public static final Set<ResourceAndAction> NIFI_POLICIES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    BUCKETS_READ,
                    PROXY_READ,
                    PROXY_WRITE,
                    PROXY_DELETE
            ))
    );

    private InitialPolicies() {

    }
}
