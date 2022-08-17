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
package org.apache.nifi.registry.security.authorization;

import org.apache.nifi.registry.security.authorization.resource.Authorizable;

public interface AuthorizableLookup {

    /**
     * Get the authorizable for /actuator.
     *
     * @return authorizable
     */
    Authorizable getActuatorAuthorizable();

    /**
     * Get the authorizable for /swagger.
     *
     * @return authorizable
     */
    Authorizable getSwaggerAuthorizable();

    /**
     * Get the authorizable for /proxy.
     *
     * @return authorizable
     */
    Authorizable getProxyAuthorizable();

    /**
     * Get the authorizable for all tenants.
     *
     * Get the {@link Authorizable} that represents the resource of users and user groups.
     * @return authorizable
     */
    Authorizable getTenantsAuthorizable();

    /**
     * Get the authorizable for all access policies.
     *
     * @return authorizable
     */
    Authorizable getPoliciesAuthorizable();

    /**
     * Get the authorizable for all Buckets.
     *
     * @return authorizable
     */
    Authorizable getBucketsAuthorizable();

    /**
     * Get the authorizable for the Bucket with the bucket id.
     *
     * @param bucketIdentifier bucket id
     * @return authorizable
     */
    Authorizable getBucketAuthorizable(String bucketIdentifier);

    /**
     * Get the authorizable of the specified resource.
     * If the resource is authorized by its base/top-level
     * resource type, the authorizable for the base type will be returned.
     *
     * @param resource resource
     * @return authorizable
     */
    Authorizable getAuthorizableByResource(final String resource);

}
