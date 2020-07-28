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
package org.apache.nifi.web.dao;

import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;

public interface AccessPolicyDAO {

    String MSG_NON_MANAGED_AUTHORIZER = "This NiFi is not configured to internally manage users, groups, or policies.  Please contact your system administrator.";
    String MSG_NON_CONFIGURABLE_POLICIES = "This NiFi is not configured to allow configurable policies. Please contact your system administrator.";
    String MSG_NON_CONFIGURABLE_USERS = "This NiFi is not configured to allow configurable users and groups. Please contact your system administrator.";

    /**
     * Whether or not NiFi supports a configurable authorizer.
     *
     * @return whether or not NiFi supports a configurable authorizer
     */
    boolean supportsConfigurableAuthorizer();

    /**
     * @param accessPolicyId access policy ID
     * @return Determines if the specified access policy exists
     */
    boolean hasAccessPolicy(String accessPolicyId);

    /**
     * Creates an access policy.
     *
     * @param accessPolicyDTO The access policy DTO
     * @return The access policy transfer object
     */
    AccessPolicy createAccessPolicy(AccessPolicyDTO accessPolicyDTO);

    /**
     * Gets the access policy with the specified ID.
     *
     * @param accessPolicyId The access policy ID
     * @return The access policy transfer object
     */
    AccessPolicy getAccessPolicy(String accessPolicyId);

    /**
     * Gets the access policy according to the action and authorizable. Will return null
     * if no policy exists for the specific resource.
     *
     * @param requestAction action
     * @param resource resource
     * @return access policy
     */
    AccessPolicy getAccessPolicy(RequestAction requestAction, String resource);

    /**
     * Gets the access policy according to the action and authorizable. Will return the
     * effective policy if no policy exists for the specific authorizable.
     *
     * @param requestAction action
     * @param authorizable authorizable
     * @return access policy
     */
    AccessPolicy getAccessPolicy(RequestAction requestAction, Authorizable authorizable);

    /**
     * Updates the specified access policy.
     *
     * @param accessPolicyDTO The access policy DTO
     * @return The access policy transfer object
     */
    AccessPolicy updateAccessPolicy(AccessPolicyDTO accessPolicyDTO);

    /**
     * Deletes the specified access policy.
     *
     * @param accessPolicyId The access policy ID
     * @return The access policy transfer object of the deleted access policy
     */
    AccessPolicy deleteAccessPolicy(String accessPolicyId);


}
