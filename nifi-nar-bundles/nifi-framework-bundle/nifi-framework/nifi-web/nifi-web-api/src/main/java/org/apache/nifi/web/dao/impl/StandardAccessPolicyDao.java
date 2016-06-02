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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.authorization.AbstractPolicyBasedAuthorizer;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.dao.AccessPolicyDAO;

public class StandardAccessPolicyDao implements AccessPolicyDAO {

    final AbstractPolicyBasedAuthorizer authorizer;

    public StandardAccessPolicyDao(AbstractPolicyBasedAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public boolean hasAccessPolicy(String accessPolicyId) {
        return authorizer.getAccessPolicy(accessPolicyId) != null;
    }

    @Override
    public AccessPolicy createAccessPolicy(AccessPolicyDTO accessPolicyDTO) {
        return authorizer.addAccessPolicy(buildAccessPolicy(accessPolicyDTO));
    }

    @Override
    public AccessPolicy getAccessPolicy(String accessPolicyId) {
        return authorizer.getAccessPolicy(accessPolicyId);
    }

    @Override
    public AccessPolicy updateAccessPolicy(AccessPolicyDTO accessPolicyDTO) {
        return authorizer.updateAccessPolicy(buildAccessPolicy(accessPolicyDTO));
    }

    @Override
    public AccessPolicy deleteAccessPolicy(String accessPolicyId) {
        return authorizer.deleteAccessPolicy(authorizer.getAccessPolicy(accessPolicyId));
    }

    private AccessPolicy buildAccessPolicy(AccessPolicyDTO accessPolicyDTO) {
        final AccessPolicy.Builder builder = new AccessPolicy.Builder().addGroups(accessPolicyDTO.getGroups()).addUsers(accessPolicyDTO.getUsers());
        if (accessPolicyDTO.getCanRead()) {
            builder.addAction(RequestAction.READ);
        }
        if (accessPolicyDTO.getCanWrite()) {
            builder.addAction(RequestAction.WRITE);
        }
        return (AccessPolicy) builder.build();
    }
}
