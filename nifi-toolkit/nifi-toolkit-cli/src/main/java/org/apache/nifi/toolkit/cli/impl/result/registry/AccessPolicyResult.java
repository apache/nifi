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
package org.apache.nifi.toolkit.cli.impl.result.registry;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.authorization.Tenant;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Set;
import java.util.stream.Collectors;

public class AccessPolicyResult extends AbstractWritableResult<AccessPolicy> {
    private final AccessPolicy accessPolicy;

    public AccessPolicyResult(ResultType resultType, AccessPolicy accessPolicy) {
        super(resultType);
        this.accessPolicy = accessPolicy;
        Validate.notNull(accessPolicy);
    }

    @Override
    public AccessPolicy getResult() {
        return accessPolicy;
    }

    @Override
    protected void writeSimpleResult(PrintStream output) throws IOException {
        output.printf("Resource: %s\nAction  : %s\nUsers   : %s\nGroups  : %s\n",
            accessPolicy.getResource(),
            accessPolicy.getAction(),
            joinTenantIdentities(accessPolicy.getUsers()),
            joinTenantIdentities(accessPolicy.getUserGroups())
        );
    }

    private String joinTenantIdentities(Set<Tenant> tenants) {
        return tenants.stream()
            .map(tenant -> tenant.getIdentity())
            .collect(Collectors.joining(", "));
    }
}
