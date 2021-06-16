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
package org.apache.nifi.toolkit.cli.impl.result.nifi;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.TenantEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Set;
import java.util.stream.Collectors;

public class AccessPolicyResult extends AbstractWritableResult<AccessPolicyEntity> {

    private final AccessPolicyEntity accessPolicyEntity;

    public AccessPolicyResult(ResultType resultType, AccessPolicyEntity accessPolicyEntity) {
        super(resultType);
        this.accessPolicyEntity = accessPolicyEntity;
        Validate.notNull(accessPolicyEntity);
    }

    @Override
    public AccessPolicyEntity getResult() {
        return accessPolicyEntity;
    }

    @Override
    protected void writeSimpleResult(PrintStream output) throws IOException {
        final AccessPolicyDTO accessPolicyDTO = accessPolicyEntity.getComponent();

        output.printf("Resource: %s\nAction  : %s\nUsers   : %s\nGroups  : %s\n",
                accessPolicyDTO.getResource(),
                accessPolicyDTO.getAction(),
                joinTenantIdentity(accessPolicyDTO.getUsers()),
                joinTenantIdentity(accessPolicyDTO.getUserGroups())
        );
    }

    private String joinTenantIdentity(Set<TenantEntity> entities) {
        return entities.stream()
                .map(e -> e.getComponent() != null ? e.getComponent().getIdentity() : e.getId())
                .collect(Collectors.joining(", "));
    }
}
