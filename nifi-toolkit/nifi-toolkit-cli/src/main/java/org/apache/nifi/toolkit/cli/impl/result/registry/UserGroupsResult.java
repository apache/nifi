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
import org.apache.nifi.registry.authorization.Tenant;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Result for a list of users.
 */
public class UserGroupsResult extends AbstractWritableResult<List<UserGroup>> {
    private final List<UserGroup> userGroups;

    public UserGroupsResult(final ResultType resultType, final List<UserGroup> userGroups) {
        super(resultType);
        this.userGroups = userGroups;
        Validate.notNull(userGroups);
    }

    @Override
    public List<UserGroup> getResult() {
        return userGroups;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        if (userGroups.isEmpty()) {
            return;
        }

        final Table table = new Table.Builder()
            .column("#", 3, 3, false)
            .column("Name", 20, 36, true)
            .column("Id", 36, 36, false)
            .column("Users", 36, 200, false)
            .build();

        for (int userIndex = 0; userIndex < userGroups.size(); ++userIndex) {
            final UserGroup userGroup = userGroups.get(userIndex);
            table.addRow(
                String.valueOf(userIndex + 1),
                userGroup.getIdentity(),
                userGroup.getIdentifier(),
                joinTenantIdentities(userGroup.getUsers())
            );
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    private String joinTenantIdentities(Set<Tenant> tenants) {
        return tenants.stream()
            .map(tenant -> tenant.getIdentity())
            .collect(Collectors.joining("; "));
    }
}
