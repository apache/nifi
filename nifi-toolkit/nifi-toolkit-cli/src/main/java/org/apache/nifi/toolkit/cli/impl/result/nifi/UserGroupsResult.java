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
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.entity.UserGroupsEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Result for UserGroupsEntity.
 */
public class UserGroupsResult extends AbstractWritableResult<UserGroupsEntity> {

    private final UserGroupsEntity userGroupsEntity;

    public UserGroupsResult(final ResultType resultType, final UserGroupsEntity userGroupsEntity) {
        super(resultType);
        this.userGroupsEntity = userGroupsEntity;
        Validate.notNull(this.userGroupsEntity);
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Collection<UserGroupEntity> userGroupEntities = userGroupsEntity.getUserGroups();
        if (userGroupEntities == null) {
            return;
        }

        final List<UserGroupDTO> userGroupDTOS = userGroupEntities.stream()
                .map(s -> s.getComponent())
                .collect(Collectors.toList());

        Collections.sort(userGroupDTOS, Comparator.comparing(UserGroupDTO::getIdentity));

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 5, 40, false)
                .column("ID", 36, 36, false)
                .column("Members", 20, 40, true)
                .build();

        for (int i = 0; i < userGroupDTOS.size(); i++) {
            final UserGroupDTO userGroupDTO = userGroupDTOS.get(i);
            table.addRow(
                    String.valueOf(i + 1),
                    userGroupDTO.getIdentity(),
                    userGroupDTO.getId(),
                    userGroupDTO.getUsers().stream().map(u -> u.getComponent().getIdentity())
                            .collect(Collectors.joining(", "))
            );
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public UserGroupsEntity getResult() {
        return userGroupsEntity;
    }
}
