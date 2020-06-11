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
import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;

import java.io.PrintStream;
import java.util.List;

/**
 * Result for a list of users.
 */
public class UsersResult extends AbstractWritableResult<List<User>> {
    private final List<User> users;

    public UsersResult(ResultType resultType, List<User> users) {
        super(resultType);
        this.users = users;
        Validate.notNull(users);
    }

    @Override
    public List<User> getResult() {
        return users;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        if (users.isEmpty()) {
            return;
        }

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 20, 36, true)
                .column("Id", 36, 36, false)
                .build();

        for (int userIndex = 0; userIndex < users.size(); ++userIndex) {
            final User user = users.get(userIndex);
            table.addRow(String.valueOf(userIndex + 1), user.getIdentity(), user.getIdentifier());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }
}
