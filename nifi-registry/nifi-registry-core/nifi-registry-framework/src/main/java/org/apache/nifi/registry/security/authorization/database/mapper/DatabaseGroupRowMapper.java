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
package org.apache.nifi.registry.security.authorization.database.mapper;

import org.apache.nifi.registry.security.authorization.database.entity.DatabaseGroup;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseGroupRowMapper implements RowMapper<DatabaseGroup> {

    @Override
    public DatabaseGroup mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        final DatabaseGroup group = new DatabaseGroup();
        group.setIdentifier(rs.getString("IDENTIFIER"));
        group.setIdentity(rs.getString("IDENTITY"));
        return group;
    }
}
