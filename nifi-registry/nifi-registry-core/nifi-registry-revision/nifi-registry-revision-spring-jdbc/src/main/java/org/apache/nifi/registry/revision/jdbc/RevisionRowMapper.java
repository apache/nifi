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
package org.apache.nifi.registry.revision.jdbc;

import org.apache.nifi.registry.revision.api.Revision;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RevisionRowMapper implements RowMapper<Revision> {

    @Override
    public Revision mapRow(final ResultSet rs, final int i) throws SQLException {
        final String entityId = rs.getString("ENTITY_ID");
        final Long version = rs.getLong("VERSION");
        final String clientId = rs.getString("CLIENT_ID");
        return new Revision(version, clientId, entityId);
    }

}
