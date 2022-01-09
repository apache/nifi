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
package org.apache.nifi.registry.db.mapper;

import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

public class FlowSnapshotEntityRowMapper implements RowMapper<FlowSnapshotEntity> {

    @Nullable
    @Override
    public FlowSnapshotEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        final FlowSnapshotEntity entity = new FlowSnapshotEntity();
        entity.setFlowId(rs.getString("FLOW_ID"));
        entity.setVersion(rs.getInt("VERSION"));
        entity.setCreated(rs.getTimestamp("CREATED"));
        entity.setCreatedBy(rs.getString("CREATED_BY"));
        entity.setComments(rs.getString("COMMENTS"));
        return entity;
    }
}
