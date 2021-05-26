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
package org.apache.nifi.registry.db.migration;

import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

/**
 * Service used to load data from original database used in the 0.1.0 release.
 */
public class LegacyDatabaseService {

    private final JdbcTemplate jdbcTemplate;

    public LegacyDatabaseService(final DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public List<BucketEntityV1> getAllBuckets() {
        final String sql = "SELECT * FROM bucket ORDER BY name ASC";

        return jdbcTemplate.query(sql, (rs, i) -> {
            final BucketEntityV1 b = new BucketEntityV1();
            b.setId(rs.getString("ID"));
            b.setName(rs.getString("NAME"));
            b.setDescription(rs.getString("DESCRIPTION"));
            b.setCreated(rs.getTimestamp("CREATED"));
            return b;
        });
    }

    public List<FlowEntityV1> getAllFlows() {
        final String sql = "SELECT * FROM flow f, bucket_item item WHERE item.id = f.id";

        return jdbcTemplate.query(sql, (rs, i) -> {
            final FlowEntityV1 flowEntity = new FlowEntityV1();
            flowEntity.setId(rs.getString("ID"));
            flowEntity.setName(rs.getString("NAME"));
            flowEntity.setDescription(rs.getString("DESCRIPTION"));
            flowEntity.setCreated(rs.getTimestamp("CREATED"));
            flowEntity.setModified(rs.getTimestamp("MODIFIED"));
            flowEntity.setBucketId(rs.getString("BUCKET_ID"));
            return flowEntity;
        });
    }

    public List<FlowSnapshotEntityV1> getAllFlowSnapshots() {
        final String sql = "SELECT * FROM flow_snapshot fs";

        return jdbcTemplate.query(sql, (rs, i) -> {
            final FlowSnapshotEntityV1 fs = new FlowSnapshotEntityV1();
            fs.setFlowId(rs.getString("FLOW_ID"));
            fs.setVersion(rs.getInt("VERSION"));
            fs.setCreated(rs.getTimestamp("CREATED"));
            fs.setCreatedBy(rs.getString("CREATED_BY"));
            fs.setComments(rs.getString("COMMENTS"));
            return fs;
        });
    }

}
