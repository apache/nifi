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

import org.apache.nifi.registry.db.entity.BundleVersionEntity;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BundleVersionEntityRowMapper implements RowMapper<BundleVersionEntity> {

    @Override
    public BundleVersionEntity mapRow(final ResultSet rs, final int i) throws SQLException {
        final BundleVersionEntity entity = new BundleVersionEntity();
        entity.setId(rs.getString("ID"));
        entity.setBundleId(rs.getString("BUNDLE_ID"));
        entity.setBucketId(rs.getString("BUCKET_ID"));
        entity.setGroupId(rs.getString("GROUP_ID"));
        entity.setArtifactId(rs.getString("ARTIFACT_ID"));
        entity.setVersion(rs.getString("VERSION"));
        entity.setSha256Hex(rs.getString("SHA_256_HEX"));
        entity.setSha256Supplied(rs.getInt("SHA_256_SUPPLIED") == 1);
        entity.setContentSize(rs.getLong("CONTENT_SIZE"));
        entity.setSystemApiVersion(rs.getString("SYSTEM_API_VERSION"));

        entity.setBuildTool(rs.getString("BUILD_TOOL"));
        entity.setBuildFlags(rs.getString("BUILD_FLAGS"));
        entity.setBuildBranch(rs.getString("BUILD_BRANCH"));
        entity.setBuildTag(rs.getString("BUILD_TAG"));
        entity.setBuildRevision(rs.getString("BUILD_REVISION"));
        entity.setBuilt(rs.getTimestamp("BUILT"));
        entity.setBuiltBy(rs.getString("BUILT_BY"));

        entity.setCreated(rs.getTimestamp("CREATED"));
        entity.setCreatedBy(rs.getString("CREATED_BY"));
        entity.setDescription(rs.getString("DESCRIPTION"));

        return entity;
    }

}
