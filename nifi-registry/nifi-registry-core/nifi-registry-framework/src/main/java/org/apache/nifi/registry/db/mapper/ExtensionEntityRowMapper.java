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

import org.apache.nifi.registry.db.entity.ExtensionEntity;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.component.manifest.ExtensionType;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExtensionEntityRowMapper implements RowMapper<ExtensionEntity> {

    @Override
    public ExtensionEntity mapRow(ResultSet rs, int i) throws SQLException {
        final ExtensionEntity entity = new ExtensionEntity();

        // fields from extension table...
        entity.setId(rs.getString("ID"));
        entity.setBundleVersionId(rs.getString("BUNDLE_VERSION_ID"));
        entity.setName(rs.getString("NAME"));
        entity.setDisplayName(rs.getString("DISPLAY_NAME"));
        entity.setExtensionType(ExtensionType.valueOf(rs.getString("TYPE")));
        entity.setContent(rs.getString("CONTENT"));
        entity.setHasAdditionalDetails(rs.getInt("HAS_ADDITIONAL_DETAILS") == 1 ? true : false);

        // fields from joined tables that we know will be there...
        entity.setBucketId(rs.getString("BUCKET_ID"));
        entity.setBucketName(rs.getString("BUCKET_NAME"));
        entity.setBundleId(rs.getString("BUNDLE_ID"));
        entity.setGroupId(rs.getString("GROUP_ID"));
        entity.setArtifactId(rs.getString("ARTIFACT_ID"));
        entity.setVersion(rs.getString("VERSION"));
        entity.setSystemApiVersion(rs.getString("SYSTEM_API_VERSION"));
        entity.setBundleType(BundleType.valueOf(rs.getString("BUNDLE_TYPE")));

        return entity;
    }

}
