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

import org.apache.nifi.registry.db.entity.BucketItemEntity;
import org.apache.nifi.registry.db.entity.BucketItemEntityType;
import org.apache.nifi.registry.db.entity.BundleEntity;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BucketItemEntityRowMapper implements RowMapper<BucketItemEntity> {

    @Nullable
    @Override
    public BucketItemEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        final BucketItemEntityType type = BucketItemEntityType.valueOf(rs.getString("ITEM_TYPE"));

        // Create the appropriate type of sub-class, eventually populate specific data for each type
        final BucketItemEntity item;
        switch (type) {
            case FLOW:
                item = new FlowEntity();
                break;
            case BUNDLE:
                final BundleEntity bundleEntity = new BundleEntity();
                bundleEntity.setBundleType(BundleType.valueOf(rs.getString("BUNDLE_TYPE")));
                bundleEntity.setGroupId(rs.getString("BUNDLE_GROUP_ID"));
                bundleEntity.setArtifactId(rs.getString("BUNDLE_ARTIFACT_ID"));
                item = bundleEntity;
                break;
            default:
                // should never happen
                item = new BucketItemEntity();
                break;
        }

        // populate fields common to all bucket items
        item.setId(rs.getString("ID"));
        item.setName(rs.getString("NAME"));
        item.setDescription(rs.getString("DESCRIPTION"));
        item.setCreated(rs.getTimestamp("CREATED"));
        item.setModified(rs.getTimestamp("MODIFIED"));
        item.setBucketId(rs.getString("BUCKET_ID"));
        item.setBucketName(rs.getString("BUCKET_NAME"));
        item.setType(type);
        return item;
    }
}
