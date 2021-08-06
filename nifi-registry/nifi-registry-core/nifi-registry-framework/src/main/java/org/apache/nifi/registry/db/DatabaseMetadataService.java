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
package org.apache.nifi.registry.db;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.BucketItemEntity;
import org.apache.nifi.registry.db.entity.BucketItemEntityType;
import org.apache.nifi.registry.db.entity.BundleEntity;
import org.apache.nifi.registry.db.entity.BundleVersionDependencyEntity;
import org.apache.nifi.registry.db.entity.BundleVersionEntity;
import org.apache.nifi.registry.db.entity.ExtensionAdditionalDetailsEntity;
import org.apache.nifi.registry.db.entity.ExtensionEntity;
import org.apache.nifi.registry.db.entity.ExtensionProvidedServiceApiEntity;
import org.apache.nifi.registry.db.entity.ExtensionRestrictionEntity;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;
import org.apache.nifi.registry.db.entity.TagCountEntity;
import org.apache.nifi.registry.db.mapper.BucketEntityRowMapper;
import org.apache.nifi.registry.db.mapper.BucketItemEntityRowMapper;
import org.apache.nifi.registry.db.mapper.BundleEntityRowMapper;
import org.apache.nifi.registry.db.mapper.BundleVersionDependencyEntityRowMapper;
import org.apache.nifi.registry.db.mapper.BundleVersionEntityRowMapper;
import org.apache.nifi.registry.db.mapper.ExtensionEntityRowMapper;
import org.apache.nifi.registry.db.mapper.FlowEntityRowMapper;
import org.apache.nifi.registry.db.mapper.FlowSnapshotEntityRowMapper;
import org.apache.nifi.registry.db.mapper.TagCountEntityMapper;
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.component.ExtensionFilterParams;
import org.apache.nifi.registry.extension.component.manifest.ExtensionType;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.service.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Repository
public class DatabaseMetadataService implements MetadataService {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public DatabaseMetadataService(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    //----------------- Buckets ---------------------------------

    @Override
    public BucketEntity createBucket(final BucketEntity b) {
        final String sql = "INSERT INTO BUCKET (ID, NAME, DESCRIPTION, CREATED, ALLOW_EXTENSION_BUNDLE_REDEPLOY, ALLOW_PUBLIC_READ) VALUES (?, ?, ?, ?, ?, ?)";
        jdbcTemplate.update(sql,
                b.getId(),
                b.getName(),
                b.getDescription(),
                b.getCreated(),
                b.isAllowExtensionBundleRedeploy() ? 1 : 0,
                b.isAllowPublicRead() ? 1 : 0);
        return b;
    }

    @Override
    public BucketEntity getBucketById(final String bucketIdentifier) {
        final String sql = "SELECT * FROM BUCKET WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, new BucketEntityRowMapper(), bucketIdentifier);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<BucketEntity> getBucketsByName(final String name) {
        final String sql = "SELECT * FROM BUCKET WHERE name = ? ORDER BY name ASC";
        return jdbcTemplate.query(sql, new Object[] {name} , new BucketEntityRowMapper());
    }

    @Override
    public BucketEntity updateBucket(final BucketEntity bucket) {
        final String sql = "UPDATE BUCKET SET " +
                    "name = ?, " +
                    "description = ?, " +
                    "allow_extension_bundle_redeploy = ?, " +
                    "allow_public_read = ? " +
                "WHERE id = ?";

        jdbcTemplate.update(sql,
                bucket.getName(),
                bucket.getDescription(),
                bucket.isAllowExtensionBundleRedeploy() ? 1 : 0,
                bucket.isAllowPublicRead() ? 1 : 0,
                bucket.getId());

        return bucket;
    }

    @Override
    public void deleteBucket(final BucketEntity bucket) {
        // NOTE: Cascading deletes will delete from all child tables
        final String sql = "DELETE FROM BUCKET WHERE id = ?";
        jdbcTemplate.update(sql, bucket.getId());
    }

    @Override
    public List<BucketEntity> getBuckets(final Set<String> bucketIds) {
        if (bucketIds == null || bucketIds.isEmpty()) {
            return Collections.emptyList();
        }

        final StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM BUCKET WHERE ");
        addIdentifiersInClause(sqlBuilder, "id", bucketIds);
        sqlBuilder.append("ORDER BY name ASC");

        return jdbcTemplate.query(sqlBuilder.toString(), bucketIds.toArray(), new BucketEntityRowMapper());
    }

    @Override
    public List<BucketEntity> getAllBuckets() {
        final String sql = "SELECT * FROM BUCKET ORDER BY name ASC";
        return jdbcTemplate.query(sql, new BucketEntityRowMapper());
    }

    //----------------- BucketItems ---------------------------------

    private static final String BASE_BUCKET_ITEMS_SQL =
            "SELECT " +
                "item.id as ID, " +
                "item.name as NAME, " +
                "item.description as DESCRIPTION, " +
                "item.created as CREATED, " +
                "item.modified as MODIFIED, " +
                "item.item_type as ITEM_TYPE, " +
                "b.id as BUCKET_ID, " +
                "b.name as BUCKET_NAME ," +
                "eb.bundle_type as BUNDLE_TYPE, " +
                "eb.group_id as BUNDLE_GROUP_ID, " +
                "eb.artifact_id as BUNDLE_ARTIFACT_ID " +
            "FROM BUCKET_ITEM item " +
            "INNER JOIN BUCKET b ON item.bucket_id = b.id " +
            "LEFT JOIN BUNDLE eb ON item.id = eb.id ";

    @Override
    public List<BucketItemEntity> getBucketItems(final String bucketIdentifier) {
        final String sql = BASE_BUCKET_ITEMS_SQL + " WHERE item.bucket_id = ?";
        final List<BucketItemEntity> items = jdbcTemplate.query(sql, new Object[] { bucketIdentifier }, new BucketItemEntityRowMapper());
        return getItemsWithCounts(items);
    }

    @Override
    public List<BucketItemEntity> getBucketItems(final Set<String> bucketIds) {
        if (bucketIds == null || bucketIds.isEmpty()) {
            return Collections.emptyList();
        }

        final StringBuilder sqlBuilder = new StringBuilder(BASE_BUCKET_ITEMS_SQL + " WHERE item.bucket_id IN (");
        for (int i=0; i < bucketIds.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append("?");
        }
        sqlBuilder.append(")");

        final List<BucketItemEntity> items = jdbcTemplate.query(sqlBuilder.toString(), bucketIds.toArray(), new BucketItemEntityRowMapper());
        return getItemsWithCounts(items);
    }

    private List<BucketItemEntity> getItemsWithCounts(final Iterable<BucketItemEntity> items) {
        final Map<String,Long> snapshotCounts = getFlowSnapshotCounts();
        final Map<String,Long> extensionBundleVersionCounts = getExtensionBundleVersionCounts();

        final List<BucketItemEntity> itemWithCounts = new ArrayList<>();
        for (final BucketItemEntity item : items) {
            if (item.getType() == BucketItemEntityType.FLOW) {
                final Long snapshotCount = snapshotCounts.get(item.getId());
                if (snapshotCount != null) {
                    final FlowEntity flowEntity = (FlowEntity) item;
                    flowEntity.setSnapshotCount(snapshotCount);
                }
            } else if (item.getType() == BucketItemEntityType.BUNDLE) {
                final Long versionCount = extensionBundleVersionCounts.get(item.getId());
                if (versionCount != null) {
                    final BundleEntity bundleEntity = (BundleEntity) item;
                    bundleEntity.setVersionCount(versionCount);
                }
            }

            itemWithCounts.add(item);
        }

        return itemWithCounts;
    }

    private Map<String,Long> getFlowSnapshotCounts() {
        final String sql = "SELECT flow_id, count(*) FROM FLOW_SNAPSHOT GROUP BY flow_id";

        final Map<String,Long> results = new HashMap<>();
        jdbcTemplate.query(sql, (rs) -> {
            results.put(rs.getString(1), rs.getLong(2));
        });
        return results;
    }

    private Long getFlowSnapshotCount(final String flowIdentifier) {
        final String sql = "SELECT count(*) FROM FLOW_SNAPSHOT WHERE flow_id = ?";

        return jdbcTemplate.queryForObject(sql, new Object[] {flowIdentifier}, (rs, num) -> {
            return rs.getLong(1);
        });
    }

    private Map<String,Long> getExtensionBundleVersionCounts() {
        final String sql = "SELECT bundle_id, count(*) FROM BUNDLE_VERSION GROUP BY bundle_id";

        final Map<String,Long> results = new HashMap<>();
        jdbcTemplate.query(sql, (rs) -> {
            results.put(rs.getString(1), rs.getLong(2));
        });
        return results;
    }

    private Long getExtensionBundleVersionCount(final String extensionBundleIdentifier) {
        final String sql = "SELECT count(*) FROM BUNDLE_VERSION WHERE bundle_id = ?";

        return jdbcTemplate.queryForObject(sql, new Object[] {extensionBundleIdentifier}, (rs, num) -> {
            return rs.getLong(1);
        });
    }

    //----------------- Flows ---------------------------------

    @Override
    public FlowEntity createFlow(final FlowEntity flow) {
        final String itemSql = "INSERT INTO BUCKET_ITEM (ID, NAME, DESCRIPTION, CREATED, MODIFIED, ITEM_TYPE, BUCKET_ID) VALUES (?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.update(itemSql,
                flow.getId(),
                flow.getName(),
                flow.getDescription(),
                flow.getCreated(),
                flow.getModified(),
                flow.getType().toString(),
                flow.getBucketId());

        final String flowSql = "INSERT INTO FLOW (ID) VALUES (?)";

        jdbcTemplate.update(flowSql, flow.getId());

        return flow;
    }

    @Override
    public FlowEntity getFlowById(final String flowIdentifier) {
        final String sql = "SELECT * FROM FLOW f, BUCKET_ITEM item WHERE f.id = ? AND item.id = f.id";
        try {
            return jdbcTemplate.queryForObject(sql, new FlowEntityRowMapper(), flowIdentifier);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public FlowEntity getFlowByIdWithSnapshotCounts(final String flowIdentifier) {
        final FlowEntity flowEntity = getFlowById(flowIdentifier);
        if (flowEntity == null) {
            return flowEntity;
        }

        final Long snapshotCount = getFlowSnapshotCount(flowIdentifier);
        if (snapshotCount != null) {
            flowEntity.setSnapshotCount(snapshotCount);
        }

        return flowEntity;
    }

    @Override
    public List<FlowEntity> getFlowsByName(final String name) {
        final String sql = "SELECT * FROM FLOW f, BUCKET_ITEM item WHERE item.name = ? AND item.id = f.id";
        return jdbcTemplate.query(sql, new Object[] {name}, new FlowEntityRowMapper());
    }

    @Override
    public List<FlowEntity> getFlowsByName(final String bucketIdentifier, final String name) {
        final String sql = "SELECT * FROM FLOW f, BUCKET_ITEM item WHERE item.name = ? AND item.id = f.id AND item.bucket_id = ?";
        return jdbcTemplate.query(sql, new Object[] {name, bucketIdentifier}, new FlowEntityRowMapper());
    }

    @Override
    public List<FlowEntity> getFlowsByBucket(final String bucketIdentifier) {
        final String sql = "SELECT * FROM FLOW f, BUCKET_ITEM item WHERE item.bucket_id = ? AND item.id = f.id";
        final List<FlowEntity> flows = jdbcTemplate.query(sql, new Object[] {bucketIdentifier}, new FlowEntityRowMapper());

        final Map<String,Long> snapshotCounts = getFlowSnapshotCounts();
        for (final FlowEntity flowEntity : flows) {
            final Long snapshotCount = snapshotCounts.get(flowEntity.getId());
            if (snapshotCount != null) {
                flowEntity.setSnapshotCount(snapshotCount);
            }
        }

        return flows;
    }

    @Override
    public FlowEntity updateFlow(final FlowEntity flow) {
        flow.setModified(new Date());

        final String sql = "UPDATE BUCKET_ITEM SET name = ?, description = ?, modified = ? WHERE id = ?";
        jdbcTemplate.update(sql, flow.getName(), flow.getDescription(), flow.getModified(), flow.getId());
        return flow;
    }

    @Override
    public void deleteFlow(final FlowEntity flow) {
        // NOTE: Cascading deletes will delete from child tables
        final String itemDeleteSql = "DELETE FROM BUCKET_ITEM WHERE id = ?";
        jdbcTemplate.update(itemDeleteSql, flow.getId());
    }

    //----------------- Flow Snapshots ---------------------------------

    @Override
    public FlowSnapshotEntity createFlowSnapshot(final FlowSnapshotEntity flowSnapshot) {
        final String sql = "INSERT INTO FLOW_SNAPSHOT (FLOW_ID, VERSION, CREATED, CREATED_BY, COMMENTS) VALUES (?, ?, ?, ?, ?)";

        jdbcTemplate.update(sql,
                flowSnapshot.getFlowId(),
                flowSnapshot.getVersion(),
                flowSnapshot.getCreated(),
                flowSnapshot.getCreatedBy(),
                flowSnapshot.getComments());

        return flowSnapshot;
    }

    @Override
    public FlowSnapshotEntity getFlowSnapshot(final String flowIdentifier, final Integer version) {
        final String sql =
                "SELECT " +
                        "fs.flow_id, " +
                        "fs.version, " +
                        "fs.created, " +
                        "fs.created_by, " +
                        "fs.comments " +
                "FROM " +
                        "FLOW_SNAPSHOT fs, " +
                        "FLOW f, " +
                        "BUCKET_ITEM item " +
                "WHERE " +
                        "item.id = f.id AND " +
                        "f.id = ? AND " +
                        "f.id = fs.flow_id AND " +
                        "fs.version = ?";

        try {
            return jdbcTemplate.queryForObject(sql, new FlowSnapshotEntityRowMapper(),
                    flowIdentifier, version);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public FlowSnapshotEntity getLatestSnapshot(final String flowIdentifier) {
        final String sql = "SELECT * FROM FLOW_SNAPSHOT WHERE flow_id = ? ORDER BY version DESC LIMIT 1";

        try {
            return jdbcTemplate.queryForObject(sql, new FlowSnapshotEntityRowMapper(), flowIdentifier);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<FlowSnapshotEntity> getSnapshots(final String flowIdentifier) {
        final String sql =
                "SELECT " +
                        "fs.flow_id, " +
                        "fs.version, " +
                        "fs.created, " +
                        "fs.created_by, " +
                        "fs.comments " +
                "FROM " +
                        "FLOW_SNAPSHOT fs, " +
                        "FLOW f, " +
                        "BUCKET_ITEM item " +
                "WHERE " +
                        "item.id = f.id AND " +
                        "f.id = ? AND " +
                        "f.id = fs.flow_id";

        final Object[] args = new Object[] { flowIdentifier };
        return jdbcTemplate.query(sql, args, new FlowSnapshotEntityRowMapper());
    }

    @Override
    public void deleteFlowSnapshot(final FlowSnapshotEntity flowSnapshot) {
        final String sql = "DELETE FROM FLOW_SNAPSHOT WHERE flow_id = ? AND version = ?";
        jdbcTemplate.update(sql, flowSnapshot.getFlowId(), flowSnapshot.getVersion());
    }

    //----------------- Extension Bundles ---------------------------------

    @Override
    public BundleEntity createBundle(final BundleEntity extensionBundle) {
        final String itemSql =
                "INSERT INTO BUCKET_ITEM (" +
                    "ID, " +
                    "NAME, " +
                    "DESCRIPTION, " +
                    "CREATED, " +
                    "MODIFIED, " +
                    "ITEM_TYPE, " +
                    "BUCKET_ID) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.update(itemSql,
                extensionBundle.getId(),
                extensionBundle.getName(),
                extensionBundle.getDescription(),
                extensionBundle.getCreated(),
                extensionBundle.getModified(),
                extensionBundle.getType().name(),
                extensionBundle.getBucketId());

        final String bundleSql =
                "INSERT INTO BUNDLE (" +
                    "ID, " +
                    "BUCKET_ID, " +
                    "BUNDLE_TYPE, " +
                    "GROUP_ID, " +
                    "ARTIFACT_ID) " +
                "VALUES (?, ?, ?, ?, ?)";

        jdbcTemplate.update(bundleSql,
                extensionBundle.getId(),
                extensionBundle.getBucketId(),
                extensionBundle.getBundleType().name(),
                extensionBundle.getGroupId(),
                extensionBundle.getArtifactId());

        return extensionBundle;
    }

    private static final String BASE_BUNDLE_SQL =
            "SELECT " +
                "item.id as ID," +
                "item.name as NAME, " +
                "item.description as DESCRIPTION, " +
                "item.created as CREATED, " +
                "item.modified as MODIFIED, " +
                "eb.bundle_type as BUNDLE_TYPE, " +
                "eb.group_id as GROUP_ID, " +
                "eb.artifact_id as ARTIFACT_ID, " +
                "b.id as BUCKET_ID, " +
                "b.name as BUCKET_NAME " +
            "FROM " +
                "BUNDLE eb, " +
                "BUCKET_ITEM item," +
                "BUCKET b " +
            "WHERE " +
                "eb.id = item.id AND " +
                "item.bucket_id = b.id";

    @Override
    public BundleEntity getBundle(final String extensionBundleId) {
        final StringBuilder sqlBuilder = new StringBuilder(BASE_BUNDLE_SQL).append(" AND eb.id = ?");
        try {
            final BundleEntity entity = jdbcTemplate.queryForObject(sqlBuilder.toString(), new BundleEntityRowMapper(), extensionBundleId);

            final Long versionCount = getExtensionBundleVersionCount(extensionBundleId);
            if (versionCount != null) {
                entity.setVersionCount(versionCount);
            }

            return entity;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public BundleEntity getBundle(final String bucketId, final String groupId, final String artifactId) {
        final StringBuilder sqlBuilder = new StringBuilder(BASE_BUNDLE_SQL)
                .append(" AND eb.bucket_id = ? ")
                .append("AND eb.group_id = ? ")
                .append("AND eb.artifact_id = ? ");

        try {
            final BundleEntity entity = jdbcTemplate.queryForObject(sqlBuilder.toString(), new BundleEntityRowMapper(), bucketId, groupId, artifactId);

            final Long versionCount = getExtensionBundleVersionCount(entity.getId());
            if (versionCount != null) {
                entity.setVersionCount(versionCount);
            }

            return entity;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<BundleEntity> getBundles(final Set<String> bucketIds, final BundleFilterParams filterParams) {
        if (bucketIds == null || bucketIds.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Object> args = new ArrayList<>();

        final StringBuilder sqlBuilder = new StringBuilder(
                "SELECT " +
                    "item.id as ID, " +
                    "item.name as NAME, " +
                    "item.description as DESCRIPTION, " +
                    "item.created as CREATED, " +
                    "item.modified as MODIFIED, " +
                    "item.item_type as ITEM_TYPE, " +
                    "b.id as BUCKET_ID, " +
                    "b.name as BUCKET_NAME ," +
                    "eb.bundle_type as BUNDLE_TYPE, " +
                    "eb.group_id as GROUP_ID, " +
                    "eb.artifact_id as ARTIFACT_ID " +
                "FROM " +
                    "BUNDLE eb, " +
                    "BUCKET_ITEM item," +
                    "BUCKET b " +
                "WHERE " +
                    "item.id = eb.id AND " +
                    "b.id = item.bucket_id");

        if (filterParams != null) {
            final String bucketName = filterParams.getBucketName();
            if (!StringUtils.isBlank(bucketName)) {
                sqlBuilder.append(" AND b.name LIKE ? ");
                args.add(bucketName);
            }

            final String groupId = filterParams.getGroupId();
            if (!StringUtils.isBlank(groupId)) {
                sqlBuilder.append(" AND eb.group_id LIKE ? ");
                args.add(groupId);
            }

            final String artifactId = filterParams.getArtifactId();
            if (!StringUtils.isBlank(artifactId)) {
                sqlBuilder.append(" AND eb.artifact_id LIKE ? ");
                args.add(artifactId);
            }
        }

        sqlBuilder.append(" AND ");
        addIdentifiersInClause(sqlBuilder, "item.bucket_id", bucketIds);
        sqlBuilder.append("ORDER BY eb.group_id ASC, eb.artifact_id ASC");

        args.addAll(bucketIds);

        final List<BundleEntity> bundleEntities = jdbcTemplate.query(sqlBuilder.toString(), args.toArray(), new BundleEntityRowMapper());
        return populateVersionCounts(bundleEntities);
    }

    @Override
    public List<BundleEntity> getBundlesByBucket(final String bucketId) {
        final StringBuilder sqlBuilder = new StringBuilder(BASE_BUNDLE_SQL)
                .append(" AND b.id = ?")
                .append(" ORDER BY eb.group_id ASC, eb.artifact_id ASC");

        final List<BundleEntity> bundles = jdbcTemplate.query(sqlBuilder.toString(), new Object[]{bucketId}, new BundleEntityRowMapper());
        return populateVersionCounts(bundles);
    }

    @Override
    public List<BundleEntity> getBundlesByBucketAndGroup(String bucketId, String groupId) {
        final StringBuilder sqlBuilder = new StringBuilder(BASE_BUNDLE_SQL)
                .append(" AND b.id = ?")
                .append(" AND eb.group_id = ?")
                .append(" ORDER BY eb.group_id ASC, eb.artifact_id ASC");

        final List<BundleEntity> bundles = jdbcTemplate.query(sqlBuilder.toString(), new Object[]{bucketId, groupId}, new BundleEntityRowMapper());
        return populateVersionCounts(bundles);
    }

    private List<BundleEntity> populateVersionCounts(final List<BundleEntity> bundles) {
        if (!bundles.isEmpty()) {
            final Map<String, Long> versionCounts = getExtensionBundleVersionCounts();
            for (final BundleEntity entity : bundles) {
                final Long versionCount = versionCounts.get(entity.getId());
                if (versionCount != null) {
                    entity.setVersionCount(versionCount);
                }
            }
        }

        return bundles;
    }

    @Override
    public void deleteBundle(final BundleEntity extensionBundle) {
        deleteBundle(extensionBundle.getId());
    }

    @Override
    public void deleteBundle(final String extensionBundleId) {
        // NOTE: All of the foreign key constraints for extension related tables are set to cascade on delete
        final String itemDeleteSql = "DELETE FROM BUCKET_ITEM WHERE id = ?";
        jdbcTemplate.update(itemDeleteSql, extensionBundleId);
    }

    //----------------- Extension Bundle Versions ---------------------------------

    @Override
    public BundleVersionEntity createBundleVersion(final BundleVersionEntity extensionBundleVersion) {
        final String sql =
                "INSERT INTO BUNDLE_VERSION (" +
                    "ID, " +
                    "BUNDLE_ID, " +
                    "VERSION, " +
                    "CREATED, " +
                    "CREATED_BY, " +
                    "DESCRIPTION, " +
                    "SHA_256_HEX, " +
                    "SHA_256_SUPPLIED," +
                    "CONTENT_SIZE, " +
                    "SYSTEM_API_VERSION, " +
                    "BUILD_TOOL, " +
                    "BUILD_FLAGS, " +
                    "BUILD_BRANCH, " +
                    "BUILD_TAG, " +
                    "BUILD_REVISION, " +
                    "BUILT, " +
                    "BUILT_BY" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.update(sql,
                extensionBundleVersion.getId(),
                extensionBundleVersion.getBundleId(),
                extensionBundleVersion.getVersion(),
                extensionBundleVersion.getCreated(),
                extensionBundleVersion.getCreatedBy(),
                extensionBundleVersion.getDescription(),
                extensionBundleVersion.getSha256Hex(),
                extensionBundleVersion.getSha256Supplied() ? 1 : 0,
                extensionBundleVersion.getContentSize(),
                extensionBundleVersion.getSystemApiVersion(),
                extensionBundleVersion.getBuildTool(),
                extensionBundleVersion.getBuildFlags(),
                extensionBundleVersion.getBuildBranch(),
                extensionBundleVersion.getBuildTag(),
                extensionBundleVersion.getBuildRevision(),
                extensionBundleVersion.getBuilt(),
                extensionBundleVersion.getBuiltBy());

        return extensionBundleVersion;
    }

    private static final String BASE_EXTENSION_BUNDLE_VERSION_SQL =
            "SELECT " +
                "ebv.id AS ID," +
                "ebv.bundle_id AS BUNDLE_ID, " +
                "ebv.version AS VERSION, " +
                "ebv.created AS CREATED, " +
                "ebv.created_by AS CREATED_BY, " +
                "ebv.description AS DESCRIPTION, " +
                "ebv.sha_256_hex AS SHA_256_HEX, " +
                "ebv.sha_256_supplied AS SHA_256_SUPPLIED ," +
                "ebv.content_size AS CONTENT_SIZE, " +
                "ebv.system_api_version AS SYSTEM_API_VERSION, " +
                "ebv.build_tool AS BUILD_TOOL, " +
                "ebv.build_flags AS BUILD_FLAGS, " +
                "ebv.build_branch AS BUILD_BRANCH, " +
                "ebv.build_tag AS BUILD_TAG, " +
                "ebv.build_revision AS BUILD_REVISION, " +
                "ebv.built AS BUILT, " +
                "ebv.built_by AS BUILT_BY, " +
                "eb.bucket_id AS BUCKET_ID, " +
                "eb.group_id AS GROUP_ID, " +
                "eb.artifact_id AS ARTIFACT_ID " +
            "FROM BUNDLE eb, BUNDLE_VERSION ebv " +
            "WHERE eb.id = ebv.bundle_id ";

    @Override
    public BundleVersionEntity getBundleVersion(final String extensionBundleId, final String version) {
        final String sql = BASE_EXTENSION_BUNDLE_VERSION_SQL +
                " AND ebv.bundle_id = ? AND ebv.version = ?";
        try {
            return jdbcTemplate.queryForObject(sql, new BundleVersionEntityRowMapper(), extensionBundleId, version);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public BundleVersionEntity getBundleVersion(final String bucketId, final String groupId, final String artifactId, final String version) {
        final String sql = BASE_EXTENSION_BUNDLE_VERSION_SQL +
                    "AND eb.bucket_id = ? " +
                    "AND eb.group_id = ? " +
                    "AND eb.artifact_id = ? " +
                    "AND ebv.version = ?";

        try {
            return jdbcTemplate.queryForObject(sql, new BundleVersionEntityRowMapper(), bucketId, groupId, artifactId, version);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<BundleVersionEntity> getBundleVersions(final Set<String> bucketIdentifiers, final BundleVersionFilterParams filterParams) {
        if (bucketIdentifiers == null || bucketIdentifiers.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Object> args = new ArrayList<>();
        final StringBuilder sqlBuilder = new StringBuilder(BASE_EXTENSION_BUNDLE_VERSION_SQL);

        if (filterParams != null) {
            final String groupId = filterParams.getGroupId();
            if (!StringUtils.isBlank(groupId)) {
                sqlBuilder.append(" AND eb.group_id LIKE ? ");
                args.add(groupId);
            }

            final String artifactId = filterParams.getArtifactId();
            if (!StringUtils.isBlank(artifactId)) {
                sqlBuilder.append(" AND eb.artifact_id LIKE ? ");
                args.add(artifactId);
            }

            final String version = filterParams.getVersion();
            if (!StringUtils.isBlank(version)) {
                sqlBuilder.append(" AND ebv.version LIKE ? ");
                args.add(version);
            }
        }

        sqlBuilder.append(" AND ");
        addIdentifiersInClause(sqlBuilder, "eb.bucket_id", bucketIdentifiers);
        args.addAll(bucketIdentifiers);

        final List<BundleVersionEntity> bundleVersionEntities = jdbcTemplate.query(
                sqlBuilder.toString(), args.toArray(), new BundleVersionEntityRowMapper());

        return bundleVersionEntities;
    }

    private void addIdentifiersInClause(StringBuilder sqlBuilder, String idFieldName, Set<String> identifiers) {
        sqlBuilder.append(idFieldName).append(" IN (");
        for (int i = 0; i < identifiers.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append("?");
        }
        sqlBuilder.append(") ");
    }

    @Override
    public List<BundleVersionEntity> getBundleVersions(final String extensionBundleId) {
        final String sql = BASE_EXTENSION_BUNDLE_VERSION_SQL + " AND ebv.bundle_id = ?";
        return jdbcTemplate.query(sql, new Object[]{extensionBundleId}, new BundleVersionEntityRowMapper());
    }

    @Override
    public List<BundleVersionEntity> getBundleVersions(final String bucketId, final String groupId, final String artifactId) {
        final String sql = BASE_EXTENSION_BUNDLE_VERSION_SQL +
                    "AND eb.bucket_id = ? " +
                    "AND eb.group_id = ? " +
                    "AND eb.artifact_id = ? ";

        final Object[] args = {bucketId, groupId, artifactId};
        return jdbcTemplate.query(sql, args, new BundleVersionEntityRowMapper());
    }

    @Override
    public List<BundleVersionEntity> getBundleVersionsGlobal(final String groupId, final String artifactId, final String version) {
        final String sql = BASE_EXTENSION_BUNDLE_VERSION_SQL +
                "AND eb.group_id = ? " +
                "AND eb.artifact_id = ? " +
                "AND ebv.version = ?";

        final Object[] args = {groupId, artifactId, version};
        return jdbcTemplate.query(sql, args, new BundleVersionEntityRowMapper());
    }

    @Override
    public void deleteBundleVersion(final BundleVersionEntity extensionBundleVersion) {
        deleteBundleVersion(extensionBundleVersion.getId());
    }

    @Override
    public void deleteBundleVersion(final String extensionBundleVersionId) {
        // NOTE: All of the foreign key constraints for extension related tables are set to cascade on delete
        final String sql = "DELETE FROM BUNDLE_VERSION WHERE id = ?";
        jdbcTemplate.update(sql, extensionBundleVersionId);
    }

    //------------ Extension Bundle Version Dependencies ------------

    @Override
    public BundleVersionDependencyEntity createDependency(final BundleVersionDependencyEntity dependencyEntity) {
        final String dependencySql =
                "INSERT INTO BUNDLE_VERSION_DEPENDENCY (" +
                    "ID, " +
                    "BUNDLE_VERSION_ID, " +
                    "GROUP_ID, " +
                    "ARTIFACT_ID, " +
                    "VERSION " +
                ") VALUES (?, ?, ?, ?, ?)";

        jdbcTemplate.update(dependencySql,
                dependencyEntity.getId(),
                dependencyEntity.getExtensionBundleVersionId(),
                dependencyEntity.getGroupId(),
                dependencyEntity.getArtifactId(),
                dependencyEntity.getVersion());

        return dependencyEntity;
    }

    @Override
    public List<BundleVersionDependencyEntity> getDependenciesForBundleVersion(final String extensionBundleVersionId) {
        final String sql = "SELECT * FROM BUNDLE_VERSION_DEPENDENCY WHERE bundle_version_id = ?";
        final Object[] args = {extensionBundleVersionId};
        return jdbcTemplate.query(sql, args, new BundleVersionDependencyEntityRowMapper());
    }


    //----------------- Extensions ---------------------------------

    private static String BASE_EXTENSION_SQL =
            "SELECT " +
                "e.id AS ID, " +
                "e.bundle_version_id AS BUNDLE_VERSION_ID, " +
                "e.name AS NAME, " +
                "e.display_name AS DISPLAY_NAME, " +
                "e.type AS TYPE, " +
                "e.content AS CONTENT," +
                "e.has_additional_details AS HAS_ADDITIONAL_DETAILS, " +
                "eb.id AS BUNDLE_ID, " +
                "eb.group_id AS GROUP_ID, " +
                "eb.artifact_id AS ARTIFACT_ID, " +
                "eb.bundle_type AS BUNDLE_TYPE, " +
                "ebv.version AS VERSION, " +
                "ebv.system_api_version AS SYSTEM_API_VERSION, " +
                "b.id AS BUCKET_ID, " +
                "b.name as BUCKET_NAME " +
            "FROM " +
                    "EXTENSION e, " +
                    "BUNDLE_VERSION ebv, " +
                    "BUNDLE eb," +
                    "BUCKET b " +
            "WHERE " +
                    "e.bundle_version_id = ebv.id AND " +
                    "ebv.bundle_id = eb.id AND " +
                    "eb.bucket_id = b.id ";

    @Override
    public ExtensionEntity createExtension(final ExtensionEntity extension) {
        final String insertExtensionSql =
                "INSERT INTO EXTENSION (" +
                    "ID, " +
                    "BUNDLE_VERSION_ID, " +
                    "NAME, " +
                    "DISPLAY_NAME, " +
                    "TYPE, " +
                    "CONTENT, " +
                    "ADDITIONAL_DETAILS, " +
                    "HAS_ADDITIONAL_DETAILS " +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.update(insertExtensionSql,
                extension.getId(),
                extension.getBundleVersionId(),
                extension.getName(),
                extension.getDisplayName(),
                extension.getExtensionType().name(),
                extension.getContent(),
                extension.getAdditionalDetails(),
                extension.getAdditionalDetails() != null ? 1 : 0
        );

        // insert tags...
        final String insertTagSql = "INSERT INTO EXTENSION_TAG (EXTENSION_ID, TAG) VALUES (?, ?);";

        final Set<String> tags = extension.getTags();
        if (tags != null) {
            for (final String tag : tags) {
                if (tag != null) {
                    final String normalizedTag = tag.trim().toLowerCase();
                    if (!normalizedTag.isEmpty()) {
                        jdbcTemplate.update(insertTagSql, extension.getId(), normalizedTag);
                    }
                }
            }
        }

        // insert provided service APIs...
        final Set<ExtensionProvidedServiceApiEntity> providedServiceApis = extension.getProvidedServiceApis();
        if (providedServiceApis != null) {
            providedServiceApis.forEach(p -> createProvidedServiceApi(p));
        }

        // insert restrictions...
        final Set<ExtensionRestrictionEntity> restrictions = extension.getRestrictions();
        if (restrictions != null) {
            restrictions.forEach(r -> createRestriction(r));
        }

        return extension;
    }

    @Override
    public ExtensionEntity getExtensionById(final String id) {
        final String selectSql = BASE_EXTENSION_SQL + " AND e.id = ?";
        try {
            return jdbcTemplate.queryForObject(selectSql, new ExtensionEntityRowMapper(), id);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public ExtensionEntity getExtensionByName(final String bundleVersionId, final String name) {
        final String selectSql = BASE_EXTENSION_SQL + " AND e.bundle_version_id = ? AND e.name = ?";
        try {
            return jdbcTemplate.queryForObject(selectSql, new ExtensionEntityRowMapper(), bundleVersionId, name);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public ExtensionAdditionalDetailsEntity getExtensionAdditionalDetails(final String bundleVersionId, final String name) {
        final String selectSql = "SELECT id, additional_details FROM EXTENSION WHERE bundle_version_id = ? AND name = ?";
        try {
            final Object[] args = {bundleVersionId, name};
            return jdbcTemplate.queryForObject(selectSql, args, (rs, i) -> {
                final ExtensionAdditionalDetailsEntity entity = new ExtensionAdditionalDetailsEntity();
                entity.setExtensionId(rs.getString("ID"));
                entity.setAdditionalDetails(Optional.ofNullable(rs.getString("ADDITIONAL_DETAILS")));
                return entity;
            });
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<ExtensionEntity> getExtensions(final Set<String> bucketIdentifiers, final ExtensionFilterParams filterParams) {
        if (bucketIdentifiers == null || bucketIdentifiers.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Object> args = new ArrayList<>();

        final StringBuilder sqlBuilder = new StringBuilder(BASE_EXTENSION_SQL);
        sqlBuilder.append(" AND ");
        addIdentifiersInClause(sqlBuilder, "eb.bucket_id", bucketIdentifiers);
        args.addAll(bucketIdentifiers);

        if (filterParams != null) {
            final BundleType bundleType = filterParams.getBundleType();
            if (bundleType != null) {
                sqlBuilder.append(" AND eb.bundle_type = ?");
                args.add(bundleType.name());
            }

            final ExtensionType extensionType = filterParams.getExtensionType();
            if (extensionType != null) {
                sqlBuilder.append(" AND e.type = ?");
                args.add(extensionType.name());
            }

            final Collection<String> tags = filterParams.getTags();
            if (tags != null && !tags.isEmpty()) {
                sqlBuilder.append(" AND e.id IN (")
                        .append(" SELECT et.extension_id FROM EXTENSION_TAG et WHERE ");

                boolean first = true;
                for (final String tag : tags) {
                    if (!first) {
                        sqlBuilder.append(" OR ");
                    }
                    sqlBuilder.append(" et.tag = ? ");
                    args.add(tag.trim().toLowerCase());
                    first = false;
                }

                sqlBuilder.append(")");
            }
        }

        sqlBuilder.append(" ORDER BY e.name ASC");
        return jdbcTemplate.query(sqlBuilder.toString(), args.toArray(), new ExtensionEntityRowMapper());
    }

    @Override
    public List<ExtensionEntity> getExtensionsByProvidedServiceApi(final Set<String> bucketIdentifiers, final ProvidedServiceAPI providedServiceAPI) {
        if (bucketIdentifiers == null || bucketIdentifiers.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Object> args = new ArrayList<>();

        final StringBuilder sqlBuilder = new StringBuilder(BASE_EXTENSION_SQL);
        sqlBuilder.append(" AND ");
        addIdentifiersInClause(sqlBuilder, "eb.bucket_id", bucketIdentifiers);
        args.addAll(bucketIdentifiers);

        sqlBuilder.append(" AND e.id IN (")
                .append(" SELECT ep.extension_id FROM EXTENSION_PROVIDED_SERVICE_API ep")
                .append(" WHERE ep.class_name = ? ")
                .append(" AND ep.group_id = ? ")
                .append(" AND ep.artifact_id = ? ")
                .append(" AND ep.version = ?")
                .append(")")
                .toString();

        args.add(providedServiceAPI.getClassName());
        args.add(providedServiceAPI.getGroupId());
        args.add(providedServiceAPI.getArtifactId());
        args.add(providedServiceAPI.getVersion());

        return jdbcTemplate.query(sqlBuilder.toString(), args.toArray(), new ExtensionEntityRowMapper());
    }

    @Override
    public List<ExtensionEntity> getExtensionsByBundleVersionId(final String bundleVersionId) {
        final String selectSql = BASE_EXTENSION_SQL + " AND e.bundle_version_id = ?";
        final Object[] args = { bundleVersionId };
        return jdbcTemplate.query(selectSql, args, new ExtensionEntityRowMapper());
    }

    @Override
    public List<TagCountEntity> getAllExtensionTags() {
        final String selectSql =
                "SELECT tag as TAG, count(*) as COUNT " +
                "FROM EXTENSION_TAG " +
                "GROUP BY tag " +
                "ORDER BY tag ASC";

        return jdbcTemplate.query(selectSql, new TagCountEntityMapper());
    }

    @Override
    public void deleteExtension(final ExtensionEntity extension) {
        // NOTE: All of the foreign key constraints for extension related tables are set to cascade on delete
        final String deleteSql = "DELETE FROM EXTENSION WHERE id = ?";
        jdbcTemplate.update(deleteSql, extension.getId());
    }

    //----------------- Extension Provided Service APIs --------------------

    private ExtensionProvidedServiceApiEntity createProvidedServiceApi(final ExtensionProvidedServiceApiEntity providedServiceApi) {
        final String sql =
                "INSERT INTO EXTENSION_PROVIDED_SERVICE_API (" +
                    "ID, " +
                    "EXTENSION_ID, " +
                    "CLASS_NAME, " +
                    "GROUP_ID, " +
                    "ARTIFACT_ID, " +
                    "VERSION) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        jdbcTemplate.update(sql,
                providedServiceApi.getId(),
                providedServiceApi.getExtensionId(),
                providedServiceApi.getClassName(),
                providedServiceApi.getGroupId(),
                providedServiceApi.getArtifactId(),
                providedServiceApi.getVersion()
        );

        return providedServiceApi;
    }

    //----------------- Extension Restrictions --------------------

    private ExtensionRestrictionEntity createRestriction(final ExtensionRestrictionEntity restriction) {
        final String sql =
                "INSERT INTO EXTENSION_RESTRICTION (" +
                    "ID, " +
                    "EXTENSION_ID, " +
                    "REQUIRED_PERMISSION, " +
                    "EXPLANATION) " +
                "VALUES (?, ?, ?, ?)";

        jdbcTemplate.update(sql,
                restriction.getId(),
                restriction.getExtensionId(),
                restriction.getRequiredPermission(),
                restriction.getExplanation());

        return restriction;
    }

    //----------------- Fields ---------------------------------

    @Override
    public Set<String> getBucketFields() {
        final Set<String> fields = new LinkedHashSet<>();
        fields.add("ID");
        fields.add("NAME");
        fields.add("DESCRIPTION");
        fields.add("CREATED");
        return fields;
    }

    @Override
    public Set<String> getBucketItemFields() {
        final Set<String> fields = new LinkedHashSet<>();
        fields.add("ID");
        fields.add("NAME");
        fields.add("DESCRIPTION");
        fields.add("CREATED");
        fields.add("MODIFIED");
        fields.add("ITEM_TYPE");
        fields.add("BUCKET_ID");
        return fields;
    }

    @Override
    public Set<String> getFlowFields() {
        final Set<String> fields = new LinkedHashSet<>();
        fields.add("ID");
        fields.add("NAME");
        fields.add("DESCRIPTION");
        fields.add("CREATED");
        fields.add("MODIFIED");
        fields.add("ITEM_TYPE");
        fields.add("BUCKET_ID");
        return fields;
    }
}
