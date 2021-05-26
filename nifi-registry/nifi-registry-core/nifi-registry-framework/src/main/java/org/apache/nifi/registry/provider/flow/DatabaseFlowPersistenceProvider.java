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
package org.apache.nifi.registry.provider.flow;

import org.apache.nifi.registry.flow.FlowPersistenceException;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.FlowSnapshotContext;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.apache.nifi.registry.provider.ProviderContext;
import org.apache.nifi.registry.provider.ProviderCreationException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

/**
 * A FlowPersistenceProvider that uses a database table for storage. The intent is to use the same database as the rest
 * of the application so that all data can be stored together and benefit from any replication/scaling of the database.
 */
public class DatabaseFlowPersistenceProvider implements FlowPersistenceProvider {

    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;

    @ProviderContext
    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(this.dataSource);
    }

    @Override
    public void onConfigured(final ProviderConfigurationContext configurationContext) throws ProviderCreationException {
        // there is no config since we get the DataSource from the framework
    }

    @Override
    public void saveFlowContent(final FlowSnapshotContext context, final byte[] content) throws FlowPersistenceException {
        final String sql = "INSERT INTO FLOW_PERSISTENCE_PROVIDER (BUCKET_ID, FLOW_ID, VERSION, FLOW_CONTENT) VALUES (?, ?, ?, ?)";
        jdbcTemplate.update(sql, context.getBucketId(), context.getFlowId(), context.getVersion(), content);
    }

    @Override
    public byte[] getFlowContent(final String bucketId, final String flowId, final int version) throws FlowPersistenceException {
        final List<byte[]> results = new ArrayList<>();
        final String sql = "SELECT FLOW_CONTENT FROM FLOW_PERSISTENCE_PROVIDER WHERE BUCKET_ID = ? and FLOW_ID = ? and VERSION = ?";

        jdbcTemplate.query(sql, new Object[] {bucketId, flowId, version}, (rs) -> {
            final byte[] content = rs.getBytes("FLOW_CONTENT");
            results.add(content);
        });

        if (results.isEmpty()) {
            return null;
        } else {
            return results.get(0);
        }
    }

    @Override
    public void deleteAllFlowContent(final String bucketId, final String flowId) throws FlowPersistenceException {
        final String sql = "DELETE FROM FLOW_PERSISTENCE_PROVIDER WHERE BUCKET_ID = ? and FLOW_ID = ?";
        jdbcTemplate.update(sql, bucketId, flowId);
    }

    @Override
    public void deleteFlowContent(final String bucketId, final String flowId, final int version) throws FlowPersistenceException {
        final String sql = "DELETE FROM FLOW_PERSISTENCE_PROVIDER WHERE BUCKET_ID = ? and FLOW_ID = ? and VERSION = ?";
        jdbcTemplate.update(sql, bucketId, flowId, version);
    }

}
