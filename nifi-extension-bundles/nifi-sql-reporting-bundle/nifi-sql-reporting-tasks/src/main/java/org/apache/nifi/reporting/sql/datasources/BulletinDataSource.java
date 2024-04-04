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

package org.apache.nifi.reporting.sql.datasources;

import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.sql.ColumnSchema;
import org.apache.nifi.sql.IterableRowStream;
import org.apache.nifi.sql.NiFiTableSchema;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.sql.RowStream;

import java.util.List;

public class BulletinDataSource implements ResettableDataSource {
    private static final NiFiTableSchema SCHEMA = new NiFiTableSchema(List.of(
        new ColumnSchema("bulletinId", long.class, false),
        new ColumnSchema("bulletinCategory", String.class, false),
        new ColumnSchema("bulletinGroupId", String.class, true),
        new ColumnSchema("bulletinGroupName", String.class, true),
        new ColumnSchema("bulletinGroupPath", String.class, true),
        new ColumnSchema("bulletinLevel", String.class, false),
        new ColumnSchema("bulletinMessage", String.class, false),
        new ColumnSchema("bulletinNodeAddress", String.class, true),
        new ColumnSchema("bulletinNodeId", String.class, true),
        new ColumnSchema("bulletinSourceId", String.class, true),
        new ColumnSchema("bulletinSourceName", String.class, true),
        new ColumnSchema("bulletinSourceType", String.class, true),
        new ColumnSchema("bulletinTimestamp", long.class, false),
        new ColumnSchema("bulletinFlowFileUuid", String.class, true)
    ));

    private final ReportingContext reportingContext;

    public BulletinDataSource(final ReportingContext reportingContext) {
        this.reportingContext = reportingContext;
    }

    @Override
    public NiFiTableSchema getSchema() {
        return SCHEMA;
    }

    @Override
    public RowStream reset() {
        final boolean isClustered = reportingContext.isClustered();
        String nodeId = "unknown";
        if (isClustered) {
            final String clusterNodeId = reportingContext.getClusterNodeIdentifier();
            if (clusterNodeId != null) {
                nodeId = clusterNodeId;
            }
        }

        final String resolvedNodeId = nodeId;
        final BulletinRepository bulletinRepo = reportingContext.getBulletinRepository();
        final List<Bulletin> bulletins = bulletinRepo.findBulletins(new BulletinQuery.Builder().build());
        return new IterableRowStream<>(bulletins, bulletin -> toArray(bulletin, resolvedNodeId));
    }

    private Object[] toArray(final Bulletin bulletin, final String nodeId) {
        return new Object[] {
            bulletin.getId(),
            bulletin.getCategory(),
            bulletin.getGroupId(),
            bulletin.getGroupName(),
            bulletin.getGroupPath(),
            bulletin.getLevel(),
            bulletin.getMessage(),
            bulletin.getNodeAddress(),
            nodeId,
            bulletin.getSourceId(),
            bulletin.getSourceName(),
            bulletin.getSourceType() == null ? null : bulletin.getSourceType().name(),
            bulletin.getTimestamp() == null ? null : bulletin.getTimestamp().getTime(),
            bulletin.getFlowFileUuid()
        };
    }
}
