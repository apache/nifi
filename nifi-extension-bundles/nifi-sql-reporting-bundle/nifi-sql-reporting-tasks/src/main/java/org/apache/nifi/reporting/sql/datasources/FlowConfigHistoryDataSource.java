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

import org.apache.nifi.action.Action;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.ConfigureDetails;
import org.apache.nifi.action.details.ConnectDetails;
import org.apache.nifi.action.details.MoveDetails;
import org.apache.nifi.action.details.PurgeDetails;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.sql.ColumnSchema;
import org.apache.nifi.sql.IterableRowStream;
import org.apache.nifi.sql.NiFiTableSchema;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.sql.RowStream;

import java.util.List;

public class FlowConfigHistoryDataSource implements ResettableDataSource {
    public static final NiFiTableSchema SCHEMA = new NiFiTableSchema(List.of(
        new ColumnSchema("actionId", int.class, false),
        new ColumnSchema("actionTimestamp", long.class, false),
        new ColumnSchema("actionUserIdentity", String.class, false),
        new ColumnSchema("actionSourceId", String.class, false),
        new ColumnSchema("actionSourceName", String.class, false),
        new ColumnSchema("actionSourceType", String.class, false),
        new ColumnSchema("actionOperation", String.class, false),
        new ColumnSchema("configureDetailsName", String.class, false),
        new ColumnSchema("configureDetailsPreviousValue", String.class, false),
        new ColumnSchema("configureDetailsValue", String.class, false),
        new ColumnSchema("connectionSourceId", String.class, false),
        new ColumnSchema("connectionSourceName", String.class, false),
        new ColumnSchema("connectionSourceType", String.class, false),
        new ColumnSchema("connectionDestinationId", String.class, false),
        new ColumnSchema("connectionDestinationName", String.class, false),
        new ColumnSchema("connectionDestinationType", String.class, false),
        new ColumnSchema("connectionRelationship", String.class, false),
        new ColumnSchema("moveGroup", String.class, false),
        new ColumnSchema("moveGroupId", String.class, false),
        new ColumnSchema("movePreviousGroup", String.class, false),
        new ColumnSchema("movePreviousGroupId", String.class, false),
        new ColumnSchema("purgeEndDate", long.class, false)
    ));

    private final ReportingContext reportingContext;

    public FlowConfigHistoryDataSource(final ReportingContext reportingContext) {
        this.reportingContext = reportingContext;
    }

    @Override
    public NiFiTableSchema getSchema() {
        return SCHEMA;
    }

    @Override
    public RowStream reset() {
        final List<Action> fullFlowConfigHistoryList = reportingContext.getEventAccess().getFlowChanges(0, Short.MAX_VALUE);
        return new IterableRowStream<>(fullFlowConfigHistoryList, this::toArray);
    }

    private Object[] toArray(final Action action) {
        final ActionDetails details = action.getActionDetails();

        return new Object[] {
            action.getId(),
            action.getTimestamp().getTime(),
            action.getUserIdentity(),
            action.getSourceId(),
            action.getSourceName(),
            action.getSourceType(),
            action.getOperation().toString(),
            details instanceof ConfigureDetails ? ((ConfigureDetails) details).getName() : null,
            details instanceof ConfigureDetails ? ((ConfigureDetails) details).getPreviousValue() : null,
            details instanceof ConfigureDetails ? ((ConfigureDetails) details).getValue() : null,
            details instanceof ConnectDetails ? ((ConnectDetails) details).getSourceId() : null,
            details instanceof ConnectDetails ? ((ConnectDetails) details).getSourceName() : null,
            details instanceof ConnectDetails ? ((ConnectDetails) details).getSourceType() : null,
            details instanceof ConnectDetails ? ((ConnectDetails) details).getDestinationId() : null,
            details instanceof ConnectDetails ? ((ConnectDetails) details).getDestinationName() : null,
            details instanceof ConnectDetails ? ((ConnectDetails) details).getDestinationType() : null,
            details instanceof ConnectDetails ? ((ConnectDetails) details).getRelationship() : null,
            details instanceof MoveDetails ? ((MoveDetails) details).getGroup() : null,
            details instanceof MoveDetails ? ((MoveDetails) details).getGroupId() : null,
            details instanceof MoveDetails ? ((MoveDetails) details).getPreviousGroup() : null,
            details instanceof MoveDetails ? ((MoveDetails) details).getPreviousGroupId() : null,
            details instanceof PurgeDetails ? ((PurgeDetails) details).getEndDate().getTime() : null
        };
    }
}
