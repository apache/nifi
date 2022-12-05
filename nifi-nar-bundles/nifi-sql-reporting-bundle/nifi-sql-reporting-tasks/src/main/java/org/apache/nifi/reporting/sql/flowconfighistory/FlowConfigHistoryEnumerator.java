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

package org.apache.nifi.reporting.sql.flowconfighistory;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.details.ConfigureDetails;
import org.apache.nifi.action.details.ConnectDetails;
import org.apache.nifi.action.details.MoveDetails;
import org.apache.nifi.action.details.PurgeDetails;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.ReportingContext;

import java.util.Iterator;
import java.util.List;

public class FlowConfigHistoryEnumerator implements Enumerator<Object> {
    private final ReportingContext context;
    private final ComponentLog logger;
    private final int[] fields;

    private Iterator<Action> actionIterator;
    private Object currentRow;
    private int recordsRead = 0;

    public FlowConfigHistoryEnumerator(final ReportingContext context, final ComponentLog logger, final int[] fields) {
        this.context = context;
        this.logger = logger;
        this.fields = fields;
        reset();
    }

    @Override
    public Object current() {
        return currentRow;
    }

    @Override
    public boolean moveNext() {
        currentRow = null;

        if (!actionIterator.hasNext()) {
            // If we are out of data, close the InputStream. We do this because
            // Calcite does not necessarily call our close() method.
            close();
            try {
                onFinish();
            } catch (final Exception e) {
                logger.error("Failed to perform tasks when enumerator was finished", e);
            }

            return false;
        }

        final Action action = actionIterator.next();
        currentRow = filterColumns(action);

        recordsRead++;
        return true;
    }

    protected int getRecordsRead() {
        return recordsRead;
    }

    protected void onFinish() {
    }

    private Object filterColumns(final Action action) {
        if (action == null) {
            return null;
        }

        final boolean isClustered = context.isClustered();
        String nodeId = context.getClusterNodeIdentifier();
        if (nodeId == null && isClustered) {
            nodeId = "unknown";
        }

        final Object[] row = new Object[]{
                action.getId(),
                action.getTimestamp().getTime(),
                action.getUserIdentity(),
                action.getSourceId(),
                action.getSourceName(),
                action.getSourceType(),
                action.getOperation().toString(),
                action.getActionDetails() instanceof ConfigureDetails ? ((ConfigureDetails) action.getActionDetails()).getName() : null,
                action.getActionDetails() instanceof ConfigureDetails ? ((ConfigureDetails) action.getActionDetails()).getPreviousValue() : null,
                action.getActionDetails() instanceof ConfigureDetails ? ((ConfigureDetails) action.getActionDetails()).getValue() : null,
                action.getActionDetails() instanceof ConnectDetails ? ((ConnectDetails) action.getActionDetails()).getSourceId() : null,
                action.getActionDetails() instanceof ConnectDetails ? ((ConnectDetails) action.getActionDetails()).getSourceName() : null,
                action.getActionDetails() instanceof ConnectDetails ? ((ConnectDetails) action.getActionDetails()).getSourceType() : null,
                action.getActionDetails() instanceof ConnectDetails ? ((ConnectDetails) action.getActionDetails()).getDestinationId() : null,
                action.getActionDetails() instanceof ConnectDetails ? ((ConnectDetails) action.getActionDetails()).getDestinationName() : null,
                action.getActionDetails() instanceof ConnectDetails ? ((ConnectDetails) action.getActionDetails()).getDestinationType() : null,
                action.getActionDetails() instanceof ConnectDetails ? ((ConnectDetails) action.getActionDetails()).getRelationship() : null,
                action.getActionDetails() instanceof MoveDetails ? ((MoveDetails) action.getActionDetails()).getGroup() : null,
                action.getActionDetails() instanceof MoveDetails ? ((MoveDetails) action.getActionDetails()).getGroupId() : null,
                action.getActionDetails() instanceof MoveDetails ? ((MoveDetails) action.getActionDetails()).getPreviousGroup() : null,
                action.getActionDetails() instanceof MoveDetails ? ((MoveDetails) action.getActionDetails()).getPreviousGroupId() : null,
                action.getActionDetails() instanceof PurgeDetails ? ((PurgeDetails) action.getActionDetails()).getEndDate().getTime() : null
        };

        // If we want no fields just return null
        if (fields == null) {
            return row;
        }

        // If we want only a single field, then Calcite is going to expect us to return
        // the actual value, NOT a 1-element array of values.
        if (fields.length == 1) {
            final int desiredCellIndex = fields[0];
            return row[desiredCellIndex];
        }

        // Create a new Object array that contains only the desired fields.
        final Object[] filtered = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            final int indexToKeep = fields[i];
            filtered[i] = row[indexToKeep];
        }

        return filtered;
    }

    @Override
    public void reset() {
        List<Action> fullFlowConfigHistoryList = context.getEventAccess().getFlowChanges(0, Short.MAX_VALUE);
        actionIterator = fullFlowConfigHistoryList.iterator();
    }

    @Override
    public void close() {
    }
}
