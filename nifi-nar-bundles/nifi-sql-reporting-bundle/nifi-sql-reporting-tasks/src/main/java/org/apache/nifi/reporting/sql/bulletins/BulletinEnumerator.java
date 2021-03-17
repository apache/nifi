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

package org.apache.nifi.reporting.sql.bulletins;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.ReportingContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BulletinEnumerator implements Enumerator<Object> {
    private final ReportingContext context;
    private final ComponentLog logger;
    private final int[] fields;

    private Iterator<Bulletin> bulletinIterator;
    private Object currentRow;
    private int recordsRead = 0;

    public BulletinEnumerator(final ReportingContext context, final ComponentLog logger, final int[] fields) {
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

        if (!bulletinIterator.hasNext()) {
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

        final Bulletin bulletin = bulletinIterator.next();
        currentRow = filterColumns(bulletin);

        recordsRead++;
        return true;
    }

    protected int getRecordsRead() {
        return recordsRead;
    }

    protected void onFinish() {
    }

    private Object filterColumns(final Bulletin bulletin) {
        if (bulletin == null) {
            return null;
        }

        final boolean isClustered = context.isClustered();
        String nodeId = context.getClusterNodeIdentifier();
        if (nodeId == null && isClustered) {
            nodeId = "unknown";
        }

        final Object[] row = new Object[]{
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
                bulletin.getTimestamp() == null ? null : bulletin.getTimestamp().getTime()
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
        BulletinRepository bulletinRepo = context.getBulletinRepository();
        List<Bulletin> fullBulletinList = new ArrayList<>(bulletinRepo.findBulletinsForController());
        fullBulletinList.addAll(bulletinRepo.findBulletins((new BulletinQuery.Builder()).sourceType(ComponentType.PROCESSOR).build()));
        fullBulletinList.addAll(bulletinRepo.findBulletins((new BulletinQuery.Builder()).sourceType(ComponentType.INPUT_PORT).build()));
        fullBulletinList.addAll(bulletinRepo.findBulletins((new BulletinQuery.Builder()).sourceType(ComponentType.OUTPUT_PORT).build()));
        fullBulletinList.addAll(bulletinRepo.findBulletins((new BulletinQuery.Builder()).sourceType(ComponentType.REMOTE_PROCESS_GROUP).build()));
        fullBulletinList.addAll(bulletinRepo.findBulletins((new BulletinQuery.Builder()).sourceType(ComponentType.REPORTING_TASK).build()));
        fullBulletinList.addAll(bulletinRepo.findBulletins((new BulletinQuery.Builder()).sourceType(ComponentType.CONTROLLER_SERVICE).build()));

        bulletinIterator = fullBulletinList.iterator();
    }

    @Override
    public void close() {
    }
}
