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
package org.apache.nifi.controller.status.history.questdb;

import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.StorageStatus;
import org.apache.nifi.questdb.InsertRowContext;
import org.apache.nifi.questdb.InsertRowDataSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

final class StorageStatusDataSource implements InsertRowDataSource {
    private final Iterator<StorageStatusStatistic> statuses;

    private StorageStatusDataSource(final Iterator<StorageStatusStatistic> statuses) {
        this.statuses = statuses;
    }

    @Override
    public boolean hasNextToInsert() {
        return statuses.hasNext();
    }

    @Override
    public void fillRowData(final InsertRowContext context) {
        final StorageStatusStatistic status = statuses.next();

        context.initializeRow(status.getCaptured());
        context.addString(1, status.getStatus().getName());
        context.addShort(2, status.getType().getId());
        context.addLong(3, status.getStatus().getFreeSpace());
        context.addLong(4, status.getStatus().getUsedSpace());
    }

    static InsertRowDataSource getInstance(final Collection<CapturedStatus<NodeStatus>> nodeStatuses) {
        final List<StorageStatusStatistic> statuses = new ArrayList<>();
        for (final CapturedStatus<NodeStatus> nodeStatus : nodeStatuses) {
            final Instant captured = nodeStatus.getCaptured();
            nodeStatus.getStatus().getContentRepositories().forEach(storageStatus -> statuses.add(new StorageStatusStatistic(storageStatus, StorageStatusType.CONTENT, captured)));
            nodeStatus.getStatus().getProvenanceRepositories().forEach(storageStatus -> statuses.add(new StorageStatusStatistic(storageStatus, StorageStatusType.PROVENANCE, captured)));
        }

        return new StorageStatusDataSource(statuses.iterator());
    }

    private static class StorageStatusStatistic {
        private final StorageStatus status;
        private final StorageStatusType type;
        private final Instant captured;

        private StorageStatusStatistic(final StorageStatus status, final StorageStatusType type, final Instant captured) {
            this.status = status;
            this.type = type;
            this.captured = captured;
        }

        public StorageStatus getStatus() {
            return status;
        }

        public StorageStatusType getType() {
            return type;
        }

        public Instant getCaptured() {
            return captured;
        }
    }
}
