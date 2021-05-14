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
package org.apache.nifi.controller.status.history.storage.questdb;

import io.questdb.cairo.TableWriter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.StorageStatus;
import org.apache.nifi.controller.status.history.questdb.QuestDbWritingTemplate;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

class StorageStatusWritingTemplate extends QuestDbWritingTemplate<Pair<Instant, NodeStatus>> {

    public StorageStatusWritingTemplate() {
        super("storageStatus");
    }

    @Override
    protected void addRows(final TableWriter tableWriter, final Collection<Pair<Instant, NodeStatus>> entries) {
        for (final Pair<Instant, NodeStatus> entry : entries) {
            for (final StorageStatus contentRepository : entry.getRight().getContentRepositories()) {
                final long capturedAt = TimeUnit.MILLISECONDS.toMicros(entry.getLeft().toEpochMilli());
                final TableWriter.Row row = tableWriter.newRow(capturedAt);
                row.putSym(1, contentRepository.getName());
                row.putShort(2, Integer.valueOf(0).shortValue());
                row.putLong(3, contentRepository.getFreeSpace());
                row.putLong(4, contentRepository.getUsedSpace());
                row.append();
            }

            for (final StorageStatus provenanceRepository : entry.getRight().getProvenanceRepositories()) {
                final long capturedAt = TimeUnit.MILLISECONDS.toMicros(entry.getLeft().toEpochMilli());
                final TableWriter.Row row = tableWriter.newRow(capturedAt);
                row.putSym(1, provenanceRepository.getName());
                row.putShort(2, Integer.valueOf(1).shortValue());
                row.putLong(3, provenanceRepository.getFreeSpace());
                row.putLong(4, provenanceRepository.getUsedSpace());
                row.append();
            }
        }
    }
}
