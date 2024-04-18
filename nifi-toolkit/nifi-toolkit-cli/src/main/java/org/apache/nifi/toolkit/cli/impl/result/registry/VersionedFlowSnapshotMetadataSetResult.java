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
package org.apache.nifi.toolkit.cli.impl.result.registry;

import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataSetEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Result for VersionedFlowSnapshotMetadataSetEntity.
 */
public class VersionedFlowSnapshotMetadataSetResult extends AbstractWritableResult<VersionedFlowSnapshotMetadataSetEntity> {

    private final VersionedFlowSnapshotMetadataSetEntity versionedFlowSnapshotMetadataSetEntity;

    public VersionedFlowSnapshotMetadataSetResult(final ResultType resultType,
                                                  final VersionedFlowSnapshotMetadataSetEntity versionedFlowSnapshotMetadataSetEntity) {
        super(resultType);
        this.versionedFlowSnapshotMetadataSetEntity = Objects.requireNonNull(versionedFlowSnapshotMetadataSetEntity);
    }

    @Override
    public VersionedFlowSnapshotMetadataSetEntity getResult() {
        return versionedFlowSnapshotMetadataSetEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Set<VersionedFlowSnapshotMetadataEntity> entities = versionedFlowSnapshotMetadataSetEntity.getVersionedFlowSnapshotMetadataSet();
        if (entities == null || entities.isEmpty()) {
            return;
        }

        // sort by timestamp
        final List<RegisteredFlowSnapshotMetadata> snapshots = entities.stream()
                .map(VersionedFlowSnapshotMetadataEntity::getVersionedFlowSnapshotMetadata)
                .sorted(Comparator.comparing(RegisteredFlowSnapshotMetadata::getTimestamp))
                .toList();

        // date length, with locale specifics
        final String datePattern = "%1$ta, %<tb %<td %<tY %<tR %<tZ";
        final int dateLength = String.format(datePattern, new Date()).length();

        final Table table = new Table.Builder()
                .column("Ver", 3, 3, false)
                .column("Date", dateLength, dateLength, false)
                .column("Author", 20, 200, true)
                .column("Message", 8, 40, true)
                .build();

        snapshots.forEach(vfs -> {
            table.addRow(
                    vfs.getVersion(),
                    String.format(datePattern, new Date(vfs.getTimestamp())),
                    vfs.getAuthor() == null ? "" : vfs.getAuthor(),
                    vfs.getComments() == null ? "" : vfs.getComments()
            );
        });

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

}
