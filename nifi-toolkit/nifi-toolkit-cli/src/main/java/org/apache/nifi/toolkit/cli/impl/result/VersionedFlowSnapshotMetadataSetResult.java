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
package org.apache.nifi.toolkit.cli.impl.result;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.api.WritableResult;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataSetEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Result for VersionedFlowSnapshotMetadataSetEntity.
 */
public class VersionedFlowSnapshotMetadataSetResult extends AbstractWritableResult<VersionedFlowSnapshotMetadataSetEntity> {

    private final VersionedFlowSnapshotMetadataSetEntity versionedFlowSnapshotMetadataSetEntity;

    public VersionedFlowSnapshotMetadataSetResult(final ResultType resultType,
                                                  final VersionedFlowSnapshotMetadataSetEntity versionedFlowSnapshotMetadataSetEntity) {
        super(resultType);
        this.versionedFlowSnapshotMetadataSetEntity = versionedFlowSnapshotMetadataSetEntity;
        Validate.notNull(this.versionedFlowSnapshotMetadataSetEntity);
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

        // this will be sorted by the child result below
        final List<VersionedFlowSnapshotMetadata> snapshots = entities.stream()
                .map(v -> v.getVersionedFlowSnapshotMetadata())
                .collect(Collectors.toList());

        final WritableResult<List<VersionedFlowSnapshotMetadata>> result = new VersionedFlowSnapshotMetadataResult(resultType, snapshots);
        result.write(output);
    }
}
