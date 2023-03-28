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

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.toolkit.cli.api.WritableResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Result for a list of VersionedFlowSnapshots.
 *
 * If this result was created with a non-null exportDirectoryName, then the write method will ignore
 * the passed in PrintStream, and will write the serialized snapshot to the given directory.
 * The file name will be generated from the flow name and its version.
 *
 * If this result was created with a null exportDirectoryName, then the write method will write the
 * serialized snapshots to the given PrintStream.
 */
public class VersionedFlowSnapshotsResult implements WritableResult<List<VersionedFlowSnapshot>> {
    private static final String FILE_NAME_PREFIX = "toolkit_registry_export_all_";
    private static final String EXPORT_FILE_NAME = "%s/%s_%s_%d";
    private final List<VersionedFlowSnapshot> versionedFlowSnapshots;
    private final String exportDirectoryName;

    public VersionedFlowSnapshotsResult(final List<VersionedFlowSnapshot> versionedFlowSnapshots, final String exportDirectoryName) {
        this.versionedFlowSnapshots = versionedFlowSnapshots;
        this.exportDirectoryName = exportDirectoryName;
        Validate.notNull(this.versionedFlowSnapshots);
    }

    @Override
    public List<VersionedFlowSnapshot> getResult() {
        return versionedFlowSnapshots;
    }

    @Override
    public void write(final PrintStream output) throws IOException {
        for (final VersionedFlowSnapshot versionedFlowSnapshot : versionedFlowSnapshots) {
            if (exportDirectoryName != null) {
                final String flowId = versionedFlowSnapshot.getFlow().getIdentifier();
                final int version = versionedFlowSnapshot.getSnapshotMetadata().getVersion();
                final String exportFileName = String.format(EXPORT_FILE_NAME, exportDirectoryName, FILE_NAME_PREFIX, flowId, version);
                try (final OutputStream resultOut = Files.newOutputStream(Paths.get(exportFileName))) {
                    JacksonUtils.write(versionedFlowSnapshot, resultOut);
                }
            } else {
                JacksonUtils.write(versionedFlowSnapshots, output);
            }
        }
    }
}
