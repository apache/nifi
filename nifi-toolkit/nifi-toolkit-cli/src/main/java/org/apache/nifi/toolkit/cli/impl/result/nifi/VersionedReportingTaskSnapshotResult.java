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
package org.apache.nifi.toolkit.cli.impl.result.nifi;

import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.toolkit.cli.api.WritableResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Objects;

/**
 * Result for a VersionedReportingTaskSnapshot.
 *
 * If this result was created with a non-null exportFileName, then the write method will ignore
 * the passed in PrintStream, and will write the serialized snapshot to the give file.
 *
 * If this result was created with a null exportFileName, then the write method will write the
 * serialized snapshot to the given PrintStream.
 */
public class VersionedReportingTaskSnapshotResult implements WritableResult<VersionedReportingTaskSnapshot> {

    private final VersionedReportingTaskSnapshot versionedReportingTaskSnapshot;
    private final String exportFileName;

    public VersionedReportingTaskSnapshotResult(final VersionedReportingTaskSnapshot versionedReportingTaskSnapshot, final String exportFileName) {
        this.versionedReportingTaskSnapshot = Objects.requireNonNull(versionedReportingTaskSnapshot);
        this.exportFileName = exportFileName;
    }

    @Override
    public VersionedReportingTaskSnapshot getResult() {
        return versionedReportingTaskSnapshot;
    }

    @Override
    public void write(final PrintStream output) throws IOException {
        if (exportFileName != null) {
            try (final OutputStream resultOut = new FileOutputStream(exportFileName)) {
                JacksonUtils.write(versionedReportingTaskSnapshot, resultOut);
            }
        } else {
            JacksonUtils.write(versionedReportingTaskSnapshot, output);
        }
    }
}
