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
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.toolkit.cli.api.WritableResult;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Result for a VersionedFlowSnapshot.
 *
 * If this result was created with a non-null exportFileName, then the write method will ignore
 * the passed in PrintStream, and will write the serialized snapshot to the give file.
 *
 * If this result was created with a null exportFileName, then the write method will write the
 * serialized snapshot to the given PrintStream.
 */
public class VersionedFlowSnapshotResult implements WritableResult<VersionedFlowSnapshot> {

    private final VersionedFlowSnapshot versionedFlowSnapshot;

    private final String exportFileName;

    public VersionedFlowSnapshotResult(final VersionedFlowSnapshot versionedFlowSnapshot, final String exportFileName) {
        this.versionedFlowSnapshot = versionedFlowSnapshot;
        this.exportFileName = exportFileName;
        Validate.notNull(this.versionedFlowSnapshot);
    }

    @Override
    public VersionedFlowSnapshot getResult() {
        return versionedFlowSnapshot;
    }

    @Override
    public void write(final PrintStream output) throws IOException {
        if (exportFileName != null) {
            try (final OutputStream resultOut = new FileOutputStream(exportFileName)) {
                JacksonUtils.write(versionedFlowSnapshot, resultOut);
            }
        } else {
            JacksonUtils.write(versionedFlowSnapshot, output);
        }
    }
}
