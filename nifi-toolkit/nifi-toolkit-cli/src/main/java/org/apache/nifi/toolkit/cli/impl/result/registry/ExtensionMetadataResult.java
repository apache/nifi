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

import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Objects;

public class ExtensionMetadataResult extends AbstractWritableResult<List<ExtensionMetadata>> {

    private List<ExtensionMetadata> extensionMetadata;

    public ExtensionMetadataResult(final ResultType resultType, final List<ExtensionMetadata> extensionMetadata) {
        super(resultType);
        this.extensionMetadata = Objects.requireNonNull(extensionMetadata);
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        if (extensionMetadata.isEmpty()) {
            return;
        }

        final Table table = new Table.Builder()
                .column("Name", 20, 100, false)
                .column("Bucket", 20, 200, false)
                .column("Group", 20, 200, false)
                .column("Artifact", 20, 200, false)
                .column("Version", 8, 40, false)
                .build();

        for (final ExtensionMetadata metadata : extensionMetadata) {
            table.addRow(
                    metadata.getDisplayName(),
                    metadata.getBundleInfo().getBucketName(),
                    metadata.getBundleInfo().getGroupId(),
                    metadata.getBundleInfo().getArtifactId(),
                    metadata.getBundleInfo().getVersion()
            );
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public List<ExtensionMetadata> getResult() {
        return extensionMetadata;
    }
}
