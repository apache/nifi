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
import org.apache.nifi.registry.extension.repo.ExtensionRepoArtifact;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;

/**
 * Result for list of bundle artifacts.
 */
public class ExtensionRepoArtifactsResult extends AbstractWritableResult<List<ExtensionRepoArtifact>> {

    private final List<ExtensionRepoArtifact> bundleArtifacts;

    public ExtensionRepoArtifactsResult(final ResultType resultType, final List<ExtensionRepoArtifact> bundleArtifacts) {
        super(resultType);
        this.bundleArtifacts = bundleArtifacts;
        Validate.notNull(this.bundleArtifacts);

        this.bundleArtifacts.sort(
                Comparator.comparing(ExtensionRepoArtifact::getBucketName)
                        .thenComparing(ExtensionRepoArtifact::getGroupId)
                        .thenComparing(ExtensionRepoArtifact::getArtifactId));
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        if (bundleArtifacts.isEmpty()) {
            return;
        }

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Bucket", 40, 400, false)
                .column("Group", 40, 200, false)
                .column("Artifact", 40, 200, false)
                .build();

        for (int i = 0; i < bundleArtifacts.size(); ++i) {
            final ExtensionRepoArtifact artifact = bundleArtifacts.get(i);
            table.addRow(String.valueOf(i + 1), artifact.getBucketName(), artifact.getGroupId(), artifact.getArtifactId());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public List<ExtensionRepoArtifact> getResult() {
        return this.bundleArtifacts;
    }
}
