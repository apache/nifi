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
import org.apache.nifi.registry.extension.repo.ExtensionRepoGroup;
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
 * Result for list of bundle groups.
 */
public class ExtensionRepoGroupsResult extends AbstractWritableResult<List<ExtensionRepoGroup>> {

    private final List<ExtensionRepoGroup> bundleGroups;

    public ExtensionRepoGroupsResult(final ResultType resultType, final List<ExtensionRepoGroup> bundleGroups) {
        super(resultType);
        this.bundleGroups = bundleGroups;
        Validate.notNull(this.bundleGroups);

        this.bundleGroups.sort(
                Comparator.comparing(ExtensionRepoGroup::getBucketName)
                        .thenComparing(ExtensionRepoGroup::getGroupId));
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        if (bundleGroups.isEmpty()) {
            return;
        }

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Bucket", 40, 400, false)
                .column("Group", 40, 200, false)
                .build();

        for (int i = 0; i < bundleGroups.size(); ++i) {
            final ExtensionRepoGroup group = bundleGroups.get(i);
            table.addRow(String.valueOf(i + 1), group.getBucketName(), group.getGroupId());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public List<ExtensionRepoGroup> getResult() {
        return this.bundleGroups;
    }
}
