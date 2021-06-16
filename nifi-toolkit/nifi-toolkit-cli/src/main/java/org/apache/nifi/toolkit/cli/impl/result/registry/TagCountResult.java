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
import org.apache.nifi.registry.extension.component.TagCount;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

public class TagCountResult extends AbstractWritableResult<List<TagCount>> {

    private final List<TagCount> tagCounts;

    public TagCountResult(ResultType resultType, final List<TagCount> tagCounts) {
        super(resultType);
        this.tagCounts = tagCounts;
        Validate.notNull(this.tagCounts);
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        if (tagCounts.isEmpty()) {
            return;
        }

        final Table table = new Table.Builder()
                .column("Tag", 20, 200, false)
                .column("Count", 5, 20, false)
                .build();

        for (int i = 0; i < tagCounts.size(); ++i) {
            final TagCount tagCount = tagCounts.get(i);
            table.addRow(tagCount.getTag(), String.valueOf(tagCount.getCount()));
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public List<TagCount> getResult() {
        return tagCounts;
    }
}
