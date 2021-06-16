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
import org.apache.nifi.registry.diff.ComponentDifference;
import org.apache.nifi.registry.diff.ComponentDifferenceGroup;
import org.apache.nifi.registry.diff.VersionedFlowDifference;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class VersionedFlowDifferenceResult extends AbstractWritableResult<VersionedFlowDifference> {

    private final VersionedFlowDifference flowDifference;

    public VersionedFlowDifferenceResult(final ResultType resultType, final VersionedFlowDifference flowDifference) {
        super(resultType);
        this.flowDifference = Validate.notNull(flowDifference);
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Component Name", 20, 40, false)
                .column("Change Type", 20, 40, true)
                .column("Difference", 40, 60, true)
                .build();

        final List<ComponentDifferenceGroup> differenceGroups = new ArrayList<>(
                flowDifference.getComponentDifferenceGroups() == null
                        ? Collections.emptyList() : flowDifference.getComponentDifferenceGroups());
        differenceGroups.sort(Comparator.comparing(ComponentDifferenceGroup::getComponentName));

        int differenceCount = 1;
        for (final ComponentDifferenceGroup differenceGroup : differenceGroups) {
            final String componentName = differenceGroup.getComponentName();

            final List<ComponentDifference> componentDifferences = new ArrayList<>(differenceGroup.getDifferences());
            componentDifferences.sort(Comparator.comparing(ComponentDifference::getDifferenceType));

            for (final ComponentDifference componentDifference : componentDifferences) {
                final String changeType = componentDifference.getDifferenceType();
                final String changeDescription = componentDifference.getChangeDescription();
                table.addRow(String.valueOf(differenceCount++), componentName, changeType, changeDescription);
            }
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public VersionedFlowDifference getResult() {
        return flowDifference;
    }
}
