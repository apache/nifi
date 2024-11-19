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

import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;
import org.apache.nifi.web.api.dto.FlowRegistryBranchDTO;
import org.apache.nifi.web.api.entity.FlowRegistryBranchEntity;
import org.apache.nifi.web.api.entity.FlowRegistryBranchesEntity;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Result for a FlowRegistryBranchesEntity.
 */
public class RegistryBranchesResult extends AbstractWritableResult<FlowRegistryBranchesEntity> {

    final FlowRegistryBranchesEntity branchesEntity;

    public RegistryBranchesResult(final ResultType resultType, final FlowRegistryBranchesEntity branchesEntity) {
        super(resultType);
        this.branchesEntity = Objects.requireNonNull(branchesEntity);
    }

    @Override
    public FlowRegistryBranchesEntity getResult() {
        return this.branchesEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        final Set<FlowRegistryBranchEntity> branches = branchesEntity.getBranches();
        if (branches == null || branches.isEmpty()) {
            return;
        }

        final List<FlowRegistryBranchDTO> branchesDTO = branches.stream()
            .map(b -> b.getBranch())
            .sorted(Comparator.comparing(FlowRegistryBranchDTO::getName))
            .toList();

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 20, 36, true)
                .build();

        for (int i = 0; i < branchesDTO.size(); i++) {
            FlowRegistryBranchDTO branch = branchesDTO.get(i);
            table.addRow("" + (i + 1), branch.getName());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

}
