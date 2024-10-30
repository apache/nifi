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
import org.apache.nifi.web.api.dto.VersionedFlowDTO;
import org.apache.nifi.web.api.entity.VersionedFlowEntity;
import org.apache.nifi.web.api.entity.VersionedFlowsEntity;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Result for a VersionedFlowsEntity.
 */
public class RegistryFlowsResult extends AbstractWritableResult<VersionedFlowsEntity> {

    final VersionedFlowsEntity flowsEntity;

    public RegistryFlowsResult(final ResultType resultType, final VersionedFlowsEntity flowsEntity) {
        super(resultType);
        this.flowsEntity = Objects.requireNonNull(flowsEntity);
    }

    @Override
    public VersionedFlowsEntity getResult() {
        return this.flowsEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        final Set<VersionedFlowEntity> flows = flowsEntity.getVersionedFlows();
        if (flows == null || flows.isEmpty()) {
            return;
        }

        final List<VersionedFlowDTO> flowsDTO = flows.stream()
            .map(f -> f.getVersionedFlow())
            .sorted(Comparator.comparing(VersionedFlowDTO::getFlowName))
            .toList();

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 20, 36, true)
                .column("Id", 36, 36, false)
                .column("Description", 11, 40, true)
                .build();

        for (int i = 0; i < flowsDTO.size(); i++) {
            VersionedFlowDTO flow = flowsDTO.get(i);
            table.addRow("" + (i + 1), flow.getFlowName(), flow.getFlowId(), flow.getDescription() == null ? "" : flow.getDescription());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

}
