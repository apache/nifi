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
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRulesEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Result for FlowAnalysisRulesEntity.
 */
public class FlowAnalysisRulesResult extends AbstractWritableResult<FlowAnalysisRulesEntity> {

    private final FlowAnalysisRulesEntity flowAnalysisRulesEntity;

    public FlowAnalysisRulesResult(final ResultType resultType, final FlowAnalysisRulesEntity flowAnalysisRulesEntity) {
        super(resultType);
        this.flowAnalysisRulesEntity = Objects.requireNonNull(flowAnalysisRulesEntity);
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Set<FlowAnalysisRuleEntity> ruleEntities = flowAnalysisRulesEntity.getFlowAnalysisRules();
        if (ruleEntities == null) {
            return;
        }

        final List<FlowAnalysisRuleDTO> ruleDTOS = ruleEntities.stream()
                .map(FlowAnalysisRuleEntity::getComponent)
                .sorted(Comparator.comparing(FlowAnalysisRuleDTO::getName))
                .collect(Collectors.toList());

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 5, 40, true)
                .column("ID", 36, 36, false)
                .column("Type", 5, 40, true)
                .column("State", 10, 20, false)
                .build();

        for (int i = 0; i < ruleDTOS.size(); i++) {
            final FlowAnalysisRuleDTO ruleDTO = ruleDTOS.get(i);
            final String[] typeSplit = ruleDTO.getType().split("\\.", -1);
            table.addRow(
                    String.valueOf(i + 1),
                    ruleDTO.getName(),
                    ruleDTO.getId(),
                    typeSplit[typeSplit.length - 1],
                    ruleDTO.getState()
            );
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public FlowAnalysisRulesEntity getResult() {
        return flowAnalysisRulesEntity;
    }
}
