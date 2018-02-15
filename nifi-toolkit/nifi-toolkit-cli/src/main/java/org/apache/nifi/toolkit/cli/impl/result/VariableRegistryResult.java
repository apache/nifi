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
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;
import org.apache.nifi.web.api.dto.VariableDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Result for a VariableRegistryEntity.
 */
public class VariableRegistryResult extends AbstractWritableResult<VariableRegistryEntity> {

    final VariableRegistryEntity variableRegistryEntity;

    public VariableRegistryResult(final ResultType resultType, final VariableRegistryEntity variableRegistryEntity) {
        super(resultType);
        this.variableRegistryEntity = variableRegistryEntity;
        Validate.notNull(this.variableRegistryEntity);
    }

    @Override
    public VariableRegistryEntity getResult() {
        return variableRegistryEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        final VariableRegistryDTO variableRegistryDTO = variableRegistryEntity.getVariableRegistry();
        if (variableRegistryDTO == null || variableRegistryDTO.getVariables() == null) {
            return;
        }

        final List<VariableDTO> variables = variableRegistryDTO.getVariables().stream()
                .map(v -> v.getVariable()).collect(Collectors.toList());
        Collections.sort(variables, Comparator.comparing(VariableDTO::getName));

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 5, 40, false)
                .column("Value", 5, 40, false)
                .build();

        for (int i=0; i < variables.size(); i++) {
            final VariableDTO var = variables.get(i);
            table.addRow(String.valueOf(i+1), var.getName(), var.getValue());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

}
