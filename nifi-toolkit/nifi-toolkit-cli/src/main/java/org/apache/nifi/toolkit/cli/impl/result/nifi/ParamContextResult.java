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
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ParamContextResult extends AbstractWritableResult<ParameterContextEntity> {

    private final ParameterContextEntity parameterContext;

    public ParamContextResult(final ResultType resultType, final ParameterContextEntity parameterContext) {
        super(resultType);
        this.parameterContext = parameterContext;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final ParameterContextDTO parameterContextDTO = parameterContext.getComponent();

        final Set<ParameterEntity> paramEntities = parameterContextDTO.getParameters() == null
                ? Collections.emptySet() : parameterContextDTO.getParameters();

        final Set<ParameterDTO> paramDTOs = paramEntities.stream()
                .map(p -> p.getParameter())
                .collect(Collectors.toSet());

        final List<ParameterDTO> sortedParams =paramDTOs.stream()
                .sorted(Comparator.comparing(ParameterDTO::getName))
                .collect(Collectors.toList());

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 20, 60, false)
                .column("Value", 20, 80, false)
                .column("Sensitive", 10, 10, false)
                .column("Description", 20, 80, true)
                .build();

        for (int i = 0; i < sortedParams.size(); i++) {
            final ParameterDTO r = sortedParams.get(i);
            table.addRow(String.valueOf(i+1), r.getName(), r.getValue(), r.getSensitive().toString(), r.getDescription());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public ParameterContextEntity getResult() {
        return parameterContext;
    }
}
