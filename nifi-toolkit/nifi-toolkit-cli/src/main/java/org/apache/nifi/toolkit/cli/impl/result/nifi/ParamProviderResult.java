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
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ParamProviderResult extends AbstractWritableResult<ParameterProviderEntity> {

    private final ParameterProviderEntity parameterProvider;

    public ParamProviderResult(final ResultType resultType, final ParameterProviderEntity parameterProvider) {
        super(resultType);
        this.parameterProvider = parameterProvider;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final ParameterProviderDTO parameterProviderDTO = parameterProvider.getComponent();

        final Set<String> fetchedParameterNames = parameterProviderDTO.getFetchedParameterNames();
        final List<String> sortedParameterNames = fetchedParameterNames == null ? Collections.emptyList() : new ArrayList<>(parameterProviderDTO.getFetchedParameterNames());
        Collections.sort(sortedParameterNames);

        final Table propertiesTable = new Table.Builder()
                .column("Property Name", 20, 60, false)
                .column("Property Value", 20, 80, false)
                .build();
        if (parameterProviderDTO.getProperties() != null && !parameterProviderDTO.getProperties().isEmpty()) {
            parameterProviderDTO.getProperties().forEach((name, value) -> propertiesTable.addRow(new String[] {name, value}));
        }
        final Table fetchedParametersTable = new Table.Builder()
                .column("Fetched Parameter Name", 20, 60, false)
                .build();
        if (!sortedParameterNames.isEmpty()) {
            sortedParameterNames.forEach(param -> fetchedParametersTable.addRow(new String[] {param}));
        }

        final TableWriter tableWriter = new DynamicTableWriter();

        if (!propertiesTable.getRows().isEmpty()) {
            tableWriter.write(propertiesTable, output);
        }
        if (!fetchedParametersTable.getRows().isEmpty()) {
            tableWriter.write(fetchedParametersTable, output);
        }
    }

    @Override
    public ParameterProviderEntity getResult() {
        return parameterProvider;
    }
}
