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
import org.apache.nifi.web.api.entity.ParameterContextsEntity;
import org.apache.nifi.web.api.entity.ParameterGroupConfigurationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ParamProviderResult extends AbstractWritableResult<ParameterProviderEntity> {

    private final ParameterProviderEntity parameterProvider;
    private final ParameterContextsEntity parameterContexts;

    public ParamProviderResult(final ResultType resultType, final ParameterProviderEntity parameterProvider,
            final ParameterContextsEntity paramContextEntity) {
        super(resultType);
        this.parameterProvider = parameterProvider;
        this.parameterContexts = paramContextEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final ParameterProviderDTO parameterProviderDTO = parameterProvider.getComponent();

        final Collection<ParameterGroupConfigurationEntity> fetchedParameterNameGroups = parameterProviderDTO.getParameterGroupConfigurations();
        final List<ParameterGroupConfigurationEntity> sortedParameterNameGroups = fetchedParameterNameGroups == null
                ? Collections.emptyList() : new ArrayList<>(parameterProviderDTO.getParameterGroupConfigurations());
        Collections.sort(sortedParameterNameGroups);

        final Table propertiesTable = new Table.Builder()
                .column("Property Name", 20, 60, false)
                .column("Property Value", 20, 80, false)
                .build();
        if (parameterProviderDTO.getProperties() != null && !parameterProviderDTO.getProperties().isEmpty()) {
            parameterProviderDTO.getProperties().forEach((name, value) -> propertiesTable.addRow(name, value));
        }
        final Table fetchedParametersTable = new Table.Builder()
                .column("Parameter Group", 20, 60, false)
                .column("Parameter Context Id", 36, 36, false)
                .column("Parameter Context Name", 20, 60, false)
                .column("Fetched Parameter Name", 20, 60, false)
                .build();
        if (!sortedParameterNameGroups.isEmpty()) {
            sortedParameterNameGroups.forEach(group -> {
                group.getParameterSensitivities().keySet().stream().sorted()
                        .forEach(param -> fetchedParametersTable.addRow(
                                group.getGroupName(),
                                getParameterContextId(group.getParameterContextName()),
                                group.getParameterContextName(),
                                param
                        ));
            });
        }

        final TableWriter tableWriter = new DynamicTableWriter();

        if (!propertiesTable.getRows().isEmpty()) {
            tableWriter.write(propertiesTable, output);
        }
        if (!fetchedParametersTable.getRows().isEmpty()) {
            tableWriter.write(fetchedParametersTable, output);
        }
    }

    private String getParameterContextId(final String name) {
        return parameterContexts.getParameterContexts()
                .stream()
                .filter(t -> t.getComponent().getName().equals(name))
                .findFirst()
                .get()
                .getComponent()
                .getId();
    }

    @Override
    public ParameterProviderEntity getResult() {
        return parameterProvider;
    }
}
