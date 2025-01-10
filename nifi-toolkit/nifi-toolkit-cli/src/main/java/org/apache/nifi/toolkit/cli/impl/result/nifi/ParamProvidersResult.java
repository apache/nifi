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
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProvidersEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ParamProvidersResult extends AbstractWritableResult<ParameterProvidersEntity> {

    private final ParameterProvidersEntity parameterProviders;

    public ParamProvidersResult(final ResultType resultType, final ParameterProvidersEntity parameterProviders) {
        super(resultType);
        this.parameterProviders = parameterProviders;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final List<ParameterProviderEntity> parameterProviderEntities = parameterProviders.getParameterProviders().stream()
                .sorted(Comparator.comparing(entity -> entity.getComponent().getName()))
                .collect(Collectors.toList());
        final Table table = new Table.Builder()
                .column("#", 20, 60, false)
                .column("Id", 20, 60, false)
                .column("Name", 20, 60, false)
                .column("Type", 20, 120, false)
                .build();
        for (int i = 0; i < parameterProviders.getParameterProviders().size(); i++) {
            final ParameterProviderEntity parameterProvider = parameterProviderEntities.get(i);
            table.addRow(String.valueOf(i), parameterProvider.getId(), parameterProvider.getComponent().getName(), parameterProvider.getComponent().getType());
        }
        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public ParameterProvidersEntity getResult() {
        return parameterProviders;
    }
}
