/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.toolkit.cli.impl.result.nifi;

import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.entity.NarDetailsEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class NarComponentTypesResult extends AbstractWritableResult<NarDetailsEntity>  {

    private final NarDetailsEntity detailsEntity;

    public NarComponentTypesResult(final ResultType resultType, final NarDetailsEntity detailsEntity) {
        super(resultType);
        this.detailsEntity = detailsEntity;
    }

    @Override
    public NarDetailsEntity getResult() {
        return detailsEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Type", 20, 100, false)
                .column("Kind", 20, 30, false)
                .build();

        final AtomicInteger counter = new AtomicInteger(0);
        writeComponentTypes(detailsEntity.getProcessorTypes(), "PROCESSOR", table, counter);
        writeComponentTypes(detailsEntity.getControllerServiceTypes(), "CONTROLLER_SERVICE", table, counter);
        writeComponentTypes(detailsEntity.getReportingTaskTypes(), "REPORTING_TASK", table, counter);
        writeComponentTypes(detailsEntity.getParameterProviderTypes(), "PARAMETER_PROVIDER", table, counter);
        writeComponentTypes(detailsEntity.getFlowRegistryClientTypes(), "FLOW_REGISTRY_CLIENT", table, counter);
        writeComponentTypes(detailsEntity.getFlowAnalysisRuleTypes(), "FLOW_ANALYSIS_RULE", table, counter);

        if (counter.get() > 0) {
            final TableWriter tableWriter = new DynamicTableWriter();
            tableWriter.write(table, output);
        }
    }

    private void writeComponentTypes(final Set<DocumentedTypeDTO> componentTypes, final String kind, final Table table, final AtomicInteger counter) {
        if (componentTypes == null) {
            return;
        }

        final List<DocumentedTypeDTO> sortedTypes = new ArrayList<>(componentTypes);
        sortedTypes.sort(Comparator.comparing(DocumentedTypeDTO::getType));

        for (final DocumentedTypeDTO documentedType : sortedTypes) {
            table.addRow("" + counter.incrementAndGet(), documentedType.getType(), kind);
        }
    }
}
