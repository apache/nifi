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
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportResponseEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ImportReportingTasksResult extends AbstractWritableResult<VersionedReportingTaskImportResponseEntity> {

    private static final String REPORTING_TASK_TYPE = "REPORTING_TASK";
    private static final String CONTROLLER_SERVICE_TYPE = "CONTROLLER_SERVICE";

    private final VersionedReportingTaskImportResponseEntity responseEntity;

    public ImportReportingTasksResult(final ResultType resultType, final VersionedReportingTaskImportResponseEntity responseEntity) {
        super(resultType);
        this.responseEntity = responseEntity;
    }

    @Override
    public VersionedReportingTaskImportResponseEntity getResult() {
        return responseEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Set<ReportingTaskEntity> tasksEntities = responseEntity.getReportingTasks();
        final Set<ControllerServiceEntity> serviceEntities = responseEntity.getControllerServices();
        if (tasksEntities == null || serviceEntities == null) {
            return;
        }

        final List<ReportingTaskDTO> taskDTOS = tasksEntities.stream()
                .map(ReportingTaskEntity::getComponent)
                .sorted(Comparator.comparing(ReportingTaskDTO::getName))
                .collect(Collectors.toList());

        final List<ControllerServiceDTO> serviceDTOS = serviceEntities.stream()
                .map(ControllerServiceEntity::getComponent)
                .sorted(Comparator.comparing(ControllerServiceDTO::getName))
                .collect(Collectors.toList());

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 5, 40, true)
                .column("ID", 36, 36, false)
                .column("Type", 15, 20, true)
                .build();

        int componentCount = 0;
        for (final ReportingTaskDTO taskDTO : taskDTOS) {
            table.addRow(String.valueOf(++componentCount), taskDTO.getName(), taskDTO.getId(), REPORTING_TASK_TYPE);
        }
        for (final ControllerServiceDTO serviceDTO : serviceDTOS) {
            table.addRow(String.valueOf(++componentCount), serviceDTO.getName(), serviceDTO.getId(), CONTROLLER_SERVICE_TYPE);
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }
}
