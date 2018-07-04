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
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Result for ControllerServicesEntity.
 */
public class ControllerServicesResult extends AbstractWritableResult<ControllerServicesEntity> {

    private final ControllerServicesEntity controllerServicesEntity;

    public ControllerServicesResult(final ResultType resultType, final ControllerServicesEntity controllerServicesEntity) {
        super(resultType);
        this.controllerServicesEntity = controllerServicesEntity;
        Validate.notNull(this.controllerServicesEntity);
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Set<ControllerServiceEntity> serviceEntities = controllerServicesEntity.getControllerServices();
        if (serviceEntities == null) {
            return;
        }

        final List<ControllerServiceDTO> serviceDTOS = serviceEntities.stream()
                .map(s -> s.getComponent())
                .collect(Collectors.toList());

        Collections.sort(serviceDTOS, Comparator.comparing(ControllerServiceDTO::getName));

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 5, 40, false)
                .column("State", 5, 40, false)
                .build();

        for (int i=0; i < serviceDTOS.size(); i++) {
            final ControllerServiceDTO serviceDTO = serviceDTOS.get(i);
            table.addRow(String.valueOf(i+1), serviceDTO.getName(), serviceDTO.getState());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public ControllerServicesEntity getResult() {
        return controllerServicesEntity;
    }
}
