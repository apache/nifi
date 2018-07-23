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
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.api.entity.RegistryClientEntity;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Result for a RegistryClientsEntity.
 */
public class RegistryClientsResult extends AbstractWritableResult<RegistryClientsEntity> {

    final RegistryClientsEntity registryClients;

    public RegistryClientsResult(final ResultType resultType, final RegistryClientsEntity registryClients) {
        super(resultType);
        this.registryClients = registryClients;
        Validate.notNull(this.registryClients);
    }

    @Override
    public RegistryClientsEntity getResult() {
        return this.registryClients;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        final Set<RegistryClientEntity> clients = registryClients.getRegistries();
        if (clients == null || clients.isEmpty()) {
            return;
        }

        final List<RegistryDTO> registries = clients.stream().map(RegistryClientEntity::getComponent)
                .sorted(Comparator.comparing(RegistryDTO::getName))
                .collect(Collectors.toList());

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 20, 36, true)
                .column("Id", 36, 36, false)
                .column("Uri", 3, Integer.MAX_VALUE, false)
                .build();

        for (int i = 0; i < registries.size(); i++) {
            RegistryDTO r = registries.get(i);
            table.addRow("" + (i+1), r.getName(), r.getId(), r.getUri());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

}
