/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.apache.nifi.web.api.entity.TemplatesEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Result for TemplatesEntity
 */
public class TemplatesResult extends AbstractWritableResult<TemplatesEntity> {

    private final TemplatesEntity templatesEntity;

    public TemplatesResult(final ResultType resultType, final TemplatesEntity templatesEntity) {
        super(resultType);
        this.templatesEntity = templatesEntity;
        Validate.notNull(this.templatesEntity);
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Collection<TemplateEntity> templateEntities = templatesEntity.getTemplates();
        if (templateEntities == null) {
            return;
        }

        final List<TemplateDTO> templateDTOS = templateEntities.stream()
            .map(TemplateEntity::getTemplate)
            .sorted(Comparator.comparing(TemplateDTO::getGroupId))
            .collect(Collectors.toList());

        final Table table = new Table.Builder()
            .column("#", 1, 4, false)
            .column("Name", 5, 40, false)
            .column("ID", 36, 36, false)
            .column("Group ID", 36, 36, false)
            .build();

        for (int i = 0; i < templateDTOS.size(); i++) {
            final TemplateDTO templateDTO = templateDTOS.get(i);
            table.addRow(
                String.valueOf(i + 1),
                templateDTO.getName(),
                templateDTO.getId(),
                templateDTO.getGroupId()
            );
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public TemplatesEntity getResult() {
        return templatesEntity;
    }
}
