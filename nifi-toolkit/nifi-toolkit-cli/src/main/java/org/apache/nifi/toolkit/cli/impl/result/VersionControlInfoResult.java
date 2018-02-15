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
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;

import java.io.PrintStream;

/**
 * Result for VersionControlInformationEntity.
 */
public class VersionControlInfoResult extends AbstractWritableResult<VersionControlInformationEntity> {

    private final VersionControlInformationEntity versionControlInformationEntity;

    public VersionControlInfoResult(final ResultType resultType,
                                    final VersionControlInformationEntity versionControlInformationEntity) {
        super(resultType);
        this.versionControlInformationEntity = versionControlInformationEntity;
        Validate.notNull(this.versionControlInformationEntity);
    }

    @Override
    public VersionControlInformationEntity getResult() {
        return versionControlInformationEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        final VersionControlInformationDTO dto = versionControlInformationEntity.getVersionControlInformation();
        if (dto == null) {
            return;
        }

        final Table table = new Table.Builder()
                .column("Registry", 20, 30, true)
                .column("Bucket", 20, 30, true)
                .column("Flow", 20, 30, true)
                .column("Ver", 3, 3, false)
                .build();

        table.addRow(
                dto.getRegistryName(),
                dto.getBucketName(),
                dto.getFlowName(),
                String.valueOf(dto.getVersion())
        );

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

}
