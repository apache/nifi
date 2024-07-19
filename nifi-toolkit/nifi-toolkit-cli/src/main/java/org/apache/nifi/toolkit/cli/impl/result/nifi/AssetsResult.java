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
import org.apache.nifi.web.api.dto.AssetDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class AssetsResult extends AbstractWritableResult<AssetsEntity> {

    private final AssetsEntity assetsEntity;

    public AssetsResult(final ResultType resultType, final AssetsEntity assetsEntity) {
        super(resultType);
        this.assetsEntity = Objects.requireNonNull(assetsEntity);
    }

    @Override
    public AssetsEntity getResult() {
        return assetsEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Collection<AssetEntity> assetEntities = assetsEntity.getAssets();
        if (assetEntities == null) {
            return;
        }

        final List<AssetDTO> assetDTOS = assetEntities.stream()
                .map(AssetEntity::getAsset)
                .sorted(Comparator.comparing(AssetDTO::getName))
                .toList();

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("ID", 36, 36, false)
                .column("Name", 5, 40, false)
                .build();

        for (int i = 0; i < assetDTOS.size(); i++) {
            final AssetDTO assetDTO = assetDTOS.get(i);
            table.addRow(String.valueOf(i + 1), assetDTO.getId(), assetDTO.getName());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }
}
