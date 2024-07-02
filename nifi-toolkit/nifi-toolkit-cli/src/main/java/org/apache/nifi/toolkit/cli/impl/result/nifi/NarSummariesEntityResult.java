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
import org.apache.nifi.web.api.dto.NarCoordinateDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

public class NarSummariesEntityResult extends AbstractWritableResult<NarSummariesEntity> {

    private final NarSummariesEntity summariesEntity;

    public NarSummariesEntityResult(final ResultType resultType, final NarSummariesEntity summariesEntity) {
        super(resultType);
        this.summariesEntity = summariesEntity;
    }

    @Override
    public NarSummariesEntity getResult() {
        return summariesEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Collection<NarSummaryEntity> summaries = summariesEntity.getNarSummaries();
        if (summaries == null || summaries.isEmpty()) {
            return;
        }

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Id", 36, 36, false)
                .column("Group", 20, 36, false)
                .column("Artifact", 36, 36, false)
                .column("Version", 6, 20, false)
                .column("State", 6, 20, false)
                .build();

        int count = 0;
        for (final NarSummaryEntity summary : summaries) {
            final NarSummaryDTO summaryDTO = summary.getNarSummary();
            final NarCoordinateDTO coordinateDTO = summaryDTO.getCoordinate();
            final String identifier = summaryDTO.getIdentifier();
            table.addRow("" + (++count), identifier, coordinateDTO.getGroup(), coordinateDTO.getArtifact(), coordinateDTO.getVersion(), summaryDTO.getState());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }
}
