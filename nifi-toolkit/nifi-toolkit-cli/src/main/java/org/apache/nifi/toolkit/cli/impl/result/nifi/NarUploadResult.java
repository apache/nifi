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
import org.apache.nifi.web.api.dto.NarCoordinateDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.NarSummaryEntity;

import java.io.IOException;
import java.io.PrintStream;

public class NarUploadResult extends AbstractWritableResult<NarSummaryEntity> {

    private static final String NAR_COORDINATE_FORMAT = "%s - %s - %s";

    private final NarSummaryEntity narSummaryEntity;

    public NarUploadResult(final ResultType resultType, final NarSummaryEntity narSummaryEntity) {
        super(resultType);
        this.narSummaryEntity = narSummaryEntity;
    }

    @Override
    public NarSummaryEntity getResult() {
        return narSummaryEntity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final NarSummaryDTO summaryDTO = narSummaryEntity.getNarSummary();
        output.println("Identifier: " + summaryDTO.getIdentifier());

        final NarCoordinateDTO coordinateDTO = summaryDTO.getCoordinate();
        final String narCoordinate = getCoordinateString(coordinateDTO);
        output.println("Coordinate: " + narCoordinate);

        final NarCoordinateDTO dependencyCoordinateDTO = summaryDTO.getDependencyCoordinate();
        if (dependencyCoordinateDTO != null) {
            final String dependencyCoordinate = getCoordinateString(dependencyCoordinateDTO);
            output.println("Dependency: " + dependencyCoordinate);
        }

        output.println("State: " + summaryDTO.getState());
    }

    private String getCoordinateString(final NarCoordinateDTO coordinateDTO) {
        return NAR_COORDINATE_FORMAT.formatted(coordinateDTO.getGroup(), coordinateDTO.getArtifact(), coordinateDTO.getVersion());
    }
}
