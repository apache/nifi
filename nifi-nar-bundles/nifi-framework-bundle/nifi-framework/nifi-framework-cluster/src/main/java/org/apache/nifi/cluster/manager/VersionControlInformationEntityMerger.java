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

package org.apache.nifi.cluster.manager;

import java.util.Map;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;

public class VersionControlInformationEntityMerger {

    public void merge(final VersionControlInformationEntity clientEntity, final Map<NodeIdentifier, VersionControlInformationEntity> entityMap) {

        final VersionControlInformationDTO clientDto = clientEntity.getVersionControlInformation();

        // We need to merge the 'current' and 'modified' flags because these are updated by nodes in the background. Since
        // the nodes can synchronize with the Flow Registry at different intervals, we have to determine how to handle these
        // flags if different nodes report different values for them.
        entityMap.values().stream()
            .filter(entity -> entity != clientEntity)
            .forEach(entity -> {
                final VersionControlInformationDTO dto = entity.getVersionControlInformation();

                updateFlowState(clientDto, dto);
            });
    }


    private static boolean isCurrent(final VersionedFlowState state) {
        return state == VersionedFlowState.UP_TO_DATE || state == VersionedFlowState.LOCALLY_MODIFIED;
    }

    private static boolean isModified(final VersionedFlowState state) {
        return state == VersionedFlowState.LOCALLY_MODIFIED || state == VersionedFlowState.LOCALLY_MODIFIED_AND_STALE;
    }

    public static void updateFlowState(final VersionControlInformationDTO clientDto, final VersionControlInformationDTO dto) {
        final VersionedFlowState clientState = VersionedFlowState.valueOf(clientDto.getState());
        if (clientState == VersionedFlowState.SYNC_FAILURE) {
            return;
        }

        final VersionedFlowState dtoState = VersionedFlowState.valueOf(dto.getState());
        if (dtoState == VersionedFlowState.SYNC_FAILURE) {
            clientDto.setState(dto.getState());
            clientDto.setStateExplanation(dto.getStateExplanation());
            return;
        }

        final boolean clientCurrent = isCurrent(clientState);
        final boolean clientModified = isModified(clientState);

        final boolean dtoCurrent = isCurrent(dtoState);
        final boolean dtoModified = isModified(dtoState);

        final boolean current = clientCurrent && dtoCurrent;
        final boolean stale = !current;
        final boolean modified = clientModified && dtoModified;

        final VersionedFlowState flowState;
        if (modified && stale) {
            flowState = VersionedFlowState.LOCALLY_MODIFIED_AND_STALE;
        } else if (modified) {
            flowState = VersionedFlowState.LOCALLY_MODIFIED;
        } else if (stale) {
            flowState = VersionedFlowState.STALE;
        } else {
            flowState = VersionedFlowState.UP_TO_DATE;
        }

        clientDto.setState(flowState.name());
        clientDto.setStateExplanation(flowState.getDescription());
    }
}
