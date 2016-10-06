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

package org.apache.nifi.cluster.coordination.http.endpoints;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.entity.DropRequestEntity;

public class DropRequestEndpointMerger extends AbstractSingleDTOEndpoint<DropRequestEntity, DropRequestDTO> {
    public static final Pattern DROP_REQUESTS_URI = Pattern.compile("/nifi-api/flowfile-queues/[a-f0-9\\-]{36}/drop-requests");
    public static final Pattern DROP_REQUEST_URI = Pattern.compile("/nifi-api/flowfile-queues/[a-f0-9\\-]{36}/drop-requests/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(URI uri, String method) {
        if (("GET".equalsIgnoreCase(method) || "DELETE".equalsIgnoreCase(method)) && DROP_REQUEST_URI.matcher(uri.getPath()).matches()) {
            return true;
        } else if (("POST".equalsIgnoreCase(method) && DROP_REQUESTS_URI.matcher(uri.getPath()).matches())) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<DropRequestEntity> getEntityClass() {
        return DropRequestEntity.class;
    }

    @Override
    protected DropRequestDTO getDto(DropRequestEntity entity) {
        return entity.getDropRequest();
    }

    @Override
    protected void mergeResponses(DropRequestDTO clientDto, Map<NodeIdentifier, DropRequestDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        boolean nodeWaiting = false;
        int originalCount = 0;
        long originalSize = 0;
        int currentCount = 0;
        long currentSize = 0;
        int droppedCount = 0;
        long droppedSize = 0;

        DropFlowFileState state = null;
        boolean allFinished = true;
        String failureReason = null;
        for (final Map.Entry<NodeIdentifier, DropRequestDTO> nodeEntry : dtoMap.entrySet()) {
            final DropRequestDTO nodeDropRequest = nodeEntry.getValue();

            if (!nodeDropRequest.isFinished()) {
                allFinished = false;
            }
            if (nodeDropRequest.getFailureReason() != null) {
                failureReason = nodeDropRequest.getFailureReason();
            }

            currentCount += nodeDropRequest.getCurrentCount();
            currentSize += nodeDropRequest.getCurrentSize();
            droppedCount += nodeDropRequest.getDroppedCount();
            droppedSize += nodeDropRequest.getDroppedSize();

            if (nodeDropRequest.getOriginalCount() == null) {
                nodeWaiting = true;
            } else {
                originalCount += nodeDropRequest.getOriginalCount();
                originalSize += nodeDropRequest.getOriginalSize();
            }

            final DropFlowFileState nodeState = DropFlowFileState.valueOfDescription(nodeDropRequest.getState());
            if (state == null || state.ordinal() > nodeState.ordinal()) {
                state = nodeState;
            }
        }

        clientDto.setCurrentCount(currentCount);
        clientDto.setCurrentSize(currentSize);
        clientDto.setCurrent(FormatUtils.formatCount(currentCount) + " / " + FormatUtils.formatDataSize(currentSize));

        clientDto.setDroppedCount(droppedCount);
        clientDto.setDroppedSize(droppedSize);
        clientDto.setDropped(FormatUtils.formatCount(droppedCount) + " / " + FormatUtils.formatDataSize(droppedSize));

        clientDto.setFinished(allFinished);
        clientDto.setFailureReason(failureReason);
        if (originalCount == 0) {
            clientDto.setPercentCompleted(allFinished ? 100 : 0);
        } else {
            clientDto.setPercentCompleted((int) ((double) droppedCount / (double) originalCount * 100D));
        }

        if (!nodeWaiting) {
            clientDto.setOriginalCount(originalCount);
            clientDto.setOriginalSize(originalSize);
            clientDto.setOriginal(FormatUtils.formatCount(originalCount) + " / " + FormatUtils.formatDataSize(originalSize));
        }

        if (state != null) {
            clientDto.setState(state.toString());
        }
    }

}
