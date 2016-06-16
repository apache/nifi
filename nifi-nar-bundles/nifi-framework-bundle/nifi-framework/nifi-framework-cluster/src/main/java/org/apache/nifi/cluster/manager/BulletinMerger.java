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

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.BulletinDTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class BulletinMerger {

    private BulletinMerger() {}

    /**
     * Merges the validation errors.
     *
     * @param bulletins bulletins
     */
    public static List<BulletinDTO> mergeBulletins(final Map<NodeIdentifier, List<BulletinDTO>> bulletins) {
        final List<BulletinDTO> bulletinDtos = new ArrayList<>();

        for (final Map.Entry<NodeIdentifier, List<BulletinDTO>> entry : bulletins.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final List<BulletinDTO> nodeBulletins = entry.getValue();
            final String nodeAddress = nodeId.getApiAddress() + ":" + nodeId.getApiPort();

            for (final BulletinDTO bulletin : nodeBulletins) {
                bulletin.setNodeAddress(nodeAddress);
                bulletinDtos.add(bulletin);
            }
        }

        Collections.sort(bulletinDtos, (BulletinDTO o1, BulletinDTO o2) -> {
            final int timeComparison = o1.getTimestamp().compareTo(o2.getTimestamp());
            if (timeComparison != 0) {
                return timeComparison;
            }

            return o1.getNodeAddress().compareTo(o2.getNodeAddress());
        });

        return bulletinDtos;
    }

    /**
     * Normalizes the validation errors.
     *
     * @param validationErrorMap validation errors for each node
     * @param totalNodes total number of nodes
     * @return the normalized validation errors
     */
    public static Set<String> normalizedMergedValidationErrors(final Map<String, Set<NodeIdentifier>> validationErrorMap, int totalNodes) {
        final Set<String> normalizedValidationErrors = new HashSet<>();
        for (final Map.Entry<String, Set<NodeIdentifier>> validationEntry : validationErrorMap.entrySet()) {
            final String msg = validationEntry.getKey();
            final Set<NodeIdentifier> nodeIds = validationEntry.getValue();

            if (nodeIds.size() == totalNodes) {
                normalizedValidationErrors.add(msg);
            } else {
                nodeIds.forEach(id -> normalizedValidationErrors.add(id.getApiAddress() + ":" + id.getApiPort() + " -- " + msg));
            }
        }
        return normalizedValidationErrors;
    }
}
