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
import org.apache.nifi.web.api.entity.BulletinEntity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class BulletinMerger {

    private BulletinMerger() {}

    public static Comparator<BulletinEntity> BULLETIN_COMPARATOR = new Comparator<BulletinEntity>() {
        @Override
        public int compare(BulletinEntity o1, BulletinEntity o2) {
            if (o1 == null && o2 == null) {
                return 0;
            }
            if (o1 == null) {
                return 1;
            }
            if (o2 == null) {
                return -1;
            }

            return -Long.compare(o1.getId(), o2.getId());
        }
    };

    /**
     * Merges the bulletins.
     *
     * @param bulletins bulletins
     */
    public static List<BulletinEntity> mergeBulletins(final Map<NodeIdentifier, List<BulletinEntity>> bulletins) {
        final List<BulletinEntity> bulletinDtos = new ArrayList<>();

        for (final Map.Entry<NodeIdentifier, List<BulletinEntity>> entry : bulletins.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final List<BulletinEntity> nodeBulletins = entry.getValue();
            final String nodeAddress = nodeId.getApiAddress() + ":" + nodeId.getApiPort();

            for (final BulletinEntity bulletin : nodeBulletins) {
                if (bulletin.getNodeAddress() == null) {
                    bulletin.setNodeAddress(nodeAddress);
                }

                bulletinDtos.add(bulletin);
            }
        }

        Collections.sort(bulletinDtos, (BulletinEntity o1, BulletinEntity o2) -> {
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
