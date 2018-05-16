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

import com.google.common.collect.Lists;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.BulletinEntity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class BulletinMerger {

    final static String ALL_NODES_MESSAGE = "All Nodes";

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
    public static List<BulletinEntity> mergeBulletins(final Map<NodeIdentifier, List<BulletinEntity>> bulletins, final int totalNodes) {
        final List<BulletinEntity> bulletinEntities = new ArrayList<>();

        for (final Map.Entry<NodeIdentifier, List<BulletinEntity>> entry : bulletins.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final List<BulletinEntity> nodeBulletins = entry.getValue();
            final String nodeAddress = nodeId.getApiAddress() + ":" + nodeId.getApiPort();

            for (final BulletinEntity bulletinEntity : nodeBulletins) {
                if (bulletinEntity.getNodeAddress() == null) {
                    bulletinEntity.setNodeAddress(nodeAddress);
                }

                if (bulletinEntity.getCanRead() && bulletinEntity.getBulletin() != null && bulletinEntity.getBulletin().getNodeAddress() == null) {
                    bulletinEntity.getBulletin().setNodeAddress(nodeAddress);
                }
                bulletinEntities.add(bulletinEntity);
            }
        }

        final List<BulletinEntity> entities = Lists.newArrayList();

        // group by message when permissions allow
        final Map<String,List<BulletinEntity>> groupingEntities = bulletinEntities.stream()
                .filter(bulletinEntity -> bulletinEntity.getCanRead())
                .collect(Collectors.groupingBy(b -> b.getBulletin().getMessage()));

        // add one from each grouped bulletin when all nodes report the same message
        groupingEntities.forEach((message, groupedBulletinEntities) -> {
            if (groupedBulletinEntities.size() == totalNodes) {
                // get the most current bulletin
                final BulletinEntity selectedBulletinEntity = groupedBulletinEntities.stream()
                        .max(Comparator.comparingLong(bulletinEntity -> {
                            if (bulletinEntity.getTimestamp() == null) {
                                return 0;
                            } else {
                                return bulletinEntity.getTimestamp().getTime();
                            }
                        })).orElse(null);

                // should never be null, but just in case
                if (selectedBulletinEntity != null) {
                    selectedBulletinEntity.setNodeAddress(ALL_NODES_MESSAGE);
                    selectedBulletinEntity.getBulletin().setNodeAddress(ALL_NODES_MESSAGE);
                    entities.add(selectedBulletinEntity);
                }
            } else {
                // since all nodes didn't report the exact same bulletin, keep them all
                entities.addAll(groupedBulletinEntities);
            }
        });

        // ensure we also get the remainder of the bulletin entities
        bulletinEntities.stream()
                .filter(bulletinEntity -> !bulletinEntity.getCanRead())
                .forEach(entities::add);

        // ensure the bulletins are sorted by time
        Collections.sort(entities, (BulletinEntity o1, BulletinEntity o2) -> {
            final int timeComparison = o1.getTimestamp().compareTo(o2.getTimestamp());
            if (timeComparison != 0) {
                return timeComparison;
            }

            return o1.getNodeAddress().compareTo(o2.getNodeAddress());
        });

        return entities;
    }
}
