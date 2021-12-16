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
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.cluster.manager.BulletinMerger.ALL_NODES_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BulletinMergerTest {
    private static final Date FIRST_TIMESTAMP = new Date(86400);

    private static final Date SECOND_TIMESTAMP = new Date(178000);

    private static final NodeIdentifier FIRST_NODE = new NodeIdentifier("node-1", "host-1", 8080, "host-address-1", 8888, null, null, null, false);

    private static final NodeIdentifier SECOND_NODE = new NodeIdentifier("node-2", "host-2", 8080, "host-address-2", 8888, null, null, null, false);

    private long bulletinId = 0;

    private BulletinEntity createBulletin(final String message, final Date timestamp, final boolean readable) {
        final BulletinDTO bulletin = new BulletinDTO();
        bulletin.setId(bulletinId++);
        bulletin.setMessage(message);
        bulletin.setTimestamp(timestamp);

        final BulletinEntity entity = new BulletinEntity();
        entity.setId(bulletin.getId());
        entity.setTimestamp(bulletin.getTimestamp());
        entity.setCanRead(readable);
        entity.setBulletin(bulletin);

        return entity;
    }

    @Test
    public void testMergeBulletins() {
        final BulletinEntity bulletinEntity1 = createBulletin("This is bulletin 1", FIRST_TIMESTAMP, true);
        final BulletinEntity bulletinEntity2 = createBulletin("This is bulletin 2", FIRST_TIMESTAMP,true);
        final BulletinEntity unauthorizedBulletin = createBulletin("Protected Bulletin", FIRST_TIMESTAMP, false);
        final List<BulletinEntity> bulletins = Arrays.asList(bulletinEntity1, bulletinEntity2, unauthorizedBulletin);

        final Map<NodeIdentifier, List<BulletinEntity>> nodeMap = new LinkedHashMap<>();
        nodeMap.put(FIRST_NODE, bulletins);
        nodeMap.put(SECOND_NODE, Collections.singletonList(bulletinEntity1));

        final List<BulletinEntity> merged = BulletinMerger.mergeBulletins(nodeMap, nodeMap.size());

        assertEquals(merged.size(), bulletins.size());
        assertTrue(merged.contains(bulletinEntity1), "First Bulletin not found");
        assertTrue(merged.contains(bulletinEntity2), "Second Bulletin not found");
        assertTrue(merged.contains(unauthorizedBulletin), "Protected Bulletin not found");

        assertEquals(bulletinEntity1.getNodeAddress(), ALL_NODES_MESSAGE);
    }

    @Test
    public void testMergeBulletinsSortedOldestNewest() {
        final BulletinEntity newerBulletin = createBulletin("Second Message", SECOND_TIMESTAMP, true);
        final BulletinEntity olderBulletin = createBulletin("First Message", FIRST_TIMESTAMP, true);
        final List<BulletinEntity> bulletins = Arrays.asList(newerBulletin, olderBulletin);

        final Map<NodeIdentifier, List<BulletinEntity>> nodeMap = new LinkedHashMap<>();
        nodeMap.put(FIRST_NODE, bulletins);

        final List<BulletinEntity> merged = BulletinMerger.mergeBulletins(nodeMap, nodeMap.size());
        assertEquals(merged.size(), bulletins.size());

        final Iterator<BulletinEntity> mergedBulletins = merged.iterator();
        assertEquals(olderBulletin, mergedBulletins.next(), "Older Bulletin not matched");
        assertEquals(newerBulletin, mergedBulletins.next(), "Newer Bulletin not matched");
    }
}
