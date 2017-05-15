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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.cluster.manager.BulletinMerger.ALL_NODES_MESSAGE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BulletinMergerTest {

    long bulletinId = 0;

    private BulletinEntity createBulletin(final String message) {
        final BulletinDTO bulletin = new BulletinDTO();
        bulletin.setId(bulletinId++);
        bulletin.setMessage(message);
        bulletin.setTimestamp(new Date());

        final BulletinEntity entity = new BulletinEntity();
        entity.setId(bulletin.getId());
        entity.setTimestamp(bulletin.getTimestamp());
        entity.setCanRead(true);
        entity.setBulletin(bulletin);

        return entity;
    }

    @Test
    public void mergeBulletins() throws Exception {
        final BulletinEntity bulletinEntity1 = createBulletin("This is bulletin 1");
        final BulletinEntity bulletinEntity2 = createBulletin("This is bulletin 2");

        final BulletinEntity unauthorizedBulletin = new BulletinEntity();
        unauthorizedBulletin.setId(bulletinId++);
        unauthorizedBulletin.setTimestamp(new Date());
        unauthorizedBulletin.setCanRead(false);

        final BulletinEntity copyOfBulletin1 = createBulletin("This is bulletin 1");

        final NodeIdentifier node1 = new NodeIdentifier("node-1", "host-1", 8080, "host-1", 19998, null, null, null, false);
        final NodeIdentifier node2 = new NodeIdentifier("node-2", "host-2", 8081, "host-2", 19999, null, null, null, false);

        final Map<NodeIdentifier, List<BulletinEntity>> nodeMap = new HashMap<>();
        nodeMap.put(node1, new ArrayList<>());
        nodeMap.put(node2, new ArrayList<>());

        nodeMap.get(node1).add(bulletinEntity1);
        nodeMap.get(node1).add(bulletinEntity2);
        nodeMap.get(node1).add(unauthorizedBulletin);

        nodeMap.get(node2).add(copyOfBulletin1);

        final List<BulletinEntity> bulletinEntities = BulletinMerger.mergeBulletins(nodeMap, nodeMap.size());
        assertEquals(bulletinEntities.size(), 3);
        assertTrue(bulletinEntities.contains(copyOfBulletin1));
        assertEquals(copyOfBulletin1.getNodeAddress(), ALL_NODES_MESSAGE);
        assertTrue(bulletinEntities.contains(bulletinEntity2));
        assertTrue(bulletinEntities.contains(unauthorizedBulletin));
    }

}