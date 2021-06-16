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
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RemoteProcessGroupEntityMergerTest {

    @Test
    public void testMergeRemoteProcessGroups() throws Exception {
        final NodeIdentifier node1 = new NodeIdentifier("node-1", "host-1", 8080, "host-1", 19998, null, null, null, false);
        final NodeIdentifier node2 = new NodeIdentifier("node-2", "host-2", 8081, "host-2", 19999, null, null, null, false);

        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(true);
        permissions.setCanWrite(true);

        final PermissionsDTO opsPermissions = new PermissionsDTO();
        opsPermissions.setCanRead(false);
        opsPermissions.setCanWrite(false);

        final RemoteProcessGroupStatusDTO status = new RemoteProcessGroupStatusDTO();
        status.setAggregateSnapshot(new RemoteProcessGroupStatusSnapshotDTO());

        final RemoteProcessGroupPortDTO in1_1 = new RemoteProcessGroupPortDTO();
        in1_1.setName("in1");

        final RemoteProcessGroupPortDTO in1_2 = new RemoteProcessGroupPortDTO();
        in1_2.setName("in2");

        final Set<RemoteProcessGroupPortDTO> inputs1 = new HashSet<>();
        inputs1.add(in1_1);
        inputs1.add(in1_2);

        final RemoteProcessGroupPortDTO out1_1 = new RemoteProcessGroupPortDTO();
        out1_1.setName("out1");

        final Set<RemoteProcessGroupPortDTO> outputs1 = new HashSet<>();
        outputs1.add(out1_1);

        final RemoteProcessGroupContentsDTO contents1 = new RemoteProcessGroupContentsDTO();
        contents1.setInputPorts(inputs1);
        contents1.setOutputPorts(outputs1);

        final RemoteProcessGroupDTO rpg1 = new RemoteProcessGroupDTO();
        rpg1.setContents(contents1);

        final RemoteProcessGroupEntity entity1 = new RemoteProcessGroupEntity();
        entity1.setPermissions(permissions);
        entity1.setOperatePermissions(opsPermissions);
        entity1.setStatus(status);
        entity1.setComponent(rpg1);

        final RemoteProcessGroupPortDTO in2_1 = new RemoteProcessGroupPortDTO();
        in2_1.setName("in1");

        final Set<RemoteProcessGroupPortDTO> inputs2 = new HashSet<>();
        inputs2.add(in2_1);

        final RemoteProcessGroupPortDTO out2_1 = new RemoteProcessGroupPortDTO();
        out2_1.setName("out1");

        final RemoteProcessGroupPortDTO out2_2 = new RemoteProcessGroupPortDTO();
        out2_2.setName("out2");

        final Set<RemoteProcessGroupPortDTO> outputs2 = new HashSet<>();
        outputs2.add(out2_1);
        outputs2.add(out2_2);

        final RemoteProcessGroupContentsDTO contents2 = new RemoteProcessGroupContentsDTO();
        contents2.setInputPorts(inputs2);
        contents2.setOutputPorts(outputs2);

        final RemoteProcessGroupDTO rpg2 = new RemoteProcessGroupDTO();
        rpg2.setContents(contents2);

        final RemoteProcessGroupEntity entity2 = new RemoteProcessGroupEntity();
        entity2.setPermissions(permissions);
        entity2.setOperatePermissions(opsPermissions);
        entity2.setStatus(status);
        entity2.setComponent(rpg2);

        final Map<NodeIdentifier, RemoteProcessGroupEntity> nodeMap = new HashMap<>();
        nodeMap.put(node1, entity1);
        nodeMap.put(node2, entity2);

        final RemoteProcessGroupEntityMerger merger = new RemoteProcessGroupEntityMerger();
        merger.merge(entity1, nodeMap);

        // should only include ports in common to all rpg's
        assertEquals(1, entity1.getComponent().getContents().getInputPorts().size());
        assertEquals("in1", entity1.getComponent().getContents().getInputPorts().iterator().next().getName());
        assertEquals(1, entity1.getComponent().getContents().getOutputPorts().size());
        assertEquals("out1", entity1.getComponent().getContents().getOutputPorts().iterator().next().getName());
    }

    @Test
    public void testNoPortsAvailableOnOneNode() throws Exception {
        final NodeIdentifier node1 = new NodeIdentifier("node-1", "host-1", 8080, "host-1", 19998, null, null, null, false);
        final NodeIdentifier node2 = new NodeIdentifier("node-2", "host-2", 8081, "host-2", 19999, null, null, null, false);

        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(true);
        permissions.setCanWrite(true);

        final PermissionsDTO opsPermissions = new PermissionsDTO();
        opsPermissions.setCanRead(false);
        opsPermissions.setCanWrite(false);

        final RemoteProcessGroupStatusDTO status = new RemoteProcessGroupStatusDTO();
        status.setAggregateSnapshot(new RemoteProcessGroupStatusSnapshotDTO());

        final RemoteProcessGroupPortDTO in1_1 = new RemoteProcessGroupPortDTO();
        in1_1.setName("in1");

        final RemoteProcessGroupPortDTO in1_2 = new RemoteProcessGroupPortDTO();
        in1_2.setName("in2");

        final Set<RemoteProcessGroupPortDTO> inputs1 = new HashSet<>();
        inputs1.add(in1_1);
        inputs1.add(in1_2);

        final RemoteProcessGroupPortDTO out1_1 = new RemoteProcessGroupPortDTO();
        out1_1.setName("out1");

        final Set<RemoteProcessGroupPortDTO> outputs1 = new HashSet<>();
        outputs1.add(out1_1);

        final RemoteProcessGroupContentsDTO contents1 = new RemoteProcessGroupContentsDTO();
        contents1.setInputPorts(inputs1);
        contents1.setOutputPorts(outputs1);

        final RemoteProcessGroupDTO rpg1 = new RemoteProcessGroupDTO();
        rpg1.setContents(contents1);
        rpg1.setInputPortCount(2);
        rpg1.setOutputPortCount(1);

        final RemoteProcessGroupEntity entity1 = new RemoteProcessGroupEntity();
        entity1.setPermissions(permissions);
        entity1.setOperatePermissions(opsPermissions);
        entity1.setStatus(status);
        entity1.setComponent(rpg1);

        final Set<RemoteProcessGroupPortDTO> inputs2 = new HashSet<>();
        final Set<RemoteProcessGroupPortDTO> outputs2 = new HashSet<>();

        final RemoteProcessGroupContentsDTO contents2 = new RemoteProcessGroupContentsDTO();
        contents2.setInputPorts(inputs2);
        contents2.setOutputPorts(outputs2);

        final RemoteProcessGroupDTO rpg2 = new RemoteProcessGroupDTO();
        rpg2.setContents(contents2);
        rpg2.setInputPortCount(0);
        rpg2.setOutputPortCount(0);

        final RemoteProcessGroupEntity entity2 = new RemoteProcessGroupEntity();
        entity2.setPermissions(permissions);
        entity2.setOperatePermissions(opsPermissions);
        entity2.setStatus(status);
        entity2.setComponent(rpg2);

        final Map<NodeIdentifier, RemoteProcessGroupEntity> nodeMap = new HashMap<>();
        nodeMap.put(node1, entity1);
        nodeMap.put(node2, entity2);

        final RemoteProcessGroupEntityMerger merger = new RemoteProcessGroupEntityMerger();
        merger.merge(entity1, nodeMap);

        // should only include ports in common to all rpg's
        assertEquals(0, entity1.getComponent().getContents().getInputPorts().size());
        assertEquals(0, entity1.getComponent().getContents().getOutputPorts().size());
        assertEquals(0, entity1.getComponent().getInputPortCount().intValue());
        assertEquals(0, entity1.getComponent().getOutputPortCount().intValue());
    }
}
