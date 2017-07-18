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
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.TenantDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AccessPolicyEntityMergerTest {

    @Test
    public void testMergeAccessPolicy() throws Exception {
        final NodeIdentifier node1 = new NodeIdentifier("node-1", "host-1", 8080, "host-1", 19998, null, null, null, false);
        final NodeIdentifier node2 = new NodeIdentifier("node-2", "host-2", 8081, "host-2", 19999, null, null, null, false);

        final PermissionsDTO permissed = new PermissionsDTO();
        permissed.setCanRead(true);
        permissed.setCanWrite(true);

        final TenantDTO user1DTO = new TenantDTO();
        user1DTO.setId("user-1");

        final TenantEntity user1Entity = new TenantEntity();
        user1Entity.setPermissions(permissed);
        user1Entity.setId(user1DTO.getId());
        user1Entity.setComponent(user1DTO);

        final TenantDTO user2DTO = new TenantDTO();
        user1DTO.setId("user-2");

        final TenantEntity user2Entity = new TenantEntity();
        user2Entity.setPermissions(permissed);
        user2Entity.setId(user2DTO.getId());
        user2Entity.setComponent(user2DTO);

        final AccessPolicyDTO accessPolicy1DTO = new AccessPolicyDTO();
        accessPolicy1DTO.setId("policy-1");
        accessPolicy1DTO.setUsers(Stream.of(user1Entity, user2Entity).collect(Collectors.toSet()));
        accessPolicy1DTO.setUserGroups(Stream.of(user2Entity).collect(Collectors.toSet()));

        final AccessPolicyEntity accessPolicy1Entity = new AccessPolicyEntity();
        accessPolicy1Entity.setPermissions(permissed);
        accessPolicy1Entity.setId(accessPolicy1DTO.getId());
        accessPolicy1Entity.setComponent(accessPolicy1DTO);

        final AccessPolicyDTO accessPolicy2DTO = new AccessPolicyDTO();
        accessPolicy2DTO.setId("policy-2");
        accessPolicy2DTO.setUsers(Stream.of(user1Entity).collect(Collectors.toSet()));
        accessPolicy2DTO.setUserGroups(Stream.of(user1Entity, user2Entity).collect(Collectors.toSet()));

        final AccessPolicyEntity accessPolicy2Entity = new AccessPolicyEntity();
        accessPolicy2Entity.setPermissions(permissed);
        accessPolicy2Entity.setId(accessPolicy2DTO.getId());
        accessPolicy2Entity.setComponent(accessPolicy2DTO);

        final Map<NodeIdentifier, AccessPolicyEntity> nodeMap = new HashMap<>();
        nodeMap.put(node1, accessPolicy1Entity);
        nodeMap.put(node2, accessPolicy2Entity);

        final AccessPolicyEntityMerger merger = new AccessPolicyEntityMerger();
        merger.merge(accessPolicy1Entity, nodeMap);

        assertEquals(1, accessPolicy1DTO.getUserGroups().size());
        assertTrue(accessPolicy1DTO.getUsers().contains(user1Entity));

        assertEquals(1, accessPolicy1DTO.getUserGroups().size());
        assertTrue(accessPolicy1DTO.getUserGroups().contains(user2Entity));
    }

}