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
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class UserGroupEntityMergerTest {

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

        final AccessPolicyDTO policy1DTO = new AccessPolicyDTO();
        policy1DTO.setId("policy-1");

        final AccessPolicyEntity policy1Entity = new AccessPolicyEntity();
        policy1Entity.setPermissions(permissed);
        policy1Entity.setId(policy1DTO.getId());
        policy1Entity.setComponent(policy1DTO);

        final AccessPolicyDTO policy2DTO = new AccessPolicyDTO();
        policy2DTO.setId("policy-2");

        final AccessPolicyEntity policy2Entity = new AccessPolicyEntity();
        policy2Entity.setPermissions(permissed);
        policy2Entity.setId(policy2DTO.getId());
        policy2Entity.setComponent(policy2DTO);

        final UserGroupDTO userGroup1DTO = new UserGroupDTO();
        userGroup1DTO.setId("user-1");
        userGroup1DTO.setAccessPolicies(Stream.of(policy1Entity, policy2Entity).collect(Collectors.toSet()));
        userGroup1DTO.setUsers(Stream.of(user2Entity).collect(Collectors.toSet()));

        final UserGroupEntity userGroup1Entity = new UserGroupEntity();
        userGroup1Entity.setPermissions(permissed);
        userGroup1Entity.setId(userGroup1DTO.getId());
        userGroup1Entity.setComponent(userGroup1DTO);

        final UserGroupDTO userGroup2DTO = new UserGroupDTO();
        userGroup2DTO.setId("user-2");
        userGroup2DTO.setAccessPolicies(Stream.of(policy1Entity).collect(Collectors.toSet()));
        userGroup2DTO.setUsers(Stream.of(user1Entity, user2Entity).collect(Collectors.toSet()));

        final UserGroupEntity userGroup2Entity = new UserGroupEntity();
        userGroup2Entity.setPermissions(permissed);
        userGroup2Entity.setId(userGroup2DTO.getId());
        userGroup2Entity.setComponent(userGroup2DTO);

        final Map<NodeIdentifier, UserGroupEntity> nodeMap = new HashMap<>();
        nodeMap.put(node1, userGroup1Entity);
        nodeMap.put(node2, userGroup2Entity);

        final UserGroupEntityMerger merger = new UserGroupEntityMerger();
        merger.merge(userGroup1Entity, nodeMap);

        assertEquals(1, userGroup1DTO.getUsers().size());
        assertTrue(userGroup1DTO.getAccessPolicies().contains(policy1Entity));

        assertEquals(1, userGroup1DTO.getUsers().size());
        assertTrue(userGroup1DTO.getUsers().contains(user2Entity));
    }

}