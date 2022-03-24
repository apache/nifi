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
import org.apache.nifi.web.api.dto.AccessPolicySummaryDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.TenantDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.entity.AccessPolicySummaryEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.UserEntity;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class UserEntityMergerTest {

    @Test
    public void testMergeAccessPolicy() throws Exception {
        final NodeIdentifier node1 = new NodeIdentifier("node-1", "host-1", 8080, "host-1", 19998, null, null, null, false);
        final NodeIdentifier node2 = new NodeIdentifier("node-2", "host-2", 8081, "host-2", 19999, null, null, null, false);

        final PermissionsDTO permissed = new PermissionsDTO();
        permissed.setCanRead(true);
        permissed.setCanWrite(true);

        final TenantDTO userGroup1DTO = new TenantDTO();
        userGroup1DTO.setId("user-group-1");

        final TenantEntity userGroup1Entity = new TenantEntity();
        userGroup1Entity.setPermissions(permissed);
        userGroup1Entity.setId(userGroup1DTO.getId());
        userGroup1Entity.setComponent(userGroup1DTO);

        final TenantDTO userGroup2DTO = new TenantDTO();
        userGroup1DTO.setId("user-group-2");

        final TenantEntity userGroup2Entity = new TenantEntity();
        userGroup2Entity.setPermissions(permissed);
        userGroup2Entity.setId(userGroup2DTO.getId());
        userGroup2Entity.setComponent(userGroup2DTO);

        final AccessPolicySummaryDTO policy1DTO = new AccessPolicySummaryDTO();
        policy1DTO.setId("policy-1");

        final AccessPolicySummaryEntity policy1Entity = new AccessPolicySummaryEntity();
        policy1Entity.setPermissions(permissed);
        policy1Entity.setId(policy1DTO.getId());
        policy1Entity.setComponent(policy1DTO);

        final AccessPolicySummaryDTO policy2DTO = new AccessPolicySummaryDTO();
        policy2DTO.setId("policy-2");

        final AccessPolicySummaryEntity policy2Entity = new AccessPolicySummaryEntity();
        policy2Entity.setPermissions(permissed);
        policy2Entity.setId(policy2DTO.getId());
        policy2Entity.setComponent(policy2DTO);

        final UserDTO user1DTO = new UserDTO();
        user1DTO.setId("user-1");
        user1DTO.setAccessPolicies(Stream.of(policy1Entity, policy2Entity).collect(Collectors.toSet()));
        user1DTO.setUserGroups(Stream.of(userGroup2Entity).collect(Collectors.toSet()));

        final UserEntity user1Entity = new UserEntity();
        user1Entity.setPermissions(permissed);
        user1Entity.setId(user1DTO.getId());
        user1Entity.setComponent(user1DTO);

        final UserDTO user2DTO = new UserDTO();
        user2DTO.setId("user-2");
        user2DTO.setAccessPolicies(Stream.of(policy1Entity).collect(Collectors.toSet()));
        user2DTO.setUserGroups(Stream.of(userGroup1Entity, userGroup2Entity).collect(Collectors.toSet()));

        final UserEntity user2Entity = new UserEntity();
        user2Entity.setPermissions(permissed);
        user2Entity.setId(user2DTO.getId());
        user2Entity.setComponent(user2DTO);

        final Map<NodeIdentifier, UserEntity> nodeMap = new HashMap<>();
        nodeMap.put(node1, user1Entity);
        nodeMap.put(node2, user2Entity);

        final UserEntityMerger merger = new UserEntityMerger();
        merger.merge(user1Entity, nodeMap);

        assertEquals(1, user1DTO.getUserGroups().size());
        assertTrue(user1DTO.getAccessPolicies().contains(policy1Entity));

        assertEquals(1, user1DTO.getUserGroups().size());
        assertTrue(user1DTO.getUserGroups().contains(userGroup2Entity));
    }

}