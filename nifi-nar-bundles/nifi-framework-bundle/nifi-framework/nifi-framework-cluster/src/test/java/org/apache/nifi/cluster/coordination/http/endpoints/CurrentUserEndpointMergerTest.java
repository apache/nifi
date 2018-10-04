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

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.web.api.dto.ComponentRestrictionPermissionDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.RequiredPermissionDTO;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CurrentUserEndpointMergerTest {

    @Test
    public void testMergeUserPermissions() {
        final NodeIdentifier nodeId1 = new NodeIdentifier("1", "localhost", 9000, "localhost", 9001, "localhost", 9006, "localhost", 9002, 9003, false);
        final CurrentUserEntity userNode1 = new CurrentUserEntity();
        userNode1.setControllerPermissions(buildPermissions(true, false));
        userNode1.setCountersPermissions(buildPermissions(true, true));
        userNode1.setPoliciesPermissions(buildPermissions(true, true));
        userNode1.setProvenancePermissions(buildPermissions(false, false));
        userNode1.setRestrictedComponentsPermissions(buildPermissions(false, false));
        userNode1.setSystemPermissions(buildPermissions(true, true));
        userNode1.setTenantsPermissions(buildPermissions(false, true));

        final Set<ComponentRestrictionPermissionDTO> componentRestrictionsNode1 = new HashSet<>();
        componentRestrictionsNode1.add(buildComponentRestriction(RequiredPermission.ACCESS_KEYTAB, true, true));
        componentRestrictionsNode1.add(buildComponentRestriction(RequiredPermission.WRITE_FILESYSTEM, false, true));
        componentRestrictionsNode1.add(buildComponentRestriction(RequiredPermission.READ_FILESYSTEM, true, true));
        userNode1.setComponentRestrictionPermissions(componentRestrictionsNode1);

        final NodeIdentifier nodeId2 = new NodeIdentifier("2", "localhost", 8000, "localhost", 8001, "localhost", 9006,"localhost", 8002, 8003, false);
        final CurrentUserEntity userNode2 = new CurrentUserEntity();
        userNode2.setControllerPermissions(buildPermissions(false, true));
        userNode2.setCountersPermissions(buildPermissions(true, false));
        userNode2.setPoliciesPermissions(buildPermissions(true, true));
        userNode2.setProvenancePermissions(buildPermissions(false, false));
        userNode2.setRestrictedComponentsPermissions(buildPermissions(true, true));
        userNode2.setSystemPermissions(buildPermissions(false, false));
        userNode2.setTenantsPermissions(buildPermissions(true, true));

        final Set<ComponentRestrictionPermissionDTO> componentRestrictionsNode2 = new HashSet<>();
        componentRestrictionsNode2.add(buildComponentRestriction(RequiredPermission.ACCESS_KEYTAB, true, false));
        componentRestrictionsNode2.add(buildComponentRestriction(RequiredPermission.WRITE_FILESYSTEM, true, false));
        componentRestrictionsNode2.add(buildComponentRestriction(RequiredPermission.EXECUTE_CODE, true, true));
        userNode2.setComponentRestrictionPermissions(componentRestrictionsNode2);

        final Map<NodeIdentifier, CurrentUserEntity> entityMap = new HashMap<>();
        entityMap.put(nodeId1, userNode1);
        entityMap.put(nodeId2, userNode2);

        final CurrentUserEndpointMerger merger = new CurrentUserEndpointMerger();
        merger.mergeResponses(userNode1, entityMap, Collections.emptySet(), Collections.emptySet());

        assertFalse(userNode1.getControllerPermissions().getCanRead());
        assertFalse(userNode1.getControllerPermissions().getCanWrite());
        assertTrue(userNode1.getCountersPermissions().getCanRead());
        assertFalse(userNode1.getCountersPermissions().getCanWrite());
        assertTrue(userNode1.getPoliciesPermissions().getCanRead());
        assertTrue(userNode1.getPoliciesPermissions().getCanWrite());
        assertFalse(userNode1.getProvenancePermissions().getCanRead());
        assertFalse(userNode1.getProvenancePermissions().getCanWrite());
        assertFalse(userNode1.getRestrictedComponentsPermissions().getCanRead());
        assertFalse(userNode1.getRestrictedComponentsPermissions().getCanWrite());
        assertFalse(userNode1.getSystemPermissions().getCanRead());
        assertFalse(userNode1.getSystemPermissions().getCanWrite());
        assertFalse(userNode1.getTenantsPermissions().getCanRead());
        assertTrue(userNode1.getTenantsPermissions().getCanWrite());

        userNode1.getComponentRestrictionPermissions().forEach(componentRestriction -> {
            if (RequiredPermission.ACCESS_KEYTAB.getPermissionIdentifier().equals(componentRestriction.getRequiredPermission().getId())) {
                assertTrue(componentRestriction.getPermissions().getCanRead());
                assertFalse(componentRestriction.getPermissions().getCanWrite());
            } else if (RequiredPermission.WRITE_FILESYSTEM.getPermissionIdentifier().equals(componentRestriction.getRequiredPermission().getId())) {
                assertFalse(componentRestriction.getPermissions().getCanRead());
                assertFalse(componentRestriction.getPermissions().getCanWrite());
            } else {
                fail();
            }
        });
    }

    private PermissionsDTO buildPermissions(final boolean canRead, final boolean canWrite) {
        final PermissionsDTO permissionsDto = new PermissionsDTO();
        permissionsDto.setCanRead(canRead);
        permissionsDto.setCanWrite(canWrite);
        return permissionsDto;
    }

    private ComponentRestrictionPermissionDTO buildComponentRestriction(final RequiredPermission requiredPermission, final boolean canRead, final boolean canWrite) {
        final RequiredPermissionDTO requiredPermissionDto = new RequiredPermissionDTO();
        requiredPermissionDto.setId(requiredPermission.getPermissionIdentifier());
        requiredPermissionDto.setLabel(requiredPermission.getPermissionLabel());

        final ComponentRestrictionPermissionDTO componentRestrictionPermissionDto = new ComponentRestrictionPermissionDTO();
        componentRestrictionPermissionDto.setRequiredPermission(requiredPermissionDto);
        componentRestrictionPermissionDto.setPermissions(buildPermissions(canRead, canWrite));
        return componentRestrictionPermissionDto;
    }
}