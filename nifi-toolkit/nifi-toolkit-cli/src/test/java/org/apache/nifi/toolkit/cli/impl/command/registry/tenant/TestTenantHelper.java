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
package org.apache.nifi.toolkit.cli.impl.command.registry.tenant;

import org.apache.nifi.registry.authorization.Tenant;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestTenantHelper {
    @Test
    public void testSelectExistingTenantsWithEmptyNamesAndIds() throws Exception {
        // GIVEN
        String names = "";
        String ids = "";

        List<Tenant> allTenants = Arrays.asList(
            createTenant("__1", "__1"),
            createTenant("__2", "__2")
        );

        List<Tenant> expected = Collections.emptyList();

        // WHEN
        // THEN
        testSelectExistingTenants(names, ids, allTenants, expected);
    }

    @Test
    public void testSelectExistingTenantsWithNames() throws Exception {
        // GIVEN
        String names = "name1,name3";
        String ids = "";

        Tenant tenantFoundByName1 = createTenant("__1", "name1");
        Tenant tenantNotFound2 = createTenant("__2", "__2");
        Tenant tenantFoundByName3 = createTenant("__3", "name3");
        Tenant tenantNotFound4 = createTenant("__4", "__4");

        List<Tenant> allTenants = Arrays.asList(
            tenantFoundByName1,
            tenantNotFound2,
            tenantFoundByName3,
            tenantNotFound4
        );

        List<Tenant> expected = Arrays.asList(
            tenantFoundByName1,
            tenantFoundByName3
        );

        // WHEN
        // THEN
        testSelectExistingTenants(names, ids, allTenants, expected);
    }

    @Test
    public void testSelectExistingTenantsWithIds() throws Exception {
        // GIVEN
        String names = "";
        String ids = "id1,id2";

        Tenant tenantFoundById1 = createTenant("id1", "__1");
        Tenant tenantFoundById2 = createTenant("id2", "__2");
        Tenant tenantNotFound3 = createTenant("__3", "__3");
        Tenant tenantNotFound4 = createTenant("__4", "__4");

        List<Tenant> allTenants = Arrays.asList(
            tenantFoundById1,
            tenantFoundById2,
            tenantNotFound3,
            tenantNotFound4
        );

        List<Tenant> expected = Arrays.asList(
            tenantFoundById1,
            tenantFoundById2
        );

        // WHEN
        // THEN
        testSelectExistingTenants(names, ids, allTenants, expected);
    }

    @Test
    public void testSelectExistingTenantsWithComplexScenario() throws Exception {
        // GIVEN
        String names = "name1,name3";
        String ids = "id1,id2";

        Tenant tenantFoundByIdAndName = createTenant("id1", "name1");
        Tenant tenantFoundById = createTenant("id2", "_2_");
        Tenant tenantFoundByName = createTenant("__3", "name3");
        Tenant tenantNotFound = createTenant("__4", "__4");

        List<Tenant> allTenants = Arrays.asList(
            tenantFoundByIdAndName,
            tenantFoundById,
            tenantFoundByName,
            tenantNotFound
        );

        List<Tenant> expected = Arrays.asList(
            tenantFoundByIdAndName,
            tenantFoundById,
            tenantFoundByName
        );

        // WHEN
        // THEN
        testSelectExistingTenants(names, ids, allTenants, expected);
    }

    private void testSelectExistingTenants(String names, String ids, List<Tenant> allTenants, List<Tenant> expectedTenants) {
        // GIVEN
        Set<Tenant> expected = new HashSet<>(expectedTenants);

        // WHEN
        Set<Tenant> actual = TenantHelper.selectExistingTenants(names, ids, allTenants);

        // THEN
        assertEquals(expected, actual);
    }

    private Tenant createTenant(String identifier, String identity) {
        Tenant tenant = new Tenant();

        tenant.setIdentifier(identifier);
        tenant.setIdentity(identity);

        return tenant;
    }
}
