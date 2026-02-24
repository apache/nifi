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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTenantHelper {
    @Test
    public void testSelectExistingTenantsWithEmptyNamesAndIds() throws Exception {
        final String names = "";
        final String ids = "";

        final List<Tenant> allTenants = Arrays.asList(
            createTenant("__1", "__1"),
            createTenant("__2", "__2")
        );

        final List<Tenant> expected = Collections.emptyList();

        testSelectExistingTenants(names, ids, allTenants, expected);
    }

    @Test
    public void testSelectExistingTenantsWithNames() throws Exception {
        final String names = "name1,name3";
        final String ids = "";

        final Tenant tenantFoundByName1 = createTenant("__1", "name1");
        final Tenant tenantNotFound2 = createTenant("__2", "__2");
        final Tenant tenantFoundByName3 = createTenant("__3", "name3");
        final Tenant tenantNotFound4 = createTenant("__4", "__4");

        final List<Tenant> allTenants = Arrays.asList(
            tenantFoundByName1,
            tenantNotFound2,
            tenantFoundByName3,
            tenantNotFound4
        );

        final List<Tenant> expected = Arrays.asList(
            tenantFoundByName1,
            tenantFoundByName3
        );

        testSelectExistingTenants(names, ids, allTenants, expected);
    }

    @Test
    public void testSelectExistingTenantsWithIds() throws Exception {
        final String names = "";
        final String ids = "id1,id2";

        final Tenant tenantFoundById1 = createTenant("id1", "__1");
        final Tenant tenantFoundById2 = createTenant("id2", "__2");
        final Tenant tenantNotFound3 = createTenant("__3", "__3");
        final Tenant tenantNotFound4 = createTenant("__4", "__4");

        final List<Tenant> allTenants = Arrays.asList(
            tenantFoundById1,
            tenantFoundById2,
            tenantNotFound3,
            tenantNotFound4
        );

        final List<Tenant> expected = Arrays.asList(
            tenantFoundById1,
            tenantFoundById2
        );

        testSelectExistingTenants(names, ids, allTenants, expected);
    }

    @Test
    public void testSelectExistingTenantsWithComplexScenario() throws Exception {
        final String names = "name1,name3";
        final String ids = "id1,id2";

        final Tenant tenantFoundByIdAndName = createTenant("id1", "name1");
        final Tenant tenantFoundById = createTenant("id2", "_2_");
        final Tenant tenantFoundByName = createTenant("__3", "name3");
        final Tenant tenantNotFound = createTenant("__4", "__4");

        final List<Tenant> allTenants = Arrays.asList(
            tenantFoundByIdAndName,
            tenantFoundById,
            tenantFoundByName,
            tenantNotFound
        );

        final List<Tenant> expected = Arrays.asList(
            tenantFoundByIdAndName,
            tenantFoundById,
            tenantFoundByName
        );

        testSelectExistingTenants(names, ids, allTenants, expected);
    }

    @Test
    public void testSelectExistingTenantsWithComma() throws Exception {
        final String names = "\"OU=platform, CN=service\",name2";
        final String ids = "";

        final Tenant tenantFoundByNameWithComma = createTenant("id1", "OU=platform, CN=service");
        final Tenant tenantFoundByName = createTenant("id2", "name2");
        final Tenant tenantNotFound = createTenant("__3", "__3");

        final List<Tenant> allTenants = Arrays.asList(
                tenantFoundByNameWithComma,
                tenantFoundByName,
                tenantNotFound
        );

        final List<Tenant> expected = Arrays.asList(
                tenantFoundByNameWithComma,
                tenantFoundByName
        );

        testSelectExistingTenants(names, ids, allTenants, expected);
    }

    private void testSelectExistingTenants(final String names, final String ids, final List<Tenant> allTenants, final List<Tenant> expectedTenants) throws IOException {
        final Set<Tenant> expected = new HashSet<>(expectedTenants);

        final Set<Tenant> actual = TenantHelper.selectExistingTenants(names, ids, allTenants);

        assertEquals(expected, actual);
    }

    private Tenant createTenant(final String identifier, final String identity) {
        final Tenant tenant = new Tenant();

        tenant.setIdentifier(identifier);
        tenant.setIdentity(identity);

        return tenant;
    }
}
