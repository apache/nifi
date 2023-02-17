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
package org.apache.nifi.registry.service.alias;

import org.apache.nifi.registry.url.aliaser.generated.Alias;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class RegistryUrlAliasServiceTest {
    private static Alias createAlias(String internal, String external) {
        Alias result = new Alias();
        result.setInternal(internal);
        result.setExternal(external);
        return result;
    }

    @Test
    public void testNoAliases() {
        RegistryUrlAliasService aliaser = new RegistryUrlAliasService(Collections.emptyList());

        String url = "https://registry.com:18080";

        assertEquals(url, aliaser.getExternal(url));
        assertEquals(url, aliaser.getInternal(url));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMalformedExternal() {
        new RegistryUrlAliasService(Collections.singletonList(createAlias("https://registry.com:18080", "registry.com:18080")));
    }

    @Test
    public void testSingleAliasUrl() {
        String internal = "https://registry-1.com:18443";
        String external = "http://localhost:18080";
        String unchanged = "https://registry-2.com:18443";

        RegistryUrlAliasService aliaser = new RegistryUrlAliasService(Collections.singletonList(createAlias(internal, external)));

        assertEquals(external, aliaser.getExternal(internal));
        assertEquals(internal, aliaser.getInternal(external));

        assertEquals(unchanged, aliaser.getExternal(unchanged));
        assertEquals(unchanged, aliaser.getInternal(unchanged));

        // Ensure replacement is only the prefix
        internal += "/nifi-registry/";
        external += "/nifi-registry/";
        unchanged += "/nifi-registry/";

        assertEquals(external, aliaser.getExternal(internal));
        assertEquals(internal, aliaser.getInternal(external));

        assertEquals(unchanged, aliaser.getExternal(unchanged));
        assertEquals(unchanged, aliaser.getInternal(unchanged));
    }

    @Test
    public void testSingleAliasToken() {
        String internal = "THIS_NIFI_REGISTRY";
        String external = "http://localhost:18080";
        String unchanged = "https://registry-2.com:18443";

        RegistryUrlAliasService aliaser = new RegistryUrlAliasService(Collections.singletonList(createAlias(internal, external)));

        assertEquals(external, aliaser.getExternal(internal));
        assertEquals(internal, aliaser.getInternal(external));

        assertEquals(unchanged, aliaser.getExternal(unchanged));
        assertEquals(unchanged, aliaser.getInternal(unchanged));

        // Ensure replacement is only the prefix
        internal += "/nifi-registry/";
        external += "/nifi-registry/";
        unchanged += "/nifi-registry/";

        assertEquals(external, aliaser.getExternal(internal));
        assertEquals(internal, aliaser.getInternal(external));

        assertEquals(unchanged, aliaser.getExternal(unchanged));
        assertEquals(unchanged, aliaser.getInternal(unchanged));
    }

    @Test
    public void testMultipleAliases() {
        String internal1 = "https://registry-1.com:18443";
        String external1 = "http://localhost:18080";
        String internal2 = "https://registry-2.com:18443";
        String external2 = "http://localhost:18081";
        String internal3 = "THIS_NIFI_REGISTRY";
        String external3 = "http://localhost:18082";

        String unchanged = "https://registry-3.com:18443";

        RegistryUrlAliasService aliaser = new RegistryUrlAliasService(Arrays.asList(createAlias(internal1, external1), createAlias(internal2, external2), createAlias(internal3, external3)));

        assertEquals(external1, aliaser.getExternal(internal1));
        assertEquals(external2, aliaser.getExternal(internal2));
        assertEquals(external3, aliaser.getExternal(internal3));

        assertEquals(internal1, aliaser.getInternal(external1));
        assertEquals(internal2, aliaser.getInternal(external2));
        assertEquals(internal3, aliaser.getInternal(external3));

        assertEquals(unchanged, aliaser.getExternal(unchanged));
        assertEquals(unchanged, aliaser.getInternal(unchanged));

        // Ensure replacement is only the prefix
        internal1 += "/nifi-registry/";
        internal2 += "/nifi-registry/";
        internal3 += "/nifi-registry/";

        external1 += "/nifi-registry/";
        external2 += "/nifi-registry/";
        external3 += "/nifi-registry/";

        unchanged += "/nifi-registry/";

        assertEquals(external1, aliaser.getExternal(internal1));
        assertEquals(external2, aliaser.getExternal(internal2));
        assertEquals(external3, aliaser.getExternal(internal3));

        assertEquals(internal1, aliaser.getInternal(external1));
        assertEquals(internal2, aliaser.getInternal(external2));
        assertEquals(internal3, aliaser.getInternal(external3));

        assertEquals(unchanged, aliaser.getExternal(unchanged));
        assertEquals(unchanged, aliaser.getInternal(unchanged));
    }

    @Test
    public void testMigrationPath() {
        String internal1 = "INTERNAL_TOKEN";
        String internal2 = "http://old.registry.url";
        String external = "https://new.registry.url";

        RegistryUrlAliasService aliaser = new RegistryUrlAliasService(Arrays.asList(createAlias(internal1, external), createAlias(internal2, external)));

        assertEquals(internal1, aliaser.getInternal(external));

        assertEquals(external, aliaser.getExternal(internal1));
        assertEquals(external, aliaser.getExternal(internal2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateInternalTokens() {
        String internal = "THIS_NIFI_REGISTRY";
        String external1 = "http://localhost:18080";
        String external2 = "http://localhost:18081";

        new RegistryUrlAliasService(Arrays.asList(createAlias(internal, external1), createAlias(internal, external2)));
    }
}
