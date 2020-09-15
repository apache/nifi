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
package org.apache.nifi.web.security;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestProxiedEntitiesUtils {

    @Test
    public void testBuildProxiedEntityGroupsString() {
        final Set<String> groups = new LinkedHashSet<>(Arrays.asList("group1", "group2", "group3"));
        final String groupsString = ProxiedEntitiesUtils.buildProxiedEntityGroupsString(groups);
        assertNotNull(groupsString);
        assertEquals("<group1><group2><group3>", groupsString);
    }

    @Test
    public void testBuildProxiedEntityGroupsStringWhenEmpty() {
        final String groupsString = ProxiedEntitiesUtils.buildProxiedEntityGroupsString(Collections.emptySet());
        assertNotNull(groupsString);
        assertEquals(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS_EMPTY, groupsString);
    }

    @Test
    public void testBuildProxiedEntityGroupsStringWithEscaping() {
        final Set<String> groups = new LinkedHashSet<>(Arrays.asList("gro<up1", "gro>up2", "group3"));
        final String groupsString = ProxiedEntitiesUtils.buildProxiedEntityGroupsString(groups);
        assertNotNull(groupsString);
        assertEquals("<gro\\<up1><gro\\>up2><group3>", groupsString);
    }

    @Test
    public void testTokenizeProxiedEntityGroups() {
        final Set<String> groups = ProxiedEntitiesUtils.tokenizeProxiedEntityGroups("<group1><group2><group3>");
        assertEquals(3, groups.size());
        assertTrue(groups.contains("group1"));
        assertTrue(groups.contains("group2"));
        assertTrue(groups.contains("group3"));
    }

    @Test
    public void testTokenizeProxiedEntityGroupsWhenEscaped() {
        final Set<String> groups = ProxiedEntitiesUtils.tokenizeProxiedEntityGroups("<gr\\<oup1><gro\\>up2><group3>");
        assertEquals(3, groups.size());
        assertTrue(groups.contains("gr<oup1"));
        assertTrue(groups.contains("gro>up2"));
        assertTrue(groups.contains("group3"));
    }

    @Test
    public void testTokenizeProxiedEntityGroupsWhenEmpty() {
        final Set<String> groups = ProxiedEntitiesUtils.tokenizeProxiedEntityGroups("<>");
        assertEquals(0, groups.size());
    }

    @Test
    public void testTokenizeProxiedEntityGroupsWhenNull() {
        final Set<String> groups = ProxiedEntitiesUtils.tokenizeProxiedEntityGroups(null);
        assertEquals(0, groups.size());
    }

}
