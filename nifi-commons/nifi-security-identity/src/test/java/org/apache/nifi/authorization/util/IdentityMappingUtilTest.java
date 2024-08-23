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
package org.apache.nifi.authorization.util;

import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdentityMappingUtilTest {

    private static final String MAPPING_KEY_DN = "dn";
    private static final String MAPPING_KEY_LDAP = "ldap";
    private static final String TRANSFORM_UPPER = "UPPER";
    private static final String TRANSFORM_LOWER = "LOWER";
    private static final String TRANSFORM_NONE = "NONE";
    private static final String IDENTITY_DN = "CN=nifi-node1, ST=MD, C=US";
    private static final String IDENTITY_NIFIADMIN = "nifiadmin";
    private static final String MAPPED_IDENTITY_NODE1 = "nifi-node1";
    private static final String MAPPED_IDENTITY_NIFIADMIN = "NIFIADMIN";

    @Test
    public void testIdentityMappingGeneration() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapProperties(TRANSFORM_UPPER));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);
        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);

        final Optional<IdentityMapping> dnMapping = identityMappings.stream().filter(im -> im.getKey().equals(MAPPING_KEY_DN)).findFirst();
        final Optional<IdentityMapping> ldapMapping = identityMappings.stream().filter(im -> im.getKey().equals(MAPPING_KEY_LDAP)).findFirst();

        assertTrue(dnMapping.isPresent());
        assertEquals(TRANSFORM_NONE, dnMapping.get().getTransform().name());

        assertTrue(ldapMapping.isPresent());
        assertEquals(TRANSFORM_UPPER, ldapMapping.get().getTransform().name());
    }

    // NIFI-13409 TC1
    @Test
    public void testMappingIdentity() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapProperties(TRANSFORM_UPPER));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);

        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);
        assertEquals(MAPPED_IDENTITY_NODE1, IdentityMappingUtil.mapIdentity(IDENTITY_DN, identityMappings));
        assertEquals(MAPPED_IDENTITY_NIFIADMIN, IdentityMappingUtil.mapIdentity(IDENTITY_NIFIADMIN, identityMappings));
    }

    // NIFI-13409 TC2
    @Test
    public void testMappingIdentityWithTwoInclusiveMappings() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapProperties(TRANSFORM_UPPER));
        properties.putAll(getUsernameProperties(TRANSFORM_LOWER));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);

        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);
        assertEquals(MAPPED_IDENTITY_NODE1, IdentityMappingUtil.mapIdentity(IDENTITY_DN, identityMappings));
        assertEquals(MAPPED_IDENTITY_NIFIADMIN, IdentityMappingUtil.mapIdentity(IDENTITY_NIFIADMIN, identityMappings));
    }

    // NIFI-13409 TC3
    @Test
    public void testMappingIdentityWithTwoInclusiveMappingsReversed() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapProperties(TRANSFORM_LOWER));
        properties.putAll(getUsernameProperties(TRANSFORM_UPPER));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);

        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);
        assertEquals(MAPPED_IDENTITY_NODE1, IdentityMappingUtil.mapIdentity(IDENTITY_DN, identityMappings));
        assertEquals(IDENTITY_NIFIADMIN, IdentityMappingUtil.mapIdentity(IDENTITY_NIFIADMIN, identityMappings));
    }

    // NIFI-13409 TC4
    @Test
    public void testMappingIdentityWithPrefixedMapping() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapPropertiesWithPrefix(TRANSFORM_LOWER));
        properties.putAll(getUsernameProperties(TRANSFORM_UPPER));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);

        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);
        assertEquals(MAPPED_IDENTITY_NODE1, IdentityMappingUtil.mapIdentity(IDENTITY_DN, identityMappings));
        assertEquals(MAPPED_IDENTITY_NIFIADMIN, IdentityMappingUtil.mapIdentity(IDENTITY_NIFIADMIN, identityMappings));
    }

    // NIFI-13409 TC5
    @Test
    public void testMappingIdentityWithThreeDifferentMappings() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapPropertiesWithPrefix(TRANSFORM_LOWER));
        properties.putAll(getUsernamePropertiesWithPostfix(TRANSFORM_UPPER));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);

        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);
        assertEquals(MAPPED_IDENTITY_NODE1, IdentityMappingUtil.mapIdentity(IDENTITY_DN, identityMappings));
        assertEquals("NIFIADMIN.TEST", IdentityMappingUtil.mapIdentity(IDENTITY_NIFIADMIN, identityMappings));
    }

    private static Map<String, String> getDnProperties() {
        final Map<String, String> values = new HashMap<>();
        values.put("nifi.security.identity.mapping.pattern.dn", "^CN=(.*?),\\s{0,1}.+$");
        values.put("nifi.security.identity.mapping.transform.dn", TRANSFORM_NONE);
        values.put("nifi.security.identity.mapping.value.dn", "$1");
        return values;
    }

    private static Map<String, String> getLdapProperties(final String transform) {
        final Map<String, String> values = new HashMap<>();
        values.put("nifi.security.identity.mapping.pattern.ldap", "^(.*)$");
        values.put("nifi.security.identity.mapping.transform.ldap", transform);
        values.put("nifi.security.identity.mapping.value.ldap", "$1");
        return values;
    }

    private static Map<String, String> getLdapPropertiesWithPrefix(final String transform) {
        final Map<String, String> values = new HashMap<>();
        values.put("nifi.security.identity.mapping.pattern.ldap", "^ldap(.*?)$");
        values.put("nifi.security.identity.mapping.transform.ldap", transform);
        values.put("nifi.security.identity.mapping.value.ldap", "$1");
        return values;
    }

    private static Map<String, String> getUsernameProperties(final String transform) {
        final Map<String, String> values = new HashMap<>();
        values.put("nifi.security.identity.mapping.pattern.username", "^(.*)$");
        values.put("nifi.security.identity.mapping.transform.username", transform);
        values.put("nifi.security.identity.mapping.value.username", "$1");
        return values;
    }

    private static Map<String, String> getUsernamePropertiesWithPostfix(final String transform) {
        final Map<String, String> values = new HashMap<>();
        values.put("nifi.security.identity.mapping.pattern.username", "^(.*)$");
        values.put("nifi.security.identity.mapping.transform.username", transform);
        values.put("nifi.security.identity.mapping.value.username", "$1.test");
        return values;
    }
}