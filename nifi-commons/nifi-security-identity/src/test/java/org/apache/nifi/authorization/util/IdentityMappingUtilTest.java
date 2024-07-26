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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class IdentityMappingUtilTest {

    @Test
    public void testIdentityMappingGeneration() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapProperties("UPPER"));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);
        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);

        final Optional<IdentityMapping> dnMapping = identityMappings.stream().filter(im -> im.getKey().equals("dn")).findFirst();
        final Optional<IdentityMapping> ldapMapping = identityMappings.stream().filter(im -> im.getKey().equals("ldap")).findFirst();

        Assertions.assertTrue(dnMapping.isPresent());
        Assertions.assertEquals("NONE", dnMapping.get().getTransform().name());

        Assertions.assertTrue(ldapMapping.isPresent());
        Assertions.assertEquals("UPPER", ldapMapping.get().getTransform().name());
    }

    // NIFI-13409 TC1
    @Test
    public void testMappingIdentity() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapProperties("UPPER"));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);

        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);
        Assertions.assertEquals("nifi-node1", IdentityMappingUtil.mapIdentity("CN=nifi-node1, ST=MD, C=US", identityMappings));
        Assertions.assertEquals("NIFIADMIN", IdentityMappingUtil.mapIdentity("nifiadmin", identityMappings));
    }

    // NIFI-13409 TC2
    @Test
    public void testMappingIdentityWithTwoInclusiveMappings() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapProperties("UPPER"));
        properties.putAll(getUsernameProperties("LOWER"));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);

        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);
        Assertions.assertEquals("nifi-node1", IdentityMappingUtil.mapIdentity("CN=nifi-node1, ST=MD, C=US", identityMappings));
        Assertions.assertEquals("NIFIADMIN", IdentityMappingUtil.mapIdentity("nifiadmin", identityMappings));
    }

    // NIFI-13409 TC3
    @Test
    public void testMappingIdentityWithTwoInclusiveMappingsReversed() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapProperties("LOWER"));
        properties.putAll(getUsernameProperties("UPPER"));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);

        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);
        Assertions.assertEquals("nifi-node1", IdentityMappingUtil.mapIdentity("CN=nifi-node1, ST=MD, C=US", identityMappings));
        Assertions.assertEquals("nifiadmin", IdentityMappingUtil.mapIdentity("nifiadmin", identityMappings));
    }

    // NIFI-13409 TC4
    @Test
    public void testMappingIdentityWithPrefixedMapping() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapPropertiesWithPrefix("LOWER"));
        properties.putAll(getUsernameProperties("UPPER"));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);

        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);
        Assertions.assertEquals("nifi-node1", IdentityMappingUtil.mapIdentity("CN=nifi-node1, ST=MD, C=US", identityMappings));
        Assertions.assertEquals("NIFIADMIN", IdentityMappingUtil.mapIdentity("nifiadmin", identityMappings));
    }

    // NIFI-13409 TC5
    @Test
    public void testMappingIdentityWithThreeDifferentMappings() {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(getDnProperties());
        properties.putAll(getLdapPropertiesWithPrefix("LOWER"));
        properties.putAll(getUsernamePropertiesWithPostfix("UPPER"));
        final NiFiProperties niFiProperties = new NiFiProperties(properties);

        final List<IdentityMapping> identityMappings = IdentityMappingUtil.getIdentityMappings(niFiProperties);
        Assertions.assertEquals("nifi-node1", IdentityMappingUtil.mapIdentity("CN=nifi-node1, ST=MD, C=US", identityMappings));
        Assertions.assertEquals("NIFIADMIN.TEST", IdentityMappingUtil.mapIdentity("nifiadmin", identityMappings));
    }

    private static Map<String, String> getDnProperties() {
        final Map<String, String> values = new HashMap<>();
        values.put("nifi.security.identity.mapping.pattern.dn", "^CN=(.*?),\\s{0,1}.+$");
        values.put("nifi.security.identity.mapping.transform.dn", "NONE");
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

    private static Map<String, String> getUsernameProperties(String transform) {
        final Map<String, String> values = new HashMap<>();
        values.put("nifi.security.identity.mapping.pattern.username", "^(.*)$");
        values.put("nifi.security.identity.mapping.transform.username", transform);
        values.put("nifi.security.identity.mapping.value.username", "$1");
        return values;
    }

    private static Map<String, String> getUsernamePropertiesWithPostfix(String transform) {
        final Map<String, String> values = new HashMap<>();
        values.put("nifi.security.identity.mapping.pattern.username", "^(.*)$");
        values.put("nifi.security.identity.mapping.transform.username", transform);
        values.put("nifi.security.identity.mapping.value.username", "$1.test");
        return values;
    }
}