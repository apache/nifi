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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NiFiAuthenticationProviderTest {

    @Test
    public void testValidPropertiesProvided() {
        final String pattern = "^cn=(.*?),dc=(.*?),dc=(.*?)$";
        final String value = "$1@$2.$3";

        Properties properties = new Properties();
        properties.setProperty("nifi.security.identity.mapping.pattern.dn", pattern);
        properties.setProperty("nifi.security.identity.mapping.value.dn", value);

        final NiFiProperties nifiProperties = getNiFiProperties(properties);

        TestableNiFiAuthenticationProvider provider = new TestableNiFiAuthenticationProvider(nifiProperties);
        List<IdentityMapping> mappings = provider.getMappings();
        assertEquals(1, mappings.size());
        assertEquals("dn", mappings.get(0).getKey());
        assertEquals(pattern, mappings.get(0).getPattern().pattern());
        assertEquals(value, mappings.get(0).getReplacementValue());
    }

    @Test
    public void testNoMappings() {
        Properties properties = new Properties();
        final NiFiProperties nifiProperties = getNiFiProperties(properties);

        TestableNiFiAuthenticationProvider provider = new TestableNiFiAuthenticationProvider(nifiProperties);
        List<IdentityMapping> mappings = provider.getMappings();
        assertEquals(0, mappings.size());

        final String identity = "john";
        assertEquals(identity, provider.mapIdentity(identity));
    }

    @Test
    public void testPatternPropertyWithNoValue() {
        Properties properties = new Properties();
        properties.setProperty("nifi.security.identity.mapping.pattern.dn", "");
        properties.setProperty("nifi.security.identity.mapping.value.dn", "value");

        final NiFiProperties nifiProperties = getNiFiProperties(properties);

        TestableNiFiAuthenticationProvider provider = new TestableNiFiAuthenticationProvider(nifiProperties);
        List<IdentityMapping> mappings = provider.getMappings();
        assertEquals(0, mappings.size());
    }

    @Test
    public void testPatternPropertyWithNoCorrespondingValueProperty() {
        Properties properties = new Properties();
        properties.setProperty("nifi.security.identity.mapping.pattern.dn", "");

        final NiFiProperties nifiProperties = getNiFiProperties(properties);

        TestableNiFiAuthenticationProvider provider = new TestableNiFiAuthenticationProvider(nifiProperties);
        List<IdentityMapping> mappings = provider.getMappings();
        assertEquals(0, mappings.size());
    }

    @Test
    public void testMultipleMappings() {
        Properties properties = new Properties();
        properties.setProperty("nifi.security.identity.mapping.pattern.1", "pattern1");
        properties.setProperty("nifi.security.identity.mapping.value.1", "value1");
        properties.setProperty("nifi.security.identity.mapping.pattern.2", "pattern2");
        properties.setProperty("nifi.security.identity.mapping.value.2", "value2");
        properties.setProperty("nifi.security.identity.mapping.pattern.3", "pattern3");
        properties.setProperty("nifi.security.identity.mapping.value.3", "value3");

        final NiFiProperties nifiProperties = getNiFiProperties(properties);

        TestableNiFiAuthenticationProvider provider = new TestableNiFiAuthenticationProvider(nifiProperties);
        List<IdentityMapping> mappings = provider.getMappings();
        assertEquals(3, mappings.size());
    }

    @Test
    public void testMapIdentityWithSingleMapping() {
        final Properties properties = new Properties();
        properties.setProperty("nifi.security.identity.mapping.pattern.dn", "^cn=(.*?),dc=(.*?),dc=(.*?)$");
        properties.setProperty("nifi.security.identity.mapping.value.dn", "$1@$2.$3");

        final NiFiProperties nifiProperties = getNiFiProperties(properties);
        final TestableNiFiAuthenticationProvider provider = new TestableNiFiAuthenticationProvider(nifiProperties);

        final String identity = "cn=jsmith,dc=aaa,dc=bbb";
        final String mappedIdentity = provider.mapIdentity(identity);
        assertEquals("jsmith@aaa.bbb", mappedIdentity);
    }

    @Test
    public void testMapIdentityWithIncorrectGroupReference() {
        final Properties properties = new Properties();
        properties.setProperty("nifi.security.identity.mapping.pattern.dn", "^cn=(.*?),dc=(.*?),dc=(.*?)$");
        properties.setProperty("nifi.security.identity.mapping.value.dn", "$1@$2.$4");

        final NiFiProperties nifiProperties = getNiFiProperties(properties);
        final TestableNiFiAuthenticationProvider provider = new TestableNiFiAuthenticationProvider(nifiProperties);

        final String identity = "cn=jsmith,dc=aaa,dc=bbb";
        final String mappedIdentity = provider.mapIdentity(identity);
        assertEquals("jsmith@aaa.$4", mappedIdentity);
    }

    @Test
    public void testMapIdentityWithNoGroupReference() {
        final Properties properties = new Properties();
        properties.setProperty("nifi.security.identity.mapping.pattern.dn", "^cn=(.*?),dc=(.*?),dc=(.*?)$");
        properties.setProperty("nifi.security.identity.mapping.value.dn", "this makes no sense");

        final NiFiProperties nifiProperties = getNiFiProperties(properties);
        final TestableNiFiAuthenticationProvider provider = new TestableNiFiAuthenticationProvider(nifiProperties);

        final String identity = "cn=jsmith,dc=aaa,dc=bbb";
        final String mappedIdentity = provider.mapIdentity(identity);
        assertEquals("this makes no sense", mappedIdentity);
    }

    @Test
    public void testMapIdentityWithMultipleMatchingPatterns() {
        // create two pattern properties that are the same, but the value properties are different
        final Properties properties = new Properties();
        properties.setProperty("nifi.security.identity.mapping.pattern.dn2", "^cn=(.*?),dc=(.*?),dc=(.*?)$");
        properties.setProperty("nifi.security.identity.mapping.value.dn2", "$1_$2_$3");
        properties.setProperty("nifi.security.identity.mapping.pattern.dn1", "^cn=(.*?),dc=(.*?),dc=(.*?)$");
        properties.setProperty("nifi.security.identity.mapping.value.dn1", "$1 $2 $3");

        final NiFiProperties nifiProperties = getNiFiProperties(properties);
        final TestableNiFiAuthenticationProvider provider = new TestableNiFiAuthenticationProvider(nifiProperties);

        // the mapping should always use dn1 because it is sorted
        final String identity = "cn=jsmith,dc=aaa,dc=bbb";
        final String mappedIdentity = provider.mapIdentity(identity);
        assertEquals("jsmith aaa bbb", mappedIdentity);
    }

    private NiFiProperties getNiFiProperties(final Properties properties) {
        final NiFiProperties nifiProperties = mock(NiFiProperties.class);
        when(nifiProperties.getPropertyKeys()).thenReturn(properties.stringPropertyNames());

        when(nifiProperties.getProperty(anyString())).then(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocationOnMock) throws Throwable {
                return properties.getProperty((String)invocationOnMock.getArguments()[0]);
            }
        });
        return nifiProperties;
    }

    private static class TestableNiFiAuthenticationProvider extends NiFiAuthenticationProvider {
        /**
         * @param properties the NiFiProperties instance
         */
        public TestableNiFiAuthenticationProvider(NiFiProperties properties) {
            super(properties, mock(Authorizer.class));
        }

        @Override
        public Authentication authenticate(Authentication authentication) throws AuthenticationException {
            return null;
        }

        @Override
        public boolean supports(Class<?> authentication) {
            return false;
        }

    }

}
