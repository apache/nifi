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
package org.apache.nifi.atlas.resolver;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

public class TestRegexNamespaceResolver {

    private PropertyContext context;
    private ValidationContext validationContext;

    public void setupMock(Map<String, String> properties) {
        context = Mockito.mock(PropertyContext.class);
        validationContext = Mockito.mock(ValidationContext.class);
        when(validationContext.getAllProperties()).thenReturn(properties);
        when(context.getAllProperties()).thenReturn(properties);
    }

    @Test
    public void testEmptySettings() {
        setupMock(Collections.EMPTY_MAP);
        final RegexNamespaceResolver resolver = new RegexNamespaceResolver();

        // It should be valid
        final Collection<ValidationResult> validationResults = resolver.validate(validationContext);
        Assert.assertEquals(0, validationResults.size());
        resolver.configure(context);

        Assert.assertNull(resolver.fromHostNames("example.com"));
    }

    @Test
    public void testInvalidNamespace() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(RegexNamespaceResolver.PATTERN_PROPERTY_PREFIX, ".*\\.example.com");
        setupMock(properties);
        final RegexNamespaceResolver resolver = new RegexNamespaceResolver();

        final Collection<ValidationResult> validationResults = resolver.validate(validationContext);
        Assert.assertEquals(1, validationResults.size());
        final ValidationResult validationResult = validationResults.iterator().next();
        Assert.assertEquals(RegexNamespaceResolver.PATTERN_PROPERTY_PREFIX, validationResult.getSubject());

        try {
            resolver.configure(context);
            Assert.fail("Configure method should fail, too");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testEmptyPattern() {
        final Map<String, String> properties = new HashMap<>();
        final String propertyName = RegexNamespaceResolver.PATTERN_PROPERTY_PREFIX + "Namespace1";
        properties.put(propertyName, "");
        setupMock(properties);
        final RegexNamespaceResolver resolver = new RegexNamespaceResolver();

        final Collection<ValidationResult> validationResults = resolver.validate(validationContext);
        Assert.assertEquals(1, validationResults.size());
        final ValidationResult validationResult = validationResults.iterator().next();
        Assert.assertEquals(propertyName, validationResult.getSubject());

        try {
            resolver.configure(context);
            Assert.fail("Configure method should fail, too");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testSinglePattern() {
        final Map<String, String> properties = new HashMap<>();
        final String propertyName = RegexNamespaceResolver.PATTERN_PROPERTY_PREFIX + "Namespace1";
        properties.put(propertyName, "^.*\\.example.com$");
        setupMock(properties);
        final RegexNamespaceResolver resolver = new RegexNamespaceResolver();

        final Collection<ValidationResult> validationResults = resolver.validate(validationContext);
        Assert.assertEquals(0, validationResults.size());

        resolver.configure(context);

        Assert.assertEquals("Namespace1", resolver.fromHostNames("host1.example.com"));
    }

    @Test
    public void testMultiplePatterns() {
        final Map<String, String> properties = new HashMap<>();
        final String propertyName = RegexNamespaceResolver.PATTERN_PROPERTY_PREFIX + "Namespace1";
        // Hostname or local ip address, delimited with a whitespace
        properties.put(propertyName, "^.*\\.example.com$\n^192.168.1.[\\d]+$");
        setupMock(properties);
        final RegexNamespaceResolver resolver = new RegexNamespaceResolver();

        final Collection<ValidationResult> validationResults = resolver.validate(validationContext);
        Assert.assertEquals(0, validationResults.size());

        resolver.configure(context);

        Assert.assertEquals("Namespace1", resolver.fromHostNames("host1.example.com"));
        Assert.assertEquals("Namespace1", resolver.fromHostNames("192.168.1.10"));
        Assert.assertEquals("Namespace1", resolver.fromHostNames("192.168.1.22"));
        Assert.assertNull(resolver.fromHostNames("192.168.2.30"));
    }

    @Test
    public void testMultipleNamespaces() {
        String namespace1 = "Namepsace1";
        String namespace2 = "Namespace2";

        final Map<String, String> properties = new HashMap<>();
        final String namespace1PropertyName = RegexNamespaceResolver.PATTERN_PROPERTY_PREFIX + namespace1;
        final String namepsace2PropertyName = RegexNamespaceResolver.PATTERN_PROPERTY_PREFIX + namespace2;
        // Hostname or local ip address
        properties.put(namespace1PropertyName, "^.*\\.c1\\.example.com$ ^192.168.1.[\\d]+$");
        properties.put(namepsace2PropertyName, "^.*\\.c2\\.example.com$ ^192.168.2.[\\d]+$");
        setupMock(properties);
        final RegexNamespaceResolver resolver = new RegexNamespaceResolver();

        final Collection<ValidationResult> validationResults = resolver.validate(validationContext);
        Assert.assertEquals(0, validationResults.size());

        resolver.configure(context);

        Assert.assertEquals(namespace1, resolver.fromHostNames("host1.c1.example.com"));
        Assert.assertEquals(namespace1, resolver.fromHostNames("192.168.1.10"));
        Assert.assertEquals(namespace1, resolver.fromHostNames("192.168.1.22"));
        Assert.assertEquals(namespace2, resolver.fromHostNames("host2.c2.example.com"));
        Assert.assertEquals(namespace2, resolver.fromHostNames("192.168.2.10"));
        Assert.assertEquals(namespace2, resolver.fromHostNames("192.168.2.22"));
        Assert.assertNull(resolver.fromHostNames("192.168.3.30"));
    }

}
