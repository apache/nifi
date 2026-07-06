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
package org.apache.nifi.registry.properties;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestNiFiRegistryPropertiesContextPath {

    @Test
    public void testGetWebContextPathNotSet() {
        final NiFiRegistryProperties properties = new NiFiRegistryProperties();
        final String contextPath = properties.getWebContextPath();
        assertNotNull(contextPath);
        assertEquals("", contextPath);
    }

    @Test
    public void testGetWebContextPathWithLeadingSlash() {
        final Map<String, String> props = new HashMap<>();
        props.put(NiFiRegistryProperties.WEB_CONTEXT_PATH, "/integration-hub");
        final NiFiRegistryProperties properties = new NiFiRegistryProperties(props);
        final String contextPath = properties.getWebContextPath();
        assertEquals("/integration-hub", contextPath);
    }

    @Test
    public void testGetWebContextPathWithoutLeadingSlash() {
        final Map<String, String> props = new HashMap<>();
        props.put(NiFiRegistryProperties.WEB_CONTEXT_PATH, "integration-hub");
        final NiFiRegistryProperties properties = new NiFiRegistryProperties(props);
        final String contextPath = properties.getWebContextPath();
        assertEquals("/integration-hub", contextPath);
    }

    @Test
    public void testGetWebContextPathWithTrailingSlash() {
        final Map<String, String> props = new HashMap<>();
        props.put(NiFiRegistryProperties.WEB_CONTEXT_PATH, "/integration-hub/");
        final NiFiRegistryProperties properties = new NiFiRegistryProperties(props);
        final String contextPath = properties.getWebContextPath();
        assertEquals("/integration-hub", contextPath);
    }

    @Test
    public void testGetWebContextPathWithBothSlashes() {
        final Map<String, String> props = new HashMap<>();
        props.put(NiFiRegistryProperties.WEB_CONTEXT_PATH, "integration-hub/");
        final NiFiRegistryProperties properties = new NiFiRegistryProperties(props);
        final String contextPath = properties.getWebContextPath();
        assertEquals("/integration-hub", contextPath);
    }

    @Test
    public void testGetWebContextPathWithWhitespace() {
        final Map<String, String> props = new HashMap<>();
        props.put(NiFiRegistryProperties.WEB_CONTEXT_PATH, "  /integration-hub  ");
        final NiFiRegistryProperties properties = new NiFiRegistryProperties(props);
        final String contextPath = properties.getWebContextPath();
        assertEquals("/integration-hub", contextPath);
    }

    @Test
    public void testGetWebContextPathEmptyString() {
        final Map<String, String> props = new HashMap<>();
        props.put(NiFiRegistryProperties.WEB_CONTEXT_PATH, "");
        final NiFiRegistryProperties properties = new NiFiRegistryProperties(props);
        final String contextPath = properties.getWebContextPath();
        assertEquals("", contextPath);
    }
}
