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
package org.apache.nifi.properties;

import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NiFiPropertiesLoaderTest {
    private static final String NULL_PATH = null;

    private static final String EMPTY_PATH = "/properties/conf/empty.nifi.properties";
    private static final String FLOW_PATH = "/properties/conf/flow.nifi.properties";
    private static final String PROTECTED_PATH = "/properties/conf/protected.nifi.properties";
    private static final String DUPLICATE_PROPERTIES_PATH = "/properties/conf/duplicates.nifi.properties";

    private static final String HEXADECIMAL_KEY = "12345678123456788765432187654321";
    private static final String EXPECTED_PASSWORD = "propertyValue";

    @AfterEach
    void clearSystemProperty() {
        System.clearProperty(NiFiProperties.PROPERTIES_FILE_PATH);
    }

    @Test
    void testDuplicateProperties() {
        final URL resource = NiFiPropertiesLoaderTest.class.getResource(DUPLICATE_PROPERTIES_PATH);
        assertNotNull(resource);

        final String path = resource.getPath();

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, path);

        final NiFiPropertiesLoader loader = NiFiPropertiesLoader.withKey(String.class.getSimpleName());

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            final NiFiProperties properties = loader.get();
        });

        String expectedMessage = "Duplicate property keys with different values were detected in the properties file: another.duplicate, nifi.flow.configuration.file";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    void testGetPropertiesNotFound() {
        final NiFiPropertiesLoader loader = new NiFiPropertiesLoader();

        assertThrows(IllegalArgumentException.class, loader::get);
    }

    @Test
    void testGetProperties() {
        final URL resource = NiFiPropertiesLoaderTest.class.getResource(FLOW_PATH);
        assertNotNull(resource);

        final String path = resource.getPath();

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, path);

        final NiFiPropertiesLoader loader = new NiFiPropertiesLoader();

        final NiFiProperties properties = loader.get();

        assertNotNull(properties);
        assertNotNull(properties.getFlowConfigurationFile());
    }

    @Test
    void testGetPropertiesWithKeyNoEncryptedProperties() {
        final URL resource = NiFiPropertiesLoaderTest.class.getResource(FLOW_PATH);
        assertNotNull(resource);

        final String path = resource.getPath();

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, path);

        final NiFiPropertiesLoader loader = NiFiPropertiesLoader.withKey(String.class.getSimpleName());

        final NiFiProperties properties = loader.get();

        assertNotNull(properties);
        assertNotNull(properties.getFlowConfigurationFile());
    }

    @Test
    void testLoadWithKey() {
        final NiFiPropertiesLoader loader = NiFiPropertiesLoader.withKey(HEXADECIMAL_KEY);

        final URL resource = NiFiPropertiesLoaderTest.class.getResource(PROTECTED_PATH);
        assertNotNull(resource);
        final String path = resource.getPath();

        final NiFiProperties properties = loader.load(path);

        assertNotNull(properties);

        assertNotNull(properties.getFlowConfigurationFile());
        assertEquals(EXPECTED_PASSWORD, properties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD));
    }

    @Test
    void testLoadWithDefaultKeyFromBootstrap() {
        final URL resource = NiFiPropertiesLoaderTest.class.getResource(FLOW_PATH);
        assertNotNull(resource);

        final String path = resource.getPath();

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, path);

        final NiFiProperties properties = NiFiPropertiesLoader.loadDefaultWithKeyFromBootstrap();

        assertNotNull(properties);
    }

    @Test
    void testLoadDefaultNotFound() {
        final NiFiPropertiesLoader loader = new NiFiPropertiesLoader();

        assertThrows(IllegalArgumentException.class, () -> loader.load(NULL_PATH));
    }

    @Test
    void testLoadPath() {
        final NiFiPropertiesLoader loader = new NiFiPropertiesLoader();

        final URL resource = NiFiPropertiesLoaderTest.class.getResource(EMPTY_PATH);
        assertNotNull(resource);

        final String path = resource.getPath();

        final NiFiProperties properties = loader.load(path);

        assertNotNull(properties);
    }
}
