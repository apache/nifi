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
package org.apache.nifi.registry.properties

import org.junit.*
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

@RunWith(JUnit4.class)
class NiFiRegistryPropertiesGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryPropertiesGroovyTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    @AfterClass
    static void tearDownOnce() {
    }

    private static NiFiRegistryProperties loadFromFile(String propertiesFilePath) {
        String filePath
        try {
            filePath = NiFiRegistryPropertiesGroovyTest.class.getResource(propertiesFilePath).toURI().getPath()
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Cannot load properties file due to " +
                    ex.getLocalizedMessage(), ex)
        }

        NiFiRegistryProperties properties = new NiFiRegistryProperties()
        FileReader reader = new FileReader(filePath)

        try {
            properties.load(reader)
            logger.info("Loaded {} properties from {}", properties.size(), filePath)

            return properties
        } catch (final Exception ex) {
            logger.error("Cannot load properties file due to " + ex.getLocalizedMessage())
            throw new RuntimeException("Cannot load properties file due to " +
                    ex.getLocalizedMessage(), ex)
        }
    }

    @Test
    void testConstructorShouldCreateNewInstance() throws Exception {
        // Arrange

        // Act
        NiFiRegistryProperties NiFiRegistryProperties = new NiFiRegistryProperties()
        logger.info("NiFiRegistryProperties has ${NiFiRegistryProperties.size()} properties: ${NiFiRegistryProperties.getPropertyKeys()}")

        // Assert
        assert NiFiRegistryProperties.size() == 0
        assert NiFiRegistryProperties.getPropertyKeys() == [] as Set
    }

    @Test
    void testConstructorShouldAcceptDefaultProperties() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        rawProperties.setProperty("key", "value")
        logger.info("rawProperties has ${rawProperties.size()} properties: ${rawProperties.stringPropertyNames()}")
        assert rawProperties.size() == 1

        // Act
        NiFiRegistryProperties NiFiRegistryProperties = new NiFiRegistryProperties(rawProperties)
        logger.info("NiFiRegistryProperties has ${NiFiRegistryProperties.size()} properties: ${NiFiRegistryProperties.getPropertyKeys()}")

        // Assert
        assert NiFiRegistryProperties.size() == 1
        assert NiFiRegistryProperties.getPropertyKeys() == ["key"] as Set
    }

    @Test
    void testShouldAllowMultipleInstances() throws Exception {
        // Arrange

        // Act
        Properties props = new Properties()
        props.setProperty("key", "value")
        NiFiRegistryProperties properties = new NiFiRegistryProperties(props)
        logger.info("niFiProperties has ${properties.size()} properties: ${properties.getPropertyKeys()}")
        NiFiRegistryProperties emptyProperties = new NiFiRegistryProperties()
        logger.info("emptyProperties has ${emptyProperties.size()} properties: ${emptyProperties.getPropertyKeys()}")

        // Assert
        assert properties.size() == 1
        assert properties.getPropertyKeys() == ["key"] as Set

        assert emptyProperties.size() == 0
        assert emptyProperties.getPropertyKeys() == [] as Set
    }

    @Test
    void testAdditionalOidcScopesAreTrimmed() {
        final String scope = "abc"
        final String scopeLeadingWhitespace = " def"
        final String scopeTrailingWhitespace = "ghi "
        final String scopeLeadingTrailingWhitespace = " jkl "

        String additionalScopes = String.join(",", scope, scopeLeadingWhitespace,
                scopeTrailingWhitespace, scopeLeadingTrailingWhitespace)

        NiFiRegistryProperties properties = mock(NiFiRegistryProperties.class)
        when(properties.getProperty(NiFiRegistryProperties.SECURITY_USER_OIDC_ADDITIONAL_SCOPES, ""))
                .thenReturn(additionalScopes)
        when(properties.getOidcAdditionalScopes()).thenCallRealMethod()

        List<String> scopes = properties.getOidcAdditionalScopes()

        assertTrue(scopes.contains(scope));
        assertFalse(scopes.contains(scopeLeadingWhitespace));
        assertTrue(scopes.contains(scopeLeadingWhitespace.trim()));
        assertFalse(scopes.contains(scopeTrailingWhitespace));
        assertTrue(scopes.contains(scopeTrailingWhitespace.trim()));
        assertFalse(scopes.contains(scopeLeadingTrailingWhitespace));
        assertTrue(scopes.contains(scopeLeadingTrailingWhitespace.trim()));
    }
}
