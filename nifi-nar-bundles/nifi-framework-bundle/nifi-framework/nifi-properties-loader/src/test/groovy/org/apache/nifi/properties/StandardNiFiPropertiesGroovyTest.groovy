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
package org.apache.nifi.properties

import org.apache.nifi.util.NiFiProperties
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class StandardNiFiPropertiesGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(StandardNiFiPropertiesGroovyTest.class)

    private static String originalPropertiesPath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)
    private static final String PREK = NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY
    private static final String PREKID = NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_ID

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
        if (originalPropertiesPath) {
            System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, originalPropertiesPath)
        }
    }

    private static StandardNiFiProperties loadFromFile(String propertiesFilePath) {
        String filePath
        try {
            filePath = StandardNiFiPropertiesGroovyTest.class.getResource(propertiesFilePath).toURI().getPath()
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Cannot load properties file due to "
                    + ex.getLocalizedMessage(), ex)
        }

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, filePath)

        StandardNiFiProperties properties = new StandardNiFiProperties()

        // clear out existing properties
        for (String prop : properties.stringPropertyNames()) {
            properties.remove(prop)
        }

        InputStream inStream = null
        try {
            inStream = new BufferedInputStream(new FileInputStream(filePath))
            properties.load(inStream)
        } catch (final Exception ex) {
            throw new RuntimeException("Cannot load properties file due to "
                    + ex.getLocalizedMessage(), ex)
        } finally {
            if (null != inStream) {
                try {
                    inStream.close()
                } catch (Exception ex) {
                    /**
                     * do nothing *
                     */
                }
            }
        }

        return properties
    }

    @Test
    void testConstructorShouldCreateNewInstance() throws Exception {
        // Arrange

        // Act
        NiFiProperties niFiProperties = new StandardNiFiProperties()
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Assert
        assert niFiProperties.size() == 0
        assert niFiProperties.getPropertyKeys() == [] as Set
    }

    @Test
    void testConstructorShouldAcceptRawProperties() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        rawProperties.setProperty("key", "value")
        logger.info("rawProperties has ${rawProperties.size()} properties: ${rawProperties.stringPropertyNames()}")
        assert rawProperties.size() == 1

        // Act
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Assert
        assert niFiProperties.size() == 1
        assert niFiProperties.getPropertyKeys() == ["key"] as Set
    }

    @Test
    void testShouldAllowMultipleInstances() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        rawProperties.setProperty("key", "value")
        logger.info("rawProperties has ${rawProperties.size()} properties: ${rawProperties.stringPropertyNames()}")
        assert rawProperties.size() == 1

        // Act
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")
        NiFiProperties emptyProperties = new StandardNiFiProperties()
        logger.info("emptyProperties has ${emptyProperties.size()} properties: ${emptyProperties.getPropertyKeys()}")

        // Assert
        assert niFiProperties.size() == 1
        assert niFiProperties.getPropertyKeys() == ["key"] as Set

        assert emptyProperties.size() == 0
        assert emptyProperties.getPropertyKeys() == [] as Set
    }

    @Test
    void testShouldGetProvenanceRepoEncryptionKeyFromDefaultProperty() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "arbitraryKeyId"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        rawProperties.setProperty(PREKID, KEY_ID)
        rawProperties.setProperty(PREK, KEY_HEX)
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getProvenanceRepoEncryptionKeyId()
        def key = niFiProperties.getProvenanceRepoEncryptionKey()
        def keys = niFiProperties.getProvenanceRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID
        assert key == KEY_HEX
        assert keys == [(KEY_ID): KEY_HEX]
    }

    @Test
    void testShouldGetProvenanceRepoEncryptionKeysFromMultipleProperties() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "arbitraryKeyId"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "arbitraryKeyId2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"
        final String KEY_ID_3 = "arbitraryKeyId3"
        final String KEY_HEX_3 = "01010101010101010101010101010101"

        rawProperties.setProperty(PREKID, KEY_ID)
        rawProperties.setProperty(PREK, KEY_HEX)
        rawProperties.setProperty("${PREK}.id.${KEY_ID_2}", KEY_HEX_2)
        rawProperties.setProperty("${PREK}.id.${KEY_ID_3}", KEY_HEX_3)
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getProvenanceRepoEncryptionKeyId()
        def key = niFiProperties.getProvenanceRepoEncryptionKey()
        def keys = niFiProperties.getProvenanceRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID
        assert key == KEY_HEX
        assert keys == [(KEY_ID): KEY_HEX, (KEY_ID_2): KEY_HEX_2, (KEY_ID_3): KEY_HEX_3]
    }

    @Test
    void testShouldGetProvenanceRepoEncryptionKeysWithNoDefaultDefined() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "arbitraryKeyId"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "arbitraryKeyId2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"
        final String KEY_ID_3 = "arbitraryKeyId3"
        final String KEY_HEX_3 = "01010101010101010101010101010101"

        rawProperties.setProperty(PREKID, KEY_ID)
        rawProperties.setProperty("${PREK}.id.${KEY_ID}", KEY_HEX)
        rawProperties.setProperty("${PREK}.id.${KEY_ID_2}", KEY_HEX_2)
        rawProperties.setProperty("${PREK}.id.${KEY_ID_3}", KEY_HEX_3)
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getProvenanceRepoEncryptionKeyId()
        def key = niFiProperties.getProvenanceRepoEncryptionKey()
        def keys = niFiProperties.getProvenanceRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID
        assert key == KEY_HEX
        assert keys == [(KEY_ID): KEY_HEX, (KEY_ID_2): KEY_HEX_2, (KEY_ID_3): KEY_HEX_3]
    }

    @Test
    void testShouldGetProvenanceRepoEncryptionKeysWithNoneDefined() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getProvenanceRepoEncryptionKeyId()
        def key = niFiProperties.getProvenanceRepoEncryptionKey()
        def keys = niFiProperties.getProvenanceRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == null
        assert key == null
        assert keys == [:]
    }

    @Test
    void testShouldNotGetProvenanceRepoEncryptionKeysIfFileBasedKeyProvider() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "arbitraryKeyId"

        rawProperties.setProperty(PREKID, KEY_ID)
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getProvenanceRepoEncryptionKeyId()
        def key = niFiProperties.getProvenanceRepoEncryptionKey()
        def keys = niFiProperties.getProvenanceRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID
        assert key == null
        assert keys == [:]
    }

    @Test
    void testGetProvenanceRepoEncryptionKeysShouldFilterOtherProperties() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "arbitraryKeyId"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "arbitraryKeyId2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"
        final String KEY_ID_3 = "arbitraryKeyId3"
        final String KEY_HEX_3 = "01010101010101010101010101010101"

        rawProperties.setProperty(PREKID, KEY_ID)
        rawProperties.setProperty("${PREK}.id.${KEY_ID}", KEY_HEX)
        rawProperties.setProperty("${PREK}.id.${KEY_ID_2}", KEY_HEX_2)
        rawProperties.setProperty("${PREK}.id.${KEY_ID_3}", KEY_HEX_3)
        rawProperties.setProperty(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS, "some.class.provider")
        rawProperties.setProperty(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_LOCATION, "some://url")
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getProvenanceRepoEncryptionKeyId()
        def key = niFiProperties.getProvenanceRepoEncryptionKey()
        def keys = niFiProperties.getProvenanceRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID
        assert key == KEY_HEX
        assert keys == [(KEY_ID): KEY_HEX, (KEY_ID_2): KEY_HEX_2, (KEY_ID_3): KEY_HEX_3]
    }
}
