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
class NiFiPropertiesGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(NiFiPropertiesGroovyTest.class)

    private static String originalPropertiesPath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)
    private static final String PREK = NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY
    private static final String PREKID = NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_ID

    private static final String CREK = NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY
    private static final String CREKID = NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_ID

    private static final String FFREK = NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY
    private static final String FFREKID = NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_ID

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

    private static NiFiProperties loadFromFile(String propertiesFilePath) {
        String filePath
        try {
            filePath = NiFiPropertiesGroovyTest.class.getResource(propertiesFilePath).toURI().getPath()
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Cannot load properties file due to " +
                    ex.getLocalizedMessage(), ex)
        }

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, filePath)

        NiFiProperties properties = new NiFiProperties()

        // clear out existing properties
        for (String prop : properties.stringPropertyNames()) {
            properties.remove(prop)
        }

        InputStream inStream = null
        try {
            inStream = new BufferedInputStream(new FileInputStream(filePath))
            properties.load(inStream)
        } catch (final Exception ex) {
            throw new RuntimeException("Cannot load properties file due to " +
                    ex.getLocalizedMessage(), ex)
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
        NiFiProperties niFiProperties = new NiFiProperties()
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
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
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
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")
        NiFiProperties emptyProperties = new NiFiProperties()
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
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
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
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
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
    void testShouldGetFlowFileRepoEncryptionKeysFromMultiplePropertiesAfterMigration() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "K1"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "K2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"

        /**
         * This simulates after the initial key migration (K1 -> K2). The {@code nifi.properties} will look like:
         *
         * nifi.flowfile.repository.encryption.key.id=K2
         * nifi.flowfile.repository.encryption.key=
         * nifi.flowfile.repository.encryption.key.id.K1=0123456789ABCDEFFEDCBA9876543210
         * nifi.flowfile.repository.encryption.key.id.K2=00000000000000000000000000000000
         */

        rawProperties.setProperty(FFREKID, KEY_ID_2)
        rawProperties.setProperty(FFREK, "")
        rawProperties.setProperty("${FFREK}.id.${KEY_ID}", KEY_HEX)
        rawProperties.setProperty("${FFREK}.id.${KEY_ID_2}", KEY_HEX_2)
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getFlowFileRepoEncryptionKeyId()
        def key = niFiProperties.getFlowFileRepoEncryptionKey()
        def keys = niFiProperties.getFlowFileRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID_2
        assert key == KEY_HEX_2
        assert keys == [(KEY_ID): KEY_HEX, (KEY_ID_2): KEY_HEX_2]
    }

    @Test
    void testShouldGetFlowFileRepoEncryptionKeysFromMultiplePropertiesWithoutExplicitKey() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "K1"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "K2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"

        /**
         * This simulates after the initial key migration (K1 -> K2). The {@code nifi.properties} will look like:
         *
         * (note no nifi.flowfile.repository.encryption.key=)
         * nifi.flowfile.repository.encryption.key.id=K2
         * nifi.flowfile.repository.encryption.key.id.K1=0123456789ABCDEFFEDCBA9876543210
         * nifi.flowfile.repository.encryption.key.id.K2=00000000000000000000000000000000
         */

        rawProperties.setProperty(FFREKID, KEY_ID_2)
//        rawProperties.setProperty(FFREK, "")
        rawProperties.setProperty("${FFREK}.id.${KEY_ID}", KEY_HEX)
        rawProperties.setProperty("${FFREK}.id.${KEY_ID_2}", KEY_HEX_2)
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getFlowFileRepoEncryptionKeyId()
        def key = niFiProperties.getFlowFileRepoEncryptionKey()
        def keys = niFiProperties.getFlowFileRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID_2
        assert key == KEY_HEX_2
        assert keys == [(KEY_ID): KEY_HEX, (KEY_ID_2): KEY_HEX_2]
    }

    @Test
    void testGetFlowFileRepoEncryptionKeysShouldWarnOnMisformattedProperties() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "K1"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "K2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"

        /**
         * This simulates after the initial key migration (K1 -> K2) when the admin has mistyped. The {@code nifi.properties} will look like:
         *
         * (note no nifi.flowfile.repository.encryption.key=)
         * nifi.flowfile.repository.encryption.key.id=K2
         * nifi.flowfile.repository.encryption.key.K1=0123456789ABCDEFFEDCBA9876543210
         * nifi.flowfile.repository.encryption.key.K2=00000000000000000000000000000000
         *
         * The above properties should have ...key.id.K1= but they are missing the "id" segment
         */

        rawProperties.setProperty(FFREKID, KEY_ID_2)
//        rawProperties.setProperty(FFREK, "")
        rawProperties.setProperty("${FFREK}.${KEY_ID}", KEY_HEX)
        rawProperties.setProperty("${FFREK}.${KEY_ID_2}", KEY_HEX_2)
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getFlowFileRepoEncryptionKeyId()
        def key = niFiProperties.getFlowFileRepoEncryptionKey()
        def keys = niFiProperties.getFlowFileRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID_2
        assert !key
        assert keys == [:]
    }

    @Test
    void testShouldGetFlowFileRepoEncryptionKeysFromMultiplePropertiesWithDuplicates() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "K1"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "K2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"
        final String KEY_HEX_DUP = "AA" * 16

        /**
         * This simulates after the initial key migration (K1 -> K2) with a mistaken duplication. The
         * {@code nifi.properties} will look like:
         *
         * nifi.flowfile.repository.encryption.key.id=K2
         * nifi.flowfile.repository.encryption.key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
         * nifi.flowfile.repository.encryption.key.id.K1=0123456789ABCDEFFEDCBA9876543210
         * nifi.flowfile.repository.encryption.key.id.K2=00000000000000000000000000000000
         *
         * The value of K2 wil be AAAA..., overriding key.K2 as .key will always win
         */

        /**
         * The properties loading code will print a warning if it detects duplicates, but will not stop execution
         */
        rawProperties.setProperty(FFREKID, KEY_ID_2)
        rawProperties.setProperty(FFREK, KEY_HEX_DUP)
        rawProperties.setProperty("${FFREK}.id.${KEY_ID}", KEY_HEX)
        rawProperties.setProperty("${FFREK}.id.${KEY_ID_2}", KEY_HEX_2)
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getFlowFileRepoEncryptionKeyId()
        def key = niFiProperties.getFlowFileRepoEncryptionKey()
        def keys = niFiProperties.getFlowFileRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID_2
        assert key == KEY_HEX_2
        assert keys == [(KEY_ID): KEY_HEX, (KEY_ID_2): KEY_HEX_DUP]
    }

    @Test
    void testShouldGetFlowFileRepoEncryptionKeysFromMultiplePropertiesWithDuplicatesInReverseOrder() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "K1"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "K2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"
        final String KEY_HEX_DUP = "AA" * 16

        /**
         * This simulates after the initial key migration (K1 -> K2) with a mistaken duplication. The
         * {@code nifi.properties} will look like:
         *
         * nifi.flowfile.repository.encryption.key.id.K1=0123456789ABCDEFFEDCBA9876543210
         * nifi.flowfile.repository.encryption.key.id.K2=00000000000000000000000000000000
         * nifi.flowfile.repository.encryption.key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
         * nifi.flowfile.repository.encryption.key.id=K2
         *
         * The value of K2 wil be AAAA..., overriding key.K2 as .key will always win
         */

        /**
         * The properties loading code will print a warning if it detects duplicates, but will not stop execution
         */
        rawProperties.setProperty("${FFREK}.id.${KEY_ID_2}", KEY_HEX_2)
        rawProperties.setProperty("${FFREK}.id.${KEY_ID}", KEY_HEX)
        rawProperties.setProperty(FFREK, KEY_HEX_DUP)
        rawProperties.setProperty(FFREKID, KEY_ID_2)
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getFlowFileRepoEncryptionKeyId()
        def key = niFiProperties.getFlowFileRepoEncryptionKey()
        def keys = niFiProperties.getFlowFileRepoEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID_2
        assert key == KEY_HEX_2
        assert keys == [(KEY_ID): KEY_HEX, (KEY_ID_2): KEY_HEX_DUP]
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
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
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
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
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
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
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
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
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
    void testShouldGetContentRepositoryEncryptionKeyFromDefaultProperty() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "arbitraryKeyId"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        rawProperties.setProperty(CREKID, KEY_ID)
        rawProperties.setProperty(CREK, KEY_HEX)
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getContentRepositoryEncryptionKeyId()
        def key = niFiProperties.getContentRepositoryEncryptionKey()
        def keys = niFiProperties.getContentRepositoryEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID
        assert key == KEY_HEX
        assert keys == [(KEY_ID): KEY_HEX]
    }

    @Test
    void testShouldGetContentRepositoryEncryptionKeysFromMultipleProperties() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "arbitraryKeyId"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "arbitraryKeyId2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"
        final String KEY_ID_3 = "arbitraryKeyId3"
        final String KEY_HEX_3 = "01010101010101010101010101010101"

        rawProperties.setProperty(CREKID, KEY_ID)
        rawProperties.setProperty(CREK, KEY_HEX)
        rawProperties.setProperty("${CREK}.id.${KEY_ID_2}", KEY_HEX_2)
        rawProperties.setProperty("${CREK}.id.${KEY_ID_3}", KEY_HEX_3)
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getContentRepositoryEncryptionKeyId()
        def key = niFiProperties.getContentRepositoryEncryptionKey()
        def keys = niFiProperties.getContentRepositoryEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID
        assert key == KEY_HEX
        assert keys == [(KEY_ID): KEY_HEX, (KEY_ID_2): KEY_HEX_2, (KEY_ID_3): KEY_HEX_3]
    }

    @Test
    void testShouldGetContentRepositoryEncryptionKeysWithNoDefaultDefined() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "arbitraryKeyId"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "arbitraryKeyId2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"
        final String KEY_ID_3 = "arbitraryKeyId3"
        final String KEY_HEX_3 = "01010101010101010101010101010101"

        rawProperties.setProperty(CREKID, KEY_ID)
        rawProperties.setProperty("${CREK}.id.${KEY_ID}", KEY_HEX)
        rawProperties.setProperty("${CREK}.id.${KEY_ID_2}", KEY_HEX_2)
        rawProperties.setProperty("${CREK}.id.${KEY_ID_3}", KEY_HEX_3)
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getContentRepositoryEncryptionKeyId()
        def key = niFiProperties.getContentRepositoryEncryptionKey()
        def keys = niFiProperties.getContentRepositoryEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID
        assert key == KEY_HEX
        assert keys == [(KEY_ID): KEY_HEX, (KEY_ID_2): KEY_HEX_2, (KEY_ID_3): KEY_HEX_3]
    }

    @Test
    void testShouldGetContentRepositoryEncryptionKeysWithNoneDefined() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getContentRepositoryEncryptionKeyId()
        def key = niFiProperties.getContentRepositoryEncryptionKey()
        def keys = niFiProperties.getContentRepositoryEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == null
        assert key == null
        assert keys == [:]
    }

    @Test
    void testShouldNotGetContentRepositoryEncryptionKeysIfFileBasedKeyProvider() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "arbitraryKeyId"

        rawProperties.setProperty(CREKID, KEY_ID)
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getContentRepositoryEncryptionKeyId()
        def key = niFiProperties.getContentRepositoryEncryptionKey()
        def keys = niFiProperties.getContentRepositoryEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID
        assert key == null
        assert keys == [:]
    }

    @Test
    void testGetContentRepoEncryptionKeysShouldFilterOtherProperties() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        final String KEY_ID = "arbitraryKeyId"
        final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
        final String KEY_ID_2 = "arbitraryKeyId2"
        final String KEY_HEX_2 = "AAAABBBBCCCCDDDDEEEEFFFF00001111"
        final String KEY_ID_3 = "arbitraryKeyId3"
        final String KEY_HEX_3 = "01010101010101010101010101010101"

        rawProperties.setProperty(CREKID, KEY_ID)
        rawProperties.setProperty("${CREK}.id.${KEY_ID}", KEY_HEX)
        rawProperties.setProperty("${CREK}.id.${KEY_ID_2}", KEY_HEX_2)
        rawProperties.setProperty("${CREK}.id.${KEY_ID_3}", KEY_HEX_3)
        rawProperties.setProperty(NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS, "some.class.provider")
        rawProperties.setProperty(NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION, "some://url")
        NiFiProperties niFiProperties = new NiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Act
        def keyId = niFiProperties.getContentRepositoryEncryptionKeyId()
        def key = niFiProperties.getContentRepositoryEncryptionKey()
        def keys = niFiProperties.getContentRepositoryEncryptionKeys()

        logger.info("Retrieved key ID: ${keyId}")
        logger.info("Retrieved key: ${key}")
        logger.info("Retrieved keys: ${keys}")

        // Assert
        assert keyId == KEY_ID
        assert key == KEY_HEX
        assert keys == [(KEY_ID): KEY_HEX, (KEY_ID_2): KEY_HEX_2, (KEY_ID_3): KEY_HEX_3]
    }

    @Test
    void testShouldNormalizeContextPathProperty() {
        // Arrange
        String noLeadingSlash = "some/context/path"
        Properties rawProps = new Properties(["nifi.web.proxy.context.path": noLeadingSlash])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw context path property [${noLeadingSlash}]")

        // Act
        String normalizedContextPath = props.getAllowedContextPaths()
        logger.info("Read from NiFiProperties instance: ${normalizedContextPath}")

        // Assert
        assert normalizedContextPath == "/" + noLeadingSlash
    }

    @Test
    void testShouldHandleNormalizedContextPathProperty() {
        // Arrange
        String leadingSlash = "/some/context/path"
        Properties rawProps = new Properties(["nifi.web.proxy.context.path": leadingSlash])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw context path property [${leadingSlash}]")

        // Act
        String normalizedContextPath = props.getAllowedContextPaths()
        logger.info("Read from NiFiProperties instance: ${normalizedContextPath}")

        // Assert
        assert normalizedContextPath == leadingSlash
    }

    @Test
    void testShouldNormalizeMultipleContextPathsInProperty() {
        // Arrange
        String noLeadingSlash = "some/context/path"
        String leadingSlash = "some/other/path"
        String leadingAndTrailingSlash = "/a/third/path/"
        List<String> paths = [noLeadingSlash, leadingSlash, leadingAndTrailingSlash]
        String combinedPaths = paths.join(",")
        Properties rawProps = new Properties(["nifi.web.proxy.context.path": combinedPaths])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw context path property [${noLeadingSlash}]")

        // Act
        String normalizedContextPath = props.getAllowedContextPaths()
        logger.info("Read from NiFiProperties instance: ${normalizedContextPath}")

        // Assert
        def splitPaths = normalizedContextPath.split(",")
        splitPaths.every {
            assert it.startsWith("/")
            assert !it.endsWith("/")
        }
    }

    @Test
    void testShouldHandleNormalizedContextPathPropertyAsList() {
        // Arrange
        String leadingSlash = "/some/context/path"
        Properties rawProps = new Properties(["nifi.web.proxy.context.path": leadingSlash])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw context path property [${leadingSlash}]")

        // Act
        def normalizedContextPaths = props.getAllowedContextPathsAsList()
        logger.info("Read from NiFiProperties instance: ${normalizedContextPaths}")

        // Assert
        assert normalizedContextPaths.size() == 1
        assert normalizedContextPaths.contains(leadingSlash)
    }

    @Test
    void testShouldNormalizeMultipleContextPathsInPropertyAsList() {
        // Arrange
        String noLeadingSlash = "some/context/path"
        String leadingSlash = "/some/other/path"
        String leadingAndTrailingSlash = "/a/third/path/"
        List<String> paths = [noLeadingSlash, leadingSlash, leadingAndTrailingSlash]
        String combinedPaths = paths.join(",")
        Properties rawProps = new Properties(["nifi.web.proxy.context.path": combinedPaths])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw context path property [${noLeadingSlash}]")

        // Act
        def normalizedContextPaths = props.getAllowedContextPathsAsList()
        logger.info("Read from NiFiProperties instance: ${normalizedContextPaths}")

        // Assert
        assert normalizedContextPaths.size() == 3
        assert normalizedContextPaths.containsAll([leadingSlash, "/" + noLeadingSlash, leadingAndTrailingSlash[0..-2]])
    }

    @Test
    void testShouldHandleNormalizingEmptyContextPathProperty() {
        // Arrange
        String empty = ""
        Properties rawProps = new Properties(["nifi.web.proxy.context.path": empty])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw context path property [${empty}]")

        // Act
        String normalizedContextPath = props.getAllowedContextPaths()
        logger.info("Read from NiFiProperties instance: ${normalizedContextPath}")

        // Assert
        assert normalizedContextPath == empty
    }

    @Test
    void testShouldNormalizeProxyHostProperty() {
        // Arrange
        String extraSpaceHostname = "somehost.com  "
        Properties rawProps = new Properties(["nifi.web.proxy.host": extraSpaceHostname])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw proxy host property [${extraSpaceHostname}]")

        // Act
        String normalizedHostname = props.getAllowedHosts()
        logger.info("Read from NiFiProperties instance: ${normalizedHostname}")

        // Assert
        assert extraSpaceHostname.startsWith(normalizedHostname)
        assert extraSpaceHostname.length() == normalizedHostname.length() + 2
    }

    @Test
    void testShouldHandleNormalizedProxyHostProperty() {
        // Arrange
        String hostname = "somehost.com"
        Properties rawProps = new Properties(["nifi.web.proxy.host": hostname])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw proxy host property [${hostname}]")

        // Act
        String normalizedHostname = props.getAllowedHosts()
        logger.info("Read from NiFiProperties instance: ${normalizedHostname}")

        // Assert
        assert hostname == normalizedHostname
    }

    @Test
    void testShouldNormalizeMultipleProxyHostsInProperty() {
        // Arrange
        String extraSpaceHostname = "somehost.com  "
        String normalHostname = "someotherhost.com"
        String hostnameWithPort = "otherhost.com:1234"
        String extraSpaceHostnameWithPort = "  anotherhost.com:9999"
        List<String> hosts = [extraSpaceHostname, normalHostname, hostnameWithPort, extraSpaceHostnameWithPort]
        String combinedHosts = hosts.join(",")
        Properties rawProps = new Properties(["nifi.web.proxy.host": combinedHosts])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw proxy host property [${combinedHosts}]")

        // Act
        String normalizedHostname = props.getAllowedHosts()
        logger.info("Read from NiFiProperties instance: ${normalizedHostname}")

        // Assert
        def splitHosts = normalizedHostname.split(",")
        def expectedValues = hosts*.trim()
        splitHosts.every {
            assert it.trim() == it
            assert expectedValues.contains(it)
        }
    }

    @Test
    void testShouldHandleNormalizedProxyHostPropertyAsList() {
        // Arrange
        String normalHostname = "someotherhost.com"
        Properties rawProps = new Properties(["nifi.web.proxy.host": normalHostname])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw proxy host property [${normalHostname}]")

        // Act
        def listOfHosts = props.getAllowedHostsAsList()
        logger.info("Read from NiFiProperties instance: ${listOfHosts}")

        // Assert
        assert listOfHosts.size() == 1
        assert listOfHosts.contains(normalHostname)
    }

    @Test
    void testShouldNormalizeMultipleProxyHostsInPropertyAsList() {
        // Arrange
        String extraSpaceHostname = "somehost.com  "
        String normalHostname = "someotherhost.com"
        String hostnameWithPort = "otherhost.com:1234"
        String extraSpaceHostnameWithPort = "  anotherhost.com:9999"
        List<String> hosts = [extraSpaceHostname, normalHostname, hostnameWithPort, extraSpaceHostnameWithPort]
        String combinedHosts = hosts.join(",")
        Properties rawProps = new Properties(["nifi.web.proxy.host": combinedHosts])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw proxy host property [${combinedHosts}]")

        // Act
        def listOfHosts = props.getAllowedHostsAsList()
        logger.info("Read from NiFiProperties instance: ${listOfHosts}")

        // Assert
        assert listOfHosts.size() == 4
        assert listOfHosts.containsAll([extraSpaceHostname[0..-3], normalHostname, hostnameWithPort, extraSpaceHostnameWithPort[2..-1]])
    }

    @Test
    void testShouldHandleNormalizingEmptyProxyHostProperty() {
        // Arrange
        String empty = ""
        Properties rawProps = new Properties(["nifi.web.proxy.host": empty])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw proxy host property [${empty}]")

        // Act
        String normalizedHost = props.getAllowedHosts()
        logger.info("Read from NiFiProperties instance: ${normalizedHost}")

        // Assert
        assert normalizedHost == empty
    }

    @Test
    void testShouldReturnEmptyProxyHostPropertyAsList() {
        // Arrange
        String empty = ""
        Properties rawProps = new Properties(["nifi.web.proxy.host": empty])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with raw proxy host property [${empty}]")

        // Act
        def hosts = props.getAllowedHostsAsList()
        logger.info("Read from NiFiProperties instance: ${hosts}")

        // Assert
        assert hosts.size() == 0
    }

    @Test
    void testStaticFactoryMethodShouldAcceptRawProperties() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        rawProperties.setProperty("key", "value")
        logger.info("rawProperties has ${rawProperties.size()} properties: ${rawProperties.stringPropertyNames()}")
        assert rawProperties.size() == 1

        // Act
        NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties("", rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Assert
        assert niFiProperties.size() == 1
        assert niFiProperties.getPropertyKeys() == ["key"] as Set
    }

    @Test
    void testStaticFactoryMethodShouldAcceptMap() throws Exception {
        // Arrange
        def mapProperties = ["key": "value"]
        logger.info("rawProperties has ${mapProperties.size()} properties: ${mapProperties.keySet()}")
        assert mapProperties.size() == 1

        // Act
        NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties("", mapProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Assert
        assert niFiProperties.size() == 1
        assert niFiProperties.getPropertyKeys() == ["key"] as Set
    }


    @Test
    void testWebMaxContentSizeShouldDefaultToEmpty() {
        // Arrange
        Properties rawProps = new Properties(["nifi.web.max.content.size": ""])
        NiFiProperties props = new NiFiProperties(rawProps)
        logger.info("Created a NiFiProperties instance with empty web max content size property")

        // Act
        String webMaxContentSize = props.getWebMaxContentSize()
        logger.info("Read from NiFiProperties instance: ${webMaxContentSize}")

        // Assert
        assert webMaxContentSize == ""
    }
    
    
    @Test
    void testShouldStripWhitespace() throws Exception {
        // Arrange
        File unprotectedFile = new File("src/test/resources/conf/nifi_with_whitespace.properties")
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()
        
        // Act
        NiFiProperties niFiProperties = niFiPropertiesLoader.load(unprotectedFile.path)

        // Assert
        assert niFiProperties.getProperty("nifi.whitespace.propWithNoSpace") == "foo"
        assert niFiProperties.getProperty("nifi.whitespace.propWithLeadingSpace") == "foo"
        assert niFiProperties.getProperty("nifi.whitespace.propWithTrailingSpace") == "foo"
        assert niFiProperties.getProperty("nifi.whitespace.propWithLeadingAndTrailingSpace") == "foo"
        assert niFiProperties.getProperty("nifi.whitespace.propWithTrailingTab") == "foo"
        assert niFiProperties.getProperty("nifi.whitespace.propWithMultipleLines") == "foobarbaz"
    }
}
