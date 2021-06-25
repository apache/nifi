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

package org.apache.nifi.controller.repository

import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.controller.repository.claim.ResourceClaimManager
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager
import org.apache.nifi.security.kms.EncryptionException
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestName
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.wali.SerDe

import java.security.Security

import static org.apache.nifi.security.kms.CryptoUtils.STATIC_KEY_PROVIDER_CLASS_NAME

@RunWith(JUnit4.class)
class EncryptedRepositoryRecordSerdeFactoryTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedRepositoryRecordSerdeFactoryTest.class)

    private ResourceClaimManager claimManager

    private static final String KEY_ID = "K1"
    private static final String KEY_1_HEX = "0123456789ABCDEFFEDCBA98765432100123456789ABCDEFFEDCBA9876543210"

    private NiFiProperties mockNiFiProperties

    @Rule
    public TestName testName = new TestName()

    @BeforeClass
    static void setUpOnce() throws Exception {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS)
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
        claimManager = new StandardResourceClaimManager()

        Map flowfileEncryptionProps = [
                (NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS): STATIC_KEY_PROVIDER_CLASS_NAME,
                (NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY)                              : KEY_1_HEX,
                (NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_ID)                           : KEY_ID
        ]
        mockNiFiProperties = new NiFiProperties(new Properties(flowfileEncryptionProps))
    }

    @After
    void tearDown() throws Exception {
        claimManager.purge()
    }

    @Test
    void testShouldCreateEncryptedSerde() {
        // Arrange
        EncryptedRepositoryRecordSerdeFactory factory = new EncryptedRepositoryRecordSerdeFactory(claimManager, mockNiFiProperties)

        // Act
        SerDe<RepositoryRecord> serde = factory.createSerDe(EncryptedSchemaRepositoryRecordSerde.class.name)
        logger.info("Created serde: ${serde} ")

        // Assert
        assert serde instanceof EncryptedSchemaRepositoryRecordSerde
    }

    @Test
    void testShouldCreateEncryptedSerdeForNullEncoding() {
        // Arrange
        EncryptedRepositoryRecordSerdeFactory factory = new EncryptedRepositoryRecordSerdeFactory(claimManager, mockNiFiProperties)

        // Act
        SerDe<RepositoryRecord> serde = factory.createSerDe(null)
        logger.info("Created serde: ${serde} ")

        // Assert
        assert serde instanceof EncryptedSchemaRepositoryRecordSerde
    }

    @Test
    void testShouldCreateStandardSerdeForStandardEncoding() {
        // Arrange
        EncryptedRepositoryRecordSerdeFactory factory = new EncryptedRepositoryRecordSerdeFactory(claimManager, mockNiFiProperties)

        // Act
        SerDe<RepositoryRecord> serde = factory.createSerDe(SchemaRepositoryRecordSerde.class.name)
        logger.info("Created serde: ${serde} ")

        // Assert
        assert serde instanceof SchemaRepositoryRecordSerde
    }

    @Test
    void testCreateSerDeShouldFailWithUnpopulatedNiFiProperties() {
        // Arrange
        NiFiProperties emptyNiFiProperties = new NiFiProperties(new Properties([:]))

        // Act
        def msg = shouldFail(EncryptionException) {
            EncryptedRepositoryRecordSerdeFactory factory = new EncryptedRepositoryRecordSerdeFactory(claimManager, emptyNiFiProperties)
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "The flowfile repository encryption configuration is not valid"
    }

    @Test
    void testConstructorShouldFailWithInvalidNiFiProperties() {
        // Arrange
        Map invalidFlowfileEncryptionProps = [
                (NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS): STATIC_KEY_PROVIDER_CLASS_NAME.reverse(),
                (NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY)                              : KEY_1_HEX,
                (NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_ID)                           : KEY_ID
        ]
        NiFiProperties invalidNiFiProperties = new NiFiProperties(new Properties(invalidFlowfileEncryptionProps))

        // Act
        def msg = shouldFail(EncryptionException) {
            EncryptedRepositoryRecordSerdeFactory factory = new EncryptedRepositoryRecordSerdeFactory(claimManager, invalidNiFiProperties)
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "The flowfile repository encryption configuration is not valid"
    }
}
