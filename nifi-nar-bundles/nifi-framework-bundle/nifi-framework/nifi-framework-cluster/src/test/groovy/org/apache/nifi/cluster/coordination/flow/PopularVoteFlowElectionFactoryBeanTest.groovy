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
package org.apache.nifi.cluster.coordination.flow

import org.apache.nifi.encrypt.PropertyEncryptor
import org.apache.nifi.encrypt.PropertyEncryptorFactory

import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.util.NiFiProperties
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class PopularVoteFlowElectionFactoryBeanTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(PopularVoteFlowElectionFactoryBeanTest.class)

    private static final EncryptionMethod DEFAULT_ENCRYPTION_METHOD = EncryptionMethod.MD5_256AES

    private final String DEFAULT_SENSITIVE_PROPS_KEY = String.class.getName()

    @BeforeClass
    static void setUpOnce() {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {
        super.setUp()

    }

    NiFiProperties mockProperties(Map<String, String> defaults = [:]) {
        def mockProps = new NiFiProperties(new Properties([
                (NiFiProperties.SENSITIVE_PROPS_ALGORITHM):DEFAULT_ENCRYPTION_METHOD.algorithm,
                (NiFiProperties.SENSITIVE_PROPS_PROVIDER):DEFAULT_ENCRYPTION_METHOD.provider,
        ] + defaults))

        mockProps
    }

    @Test
    void testGetObjectShouldPopulateSensitivePropsKeyIfPresent() {
        // Arrange
        final String REVERSE_KEY = DEFAULT_SENSITIVE_PROPS_KEY.reverse()

        PopularVoteFlowElectionFactoryBean electionFactoryBean = new PopularVoteFlowElectionFactoryBean()
        electionFactoryBean.properties = mockProperties([(NiFiProperties.SENSITIVE_PROPS_KEY): REVERSE_KEY])

        final PropertyEncryptor REVERSE_ENCRYPTOR = PropertyEncryptorFactory.getPropertyEncryptor(mockProperties([(NiFiProperties.SENSITIVE_PROPS_KEY): REVERSE_KEY]))
        final String EXPECTED_PLAINTEXT = "my.test.value"
        final String EXPECTED_CIPHERTEXT = REVERSE_ENCRYPTOR.encrypt(EXPECTED_PLAINTEXT)
        logger.info("Expected ciphertext: ${EXPECTED_CIPHERTEXT}")

        // Act
        PopularVoteFlowElection election = electionFactoryBean.object
        logger.info("Got object: ${election}")

        // Assert

        // Violates LoD but need to evaluate nested encryptor can decrypt
        def encryptor = election.fingerprintFactory.encryptor
        String decrypted = encryptor.decrypt(EXPECTED_CIPHERTEXT)
        logger.info("Decrypted plain text: ${decrypted}")
        assert decrypted == EXPECTED_PLAINTEXT
    }

}
