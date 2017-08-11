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
package org.apache.nifi.controller.serialization

import org.apache.commons.codec.binary.Hex
import org.apache.nifi.encrypt.EncryptionException
import org.apache.nifi.encrypt.StringEncryptor
import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.security.kms.CryptoUtils
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.PBEParameterSpec
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
class FlowFromDOMFactoryTest {
    private static final Logger logger = LoggerFactory.getLogger(FlowFromDOMFactoryTest.class)

    private static final String DEFAULT_PASSWORD = "nififtw!"
    private static final byte[] DEFAULT_SALT = new byte[8]
    private static final int DEFAULT_ITERATION_COUNT = 0

    private static final String ALGO = NiFiProperties.NF_SENSITIVE_PROPS_ALGORITHM
    private static final String PROVIDER = NiFiProperties.NF_SENSITIVE_PROPS_PROVIDER
    private static final String KEY = NiFiProperties.NF_SENSITIVE_PROPS_KEY

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

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

    @Test
    void testShouldDecryptSensitiveFlowValue() throws Exception {
        // Arrange
        final String plaintext = "This is a plaintext message."

        // Encrypt the value

        // Hard-coded 0x00 * 16
        byte[] salt = new byte[16]
        Cipher cipher = generateCipher(true, DEFAULT_PASSWORD, salt)

        byte[] cipherBytes = cipher.doFinal(plaintext.bytes)
        byte[] saltAndCipherBytes = CryptoUtils.concatByteArrays(salt, cipherBytes)
        String cipherTextHex = Hex.encodeHexString(saltAndCipherBytes)
        String wrappedCipherText = "enc{${cipherTextHex}}"
        logger.info("Cipher text: ${wrappedCipherText}")

        final Map MOCK_PROPERTIES = [(ALGO): EncryptionMethod.MD5_128AES.algorithm, (PROVIDER): EncryptionMethod.MD5_128AES.provider, (KEY): DEFAULT_PASSWORD]
        NiFiProperties mockProperties = new StandardNiFiProperties(new Properties(MOCK_PROPERTIES))
        StringEncryptor flowEncryptor = StringEncryptor.createEncryptor(mockProperties)

        // Act
        String recovered = FlowFromDOMFactory.decrypt(wrappedCipherText, flowEncryptor)
        logger.info("Recovered: ${recovered}")

        // Assert
        assert plaintext == recovered
    }

    @Test
    void testShouldProvideBetterErrorMessageOnDecryptionFailure() throws Exception {
        // Arrange
        final String plaintext = "This is a plaintext message."

        // Encrypt the value

        // Hard-coded 0x00 * 16
        byte[] salt = new byte[16]
        Cipher cipher = generateCipher(true, DEFAULT_PASSWORD, salt)

        byte[] cipherBytes = cipher.doFinal(plaintext.bytes)
        byte[] saltAndCipherBytes = CryptoUtils.concatByteArrays(salt, cipherBytes)
        String cipherTextHex = Hex.encodeHexString(saltAndCipherBytes)
        String wrappedCipherText = "enc{${cipherTextHex}}"
        logger.info("Cipher text: ${wrappedCipherText}")

        // Change the password in "nifi.properties" so it doesn't match the "flow"
        final Map MOCK_PROPERTIES = [(ALGO): EncryptionMethod.MD5_128AES.algorithm, (PROVIDER): EncryptionMethod.MD5_128AES.provider, (KEY): DEFAULT_PASSWORD.reverse()]
        NiFiProperties mockProperties = new StandardNiFiProperties(new Properties(MOCK_PROPERTIES))
        StringEncryptor flowEncryptor = StringEncryptor.createEncryptor(mockProperties)

        // Act
        def msg = shouldFail(EncryptionException) {
            String recovered = FlowFromDOMFactory.decrypt(wrappedCipherText, flowEncryptor)
            logger.info("Recovered: ${recovered}")
        }
        logger.expected(msg)

        // Assert
        assert msg.message =~ "Check that the ${KEY} value in nifi.properties matches the value used to encrypt the flow.xml.gz file"
    }

    private
    static Cipher generateCipher(boolean encryptMode, String password = DEFAULT_PASSWORD, byte[] salt = DEFAULT_SALT, int iterationCount = DEFAULT_ITERATION_COUNT) {
        // Initialize secret key from password
        final PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray())
        final SecretKeyFactory factory = SecretKeyFactory.getInstance(EncryptionMethod.MD5_128AES.algorithm, EncryptionMethod.MD5_128AES.provider)
        SecretKey tempKey = factory.generateSecret(pbeKeySpec)

        final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, iterationCount)
        Cipher cipher = Cipher.getInstance(EncryptionMethod.MD5_128AES.algorithm, EncryptionMethod.MD5_128AES.provider)
        cipher.init((encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE) as int, tempKey, parameterSpec)
        cipher
    }
}
