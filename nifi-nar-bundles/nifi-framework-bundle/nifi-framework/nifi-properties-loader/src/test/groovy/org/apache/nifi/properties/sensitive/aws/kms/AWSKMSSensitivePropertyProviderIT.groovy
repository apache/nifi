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
package org.apache.nifi.properties.sensitive.aws.kms

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.kms.AWSKMSClient
import com.amazonaws.services.kms.AWSKMSClientBuilder
import com.amazonaws.services.kms.model.CreateAliasRequest
import com.amazonaws.services.kms.model.CreateKeyRequest
import com.amazonaws.services.kms.model.CreateKeyResult
import com.amazonaws.services.kms.model.DescribeKeyRequest
import com.amazonaws.services.kms.model.DescribeKeyResult
import com.amazonaws.services.kms.model.GenerateDataKeyRequest
import com.amazonaws.services.kms.model.GenerateDataKeyResult
import com.amazonaws.services.kms.model.ScheduleKeyDeletionRequest
import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.properties.sensitive.ProtectedNiFiProperties
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider
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

import java.security.SecureRandom


@RunWith(JUnit4.class)
class AWSKMSSensitivePropertyProviderIT extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(AWSKMSSensitivePropertyProviderIT.class)
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    private static String[] knownGoodKeys = []
    private static AWSKMSClient client

    /**
     * This method creates a CMK, DEK, and an alias to that DEK for exercising the AWS KMS calls.
     *
     * @throws Exception
     */
    @BeforeClass
    static void setUpOnce() throws Exception {
        final FileInputStream fis
        try {
            fis = new FileInputStream(CREDENTIALS_FILE)
        } catch (FileNotFoundException e1) {
            fail("Could not open credentials file " + CREDENTIALS_FILE + ": " + e1.getLocalizedMessage());
            return
        }
        final PropertiesCredentials credentials = new PropertiesCredentials(fis)

        // We're copying the properties directly so the standard builder works.
        System.setProperty("aws.accessKeyId", credentials.AWSAccessKeyId)
        System.setProperty("aws.secretKey", credentials.AWSSecretKey)
        System.setProperty("aws.region", "us-east-2")

        client = AWSKMSClientBuilder.standard().build() as AWSKMSClient

        // generate a cmk
        CreateKeyRequest cmkRequest = new CreateKeyRequest().withDescription("CMK for unit tests")
        CreateKeyResult cmkResult = client.createKey(cmkRequest)

        // from the cmk, generate a dek
        GenerateDataKeyRequest dekRequest = new GenerateDataKeyRequest().withKeyId(cmkResult.keyMetadata.getKeyId()).withKeySpec("AES_128")
        GenerateDataKeyResult dekResult = client.generateDataKey(dekRequest)

        // add an alias to the dek
        final String aliasName = "alias/aws-kms-spp-integration-test-" + UUID.randomUUID().toString()
        CreateAliasRequest aliasReq = new CreateAliasRequest().withAliasName(aliasName).withTargetKeyId(dekResult.getKeyId())
        client.createAlias(aliasReq)

        // re-read the dek so we have the arn
        DescribeKeyRequest descRequest = new DescribeKeyRequest().withKeyId(dekResult.getKeyId())
        DescribeKeyResult descResult = client.describeKey(descRequest)

        knownGoodKeys = [
                dekResult.getKeyId(),
                descResult.keyMetadata.getArn(),
                aliasName
        ]
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    /**
     * This method schedules the deletion of the CMK created during setup.  The delete will cascade to the DEK and DEK alias.
     */
    @AfterClass
    static void tearDownOnce() {
        if (knownGoodKeys.size() > 0) {
            ScheduleKeyDeletionRequest req = new ScheduleKeyDeletionRequest().withKeyId(knownGoodKeys[0]).withPendingWindowInDays(7)
            client.scheduleKeyDeletion(req)
        }
    }


    /**
     * This test shows that bad keys lead to exceptions, not invalid instances.
     */
    @Test
    void testShouldThrowExceptionsWithBadKeys() throws Exception {
        SensitivePropertyProvider propProvider
        String msg

        msg = shouldFail(SensitivePropertyProtectionException) {
            propProvider = new AWSKMSSensitivePropertyProvider("")
        }
        assert msg =~ "The key cannot be empty"


        assert propProvider == null

        msg = shouldFail(com.amazonaws.services.kms.model.NotFoundException) {
            propProvider = new AWSKMSSensitivePropertyProvider("bad key")
            propProvider.protect("value")
        }
        assert msg =~ "Invalid keyId"
    }

    /**
     * These tests show that the provider with known keys can round-trip protect + unprotect random, generated text.
     */
    @Test
    void testShouldProtectAndUnprotectValues() throws Exception {
        SensitivePropertyProvider propProvider
        String plainText

        knownGoodKeys.each { k ->
            propProvider = new AWSKMSSensitivePropertyProvider(k)
            assert propProvider != null

            byte[] randBytes = new byte[1024]
            new SecureRandom().nextBytes(randBytes)
            plainText = randBytes.encodeBase64()

            assert plainText != null
            assert plainText != ""

            assert plainText == propProvider.unprotect(propProvider.protect(plainText))
        }
    }

    /**
     * These tests show that the provider cannot encrypt empty values.
     */
    @Test
    void testShouldHandleProtectEmptyValue() throws Exception {
        SensitivePropertyProvider propProvider
        final List<String> EMPTY_PLAINTEXTS = ["", "    ", null]

        knownGoodKeys.each { k ->
            propProvider = new AWSKMSSensitivePropertyProvider(k)
            assert propProvider != null

            EMPTY_PLAINTEXTS.each { String emptyPlaintext ->
                def msg = shouldFail(IllegalArgumentException) {
                    propProvider.protect(emptyPlaintext)
                }
                assert msg == "Cannot encrypt an empty value"
            }
        }
    }

    /**
     * These tests show that the provider cannot decrypt invalid ciphertext.
     */
    @Test
    void testShouldUnprotectValue() throws Exception {
        SensitivePropertyProvider propProvider
        final List<String> BAD_CIPHERTEXTS = ["any", "bad", "value"]

        knownGoodKeys.each { k ->
            propProvider = new AWSKMSSensitivePropertyProvider(k)
            assert propProvider != null

            // text that cannot be decoded values throw a bouncy castle exception:
            BAD_CIPHERTEXTS.each { String emptyPlaintext ->
                def msg = shouldFail(org.bouncycastle.util.encoders.DecoderException) {
                    propProvider.unprotect(emptyPlaintext)
                }
                assert msg != null
            }

            // Empty string throws a different exception:
            def msg = shouldFail(com.amazonaws.services.kms.model.AWSKMSException) {
                propProvider.unprotect("")
            }
            assert msg != null
        }
    }

    /**
     * These tests show we can use an AWS KMS key to encrypt/decrypt property values.
     */
    @Test
    void testShouldProtectAndUnprotectProperties() throws Exception {
        Properties rawProps
        NiFiProperties standardProps
        ProtectedNiFiProperties protectedProps
        NiFiProperties encryptedProps
        SensitivePropertyProvider propProvider
        String propKey = NiFiProperties.SENSITIVE_PROPS_KEY

        byte[] randBytes = new byte[128]
        new SecureRandom().nextBytes(randBytes)
        String clearText = "clear + random: " + randBytes.encodeHex()

        knownGoodKeys.each { awsKmsKey ->
            rawProps = new Properties()

            // set an unprotected value along with the specific key
            rawProps.setProperty(propKey, clearText)
            rawProps.setProperty(propKey + ".protected", "aws/kms/" + awsKmsKey)
            standardProps = new StandardNiFiProperties(rawProps)
            protectedProps = new ProtectedNiFiProperties(standardProps, "aws/kms/" + awsKmsKey)

            logger.info("protectedProps has ${protectedProps.size()} properties: ${protectedProps.getPropertyKeys()}")

            // check to see if the property was encrypted
            encryptedProps = protectedProps.protectPlainProperties()
            assert encryptedProps.getProperty(propKey) != clearText

            // decrypt the encrypted value manually and compare
            propProvider = new AWSKMSSensitivePropertyProvider(awsKmsKey)
            assert propProvider.unprotect(encryptedProps.getProperty(propKey)) == clearText
        }
    }
}
