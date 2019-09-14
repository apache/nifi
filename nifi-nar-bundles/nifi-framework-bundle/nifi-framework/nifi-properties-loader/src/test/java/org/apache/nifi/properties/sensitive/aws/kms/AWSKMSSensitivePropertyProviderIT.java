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
package org.apache.nifi.properties.sensitive.aws.kms;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.CreateAliasRequest;
import com.amazonaws.services.kms.model.CreateKeyRequest;
import com.amazonaws.services.kms.model.CreateKeyResult;
import com.amazonaws.services.kms.model.DescribeKeyRequest;
import com.amazonaws.services.kms.model.DescribeKeyResult;
import com.amazonaws.services.kms.model.GenerateDataKeyRequest;
import com.amazonaws.services.kms.model.GenerateDataKeyResult;
import com.amazonaws.services.kms.model.ScheduleKeyDeletionRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.AbstractSensitivePropertyProviderTest;
import org.apache.nifi.properties.sensitive.SensitivePropertyConfigurationException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.CipherUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Tests the AWS KMS Sensitive Property Provider.
 *
 * These tests rely on an environment with AWS credentials stored on disk, and require that those credentials support
 * creating and using AWS KMS keys.
 *
 * These tests do create and destroy keys and key material, but there is no chance that another existing user key is
 * effected; all ids, keys, names and aliases are either unspecified or effectively random.
 *
 * To enable these tests, add the file `aws-credentials.properties` to your home directory.  The file should have
 * `aws.accessKeyId` and `aws.secretKey` values set.
 */
public class AWSKMSSensitivePropertyProviderIT extends AbstractSensitivePropertyProviderTest {
    private static final Logger logger = LoggerFactory.getLogger(AWSKMSSensitivePropertyProviderIT.class);

    private static final Map<String, String> credentialsBeforeTest = new HashMap<>();
    private static final Map<String, String> credentialsDuringTest = new HashMap<>();
    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    private static String[] knownGoodKeys;
    private static AWSKMSClient client;

    /**
     * Before the tests are run, this method reads the aws credentials file, and when successful, sets those values as
     * system properties.
     */
    @BeforeClass
    public static void setUpOnce() throws Exception {
        final FileInputStream fis;
        try {
            fis = new FileInputStream(CREDENTIALS_FILE);
        } catch (final Exception e1) {
            logger.warn("Could not open credentials file " + CREDENTIALS_FILE + ": " + e1.getLocalizedMessage());
            Assume.assumeNoException(e1);
            return;
        }

        final PropertiesCredentials credentials = new PropertiesCredentials(fis);
        credentialsDuringTest.put("aws.accessKeyId", credentials.getAWSAccessKeyId());
        credentialsDuringTest.put("aws.secretKey", credentials.getAWSSecretKey());

        for (String name : credentialsDuringTest.keySet()) {
            String value = System.getProperty(name);
            credentialsBeforeTest.put(name, value);
            if (StringUtils.isNotBlank(value)) {
                logger.info("Overwriting credential system property: " + name);
            }
            // We're copying the properties directly so the standard builder works.
            System.setProperty(name, credentialsDuringTest.get(name));
        }
        System.setProperty("aws.region", "us-east-2");

        client = (AWSKMSClient) AWSKMSClientBuilder.standard().build();

        // Our first step is to generate a cmk (Customer Master Key):
        CreateKeyRequest cmkRequest = new CreateKeyRequest().withDescription("CMK for unit tests");
        CreateKeyResult cmkResult = client.createKey(cmkRequest);
        logger.info("Created customer master key: " + cmkResult.getKeyMetadata().getKeyId());

        // Our next step is to generate a DEK (data encryption key) from the cmk:
        GenerateDataKeyRequest dekRequest = new GenerateDataKeyRequest().withKeyId(cmkResult.getKeyMetadata().getKeyId()).withKeySpec("AES_128");
        GenerateDataKeyResult dekResult = client.generateDataKey(dekRequest);
        logger.info("Created data encryption key: " + dekResult.getKeyId());

        // Here we add an alias to the DEK to test the fact that aliases can be used in place of key ids:
        final String aliasName = "alias/aws-kms-spp-integration-test-" + UUID.randomUUID().toString();
        CreateAliasRequest aliasReq = new CreateAliasRequest().withAliasName(aliasName).withTargetKeyId(dekResult.getKeyId());
        client.createAlias(aliasReq);
        logger.info("Created key alias: " + aliasName);

        // Finally, we re-read the DEK so we have the ARN:
        DescribeKeyRequest descRequest = new DescribeKeyRequest().withKeyId(dekResult.getKeyId());
        DescribeKeyResult descResult = client.describeKey(descRequest);
        logger.info("Retrieved description for: " + descResult.getKeyMetadata().getArn());

        knownGoodKeys = new String[]{
                dekResult.getKeyId(),
                descResult.getKeyMetadata().getArn(),
                aliasName
        };
    }

    /**
     * This method schedules the deletion of the CMK created during setup.  The delete will cascade to the DEK and DEK alias.
     */
    @AfterClass
    public static void tearDownOnce() {
        if (knownGoodKeys != null && knownGoodKeys.length > 0) {
            ScheduleKeyDeletionRequest req = new ScheduleKeyDeletionRequest().withKeyId(knownGoodKeys[0]).withPendingWindowInDays(7);
            client.scheduleKeyDeletion(req);
        }
    }

    /**
     * After the tests have run, this method restores the system properties that were set during test class setup.
     */
    @AfterClass
    public static void tearDownCredentialsOnce() throws Exception {
        for (String name : credentialsBeforeTest.keySet()) {
            String value = credentialsBeforeTest.get(name);
            if (StringUtils.isNotBlank(value)) {
                logger.info("Restoring credential system property: " + name);
            }
            System.setProperty(name, value == null ? "" : value);
        }
    }

    /**
     * This test shows that bad keys lead to exceptions, not invalid instances.
     */
    @Test
    public void testShouldThrowExceptionsWithBadKeys() throws Exception {
        try {
            new AWSKMSSensitivePropertyProvider("");
        } catch (final SensitivePropertyConfigurationException e) {
            Assert.assertTrue(Pattern.compile("The key cannot be empty").matcher(e.getMessage()).matches());
        }

        try {
            new AWSKMSSensitivePropertyProvider("this is an invalid key and will not work");
        } catch (final SensitivePropertyConfigurationException e) {
            Assert.assertTrue(Pattern.compile("Invalid keyId").matcher(e.getMessage()).matches());
        }
    }

    /**
     * These tests show that the provider with known keys can round-trip protect + unprotect random, generated text.
     */
    @Test
    public void testShouldProtectAndUnprotectValues() throws Exception {
        for (String knownGoodKey : knownGoodKeys) {
            SensitivePropertyProvider sensitivePropertyProvider = new AWSKMSSensitivePropertyProvider(knownGoodKey);
            int plainSize = CipherUtils.getRandomInt(32, 256);
            checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, plainSize);
            logger.info("AES SPP protected and unprotected string of " + plainSize + " bytes using material: " + knownGoodKey);
        }
    }

    /**
     * These tests show that the provider cannot encrypt empty values.
     */
    @Test
    public void testShouldHandleProtectEmptyValue() throws Exception {
        for (String knownGoodKey : knownGoodKeys) {
            final SensitivePropertyProvider propProvider = new AWSKMSSensitivePropertyProvider(knownGoodKey);
            checkProviderProtectDoesNotAllowBlankValues(propProvider);
        }
    }

    /**
     * These tests show that the provider cannot decrypt invalid ciphertext.
     */
    @Test
    public void testShouldUnprotectValue() throws Exception {
        for (String knownGoodKey : knownGoodKeys) {
            checkProviderUnprotectDoesNotAllowInvalidBase64Values(new AWSKMSSensitivePropertyProvider(knownGoodKey));
        }
    }

    /**
     * These tests show that the provider cannot decrypt text encoded but not encrypted.
     */
    @Test
    public void testShouldThrowExceptionWithValidBase64EncodedTextInvalidCipherText() throws Exception {
        for (String knownGoodKey : knownGoodKeys) {
            checkProviderUnprotectDoesNotAllowValidBase64InvalidCipherTextValues(new AWSKMSSensitivePropertyProvider(knownGoodKey));
        }
    }

    /**
     * These tests show we can use an AWS KMS key to encrypt/decrypt property values.
     */
    @Test
    public void testShouldProtectAndUnprotectProperties() throws Exception {
        for (String knownGoodKey : knownGoodKeys) {
            checkProviderCanProtectAndUnprotectProperties(new AWSKMSSensitivePropertyProvider(knownGoodKey));
        }
    }
}
