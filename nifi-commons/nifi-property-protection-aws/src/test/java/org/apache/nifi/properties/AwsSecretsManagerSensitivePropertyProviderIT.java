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

import org.apache.nifi.properties.configuration.AwsSecretsManagerClientProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * To run this test, make sure to first configure sensitive credential information as in the following link
 * https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
 *
 * The following settings are optional. If you have a default AWS configuration and credentials in ~/.aws then
 * it will take that. Otherwise you can set all of the following:
 * set the system property -Daws.access.key.id to the access key id
 * set the system property -Daws.secret.access.key to the secret access key
 * set the system property -Daws.region to the region
 *
 * After you are satisfied with the test, and you don't need the key, you may schedule secret deletion with:
 * aws secretsmanager delete-secret --secret-id "default/property" --recovery-window-in-days 14
 *
 */

public class AwsSecretsManagerSensitivePropertyProviderIT {
    private static final String SAMPLE_PLAINTEXT = "AWSSecretsManagerSensitivePropertyProviderIT SAMPLE-PLAINTEXT";
    private static final String ACCESS_KEY_PROPS_NAME = "aws.access.key.id";
    private static final String SECRET_KEY_PROPS_NAME = "aws.secret.access.key";
    private static final String REGION_KEY_PROPS_NAME = "aws.region";

    private static final String BOOTSTRAP_AWS_FILE_PROPS_NAME = "nifi.bootstrap.protection.aws.conf";

    private static final String EMPTY_PROPERTY = "";

    private static AwsSecretsManagerSensitivePropertyProvider spp;

    private static BootstrapProperties props;

    private static Path mockBootstrapConf, mockAWSBootstrapConf;

    private static final Logger logger = LoggerFactory.getLogger(AwsSecretsManagerSensitivePropertyProviderIT.class);

    private static void initializeBootstrapProperties() throws IOException{
        mockBootstrapConf = Files.createTempFile("bootstrap", ".conf").toAbsolutePath();
        mockAWSBootstrapConf = Files.createTempFile("bootstrap-aws", ".conf").toAbsolutePath();
        IOUtil.writeText(BOOTSTRAP_AWS_FILE_PROPS_NAME + "=" + mockAWSBootstrapConf.toAbsolutePath(), mockBootstrapConf.toFile());

        final Properties bootstrapProperties = new Properties();
        try (final InputStream inputStream = Files.newInputStream(mockBootstrapConf)) {
            bootstrapProperties.load(inputStream);
            props = new BootstrapProperties("nifi", bootstrapProperties, mockBootstrapConf);
        }

        String accessKey = System.getProperty(ACCESS_KEY_PROPS_NAME, EMPTY_PROPERTY);
        String secretKey = System.getProperty(SECRET_KEY_PROPS_NAME, EMPTY_PROPERTY);
        String region = System.getProperty(REGION_KEY_PROPS_NAME, EMPTY_PROPERTY);

        StringBuilder bootstrapConfText = new StringBuilder();
        String lineSeparator = System.getProperty("line.separator");
        bootstrapConfText.append(ACCESS_KEY_PROPS_NAME + "=" + accessKey);
        bootstrapConfText.append(lineSeparator + SECRET_KEY_PROPS_NAME + "=" + secretKey);
        bootstrapConfText.append(lineSeparator + REGION_KEY_PROPS_NAME + "=" + region);

        IOUtil.writeText(bootstrapConfText.toString(), mockAWSBootstrapConf.toFile());
    }

    @BeforeAll
    public static void initOnce() throws IOException {
        initializeBootstrapProperties();
        assertNotNull(props);
        final AwsSecretsManagerClientProvider provider = new AwsSecretsManagerClientProvider();
        final Properties properties = provider.getClientProperties(props).orElse(null);
        final SecretsManagerClient secretsManagerClient = provider.getClient(properties).orElse(null);
        spp = new AwsSecretsManagerSensitivePropertyProvider(secretsManagerClient);
        assertNotNull(spp);
    }

    @AfterAll
    public static void tearDownOnce() throws IOException {
        Files.deleteIfExists(mockBootstrapConf);
        Files.deleteIfExists(mockAWSBootstrapConf);

        spp.cleanUp();
    }

    @Test
    public void testEncryptDecrypt() {
        logger.info("Running testEncryptDecrypt of AWS Secrets Manager SPP integration test");
        runEncryptDecryptTest();
        logger.info("testEncryptDecrypt of AWS Secrets Manager SPP integration test completed");
    }

    private static void runEncryptDecryptTest() {
        final String propertyName = "property2";
        logger.info("Plaintext: " + SAMPLE_PLAINTEXT);
        String protectedValue = spp.protect(SAMPLE_PLAINTEXT, ProtectedPropertyContext.defaultContext(propertyName));
        logger.info("Protected Value: " + protectedValue);
        String unprotectedValue = spp.unprotect(protectedValue, ProtectedPropertyContext.defaultContext(propertyName));
        logger.info("Unprotected Value: " + unprotectedValue);

        assertEquals(SAMPLE_PLAINTEXT, unprotectedValue);
        assertNotEquals(SAMPLE_PLAINTEXT, protectedValue);
        assertNotEquals(protectedValue, unprotectedValue);
    }
}
