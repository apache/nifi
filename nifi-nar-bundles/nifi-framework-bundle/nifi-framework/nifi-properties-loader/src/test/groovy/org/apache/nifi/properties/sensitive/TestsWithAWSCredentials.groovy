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
package org.apache.nifi.properties.sensitive

import com.amazonaws.auth.PropertiesCredentials
import org.apache.commons.lang3.StringUtils
import org.junit.AfterClass
import org.junit.BeforeClass
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * This class provides AWS credential mapping to System properties during
 * test setup.  During test teardown, changed System properties are restored.
 *
 */
abstract class TestsWithAWSCredentials extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TestsWithAWSCredentials.class)

    protected static final Map<String, String> credentialsBeforeTest = new HashMap<>()
    protected static final Map<String, String> credentialsDuringTest = new HashMap<>()
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties"

    /**
     * Before the tests are run, this method reads the aws credentials file, and when successful, sets those values as
     * system properties.
     */
    @BeforeClass
    static void setUpCredentialsOnce() throws Exception {
        final FileInputStream fis
        try {
            fis = new FileInputStream(CREDENTIALS_FILE)
        } catch (FileNotFoundException e1) {
            fail("Could not open credentials file " + CREDENTIALS_FILE + ": " + e1.getLocalizedMessage())
            return
        }
        final PropertiesCredentials credentials = new PropertiesCredentials(fis)

        credentialsDuringTest.put("aws.accessKeyId", credentials.AWSAccessKeyId)
        credentialsDuringTest.put("aws.secretKey", credentials.AWSSecretKey)
        credentialsDuringTest.put("aws.region", "us-east-2")

        credentialsDuringTest.keySet().forEach({ name ->
            def value = System.getProperty(name)
            credentialsBeforeTest.put(name, value)
            if (value != null && StringUtils.isNotBlank(value)) {
                logger.info("Overwriting credential system property: " + name)
            }
            // We're copying the properties directly so the standard builder works.
            System.setProperty(name, credentialsDuringTest.get(name))
        })

    }

    /**
     * After the tests have run, this method restores the system properties that were set during test class setup.
     */
    @AfterClass
    static void tearDownCredentialsOnce() throws Exception {
        credentialsBeforeTest.keySet().forEach { name ->
            def value = credentialsBeforeTest.get(name)
            if (StringUtils.isNotBlank(value)) {
                logger.info("Restoring credential system property: " + name)
            }
            System.setProperty(name, value == null ? "" : value)
        }
    }
}
