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
package org.apache.nifi.security.util


import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class KeyStoreUtilsGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreUtilsGroovyTest.class)

    private static final File KEYSTORE_FILE = new File("src/test/resources/keystore.jks")
    private static final String KEYSTORE_PASSWORD = "passwordpassword"
    private static final String KEY_PASSWORD = "keypassword"
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS

    @BeforeClass
    static void setUpOnce() {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {

    }

    @After
    void tearDown() {

    }

    @Test
    void testShouldVerifyKeystoreIsValid() {
        // Arrange

        // Act
        boolean keystoreIsValid = KeyStoreUtils.isStoreValid(KEYSTORE_FILE.toURI().toURL(), KEYSTORE_TYPE, KEYSTORE_PASSWORD.toCharArray())

        // Assert
        assert keystoreIsValid
    }

    @Test
    void testShouldVerifyKeystoreIsNotValid() {
        // Arrange

        // Act
        boolean keystoreIsValid = KeyStoreUtils.isStoreValid(KEYSTORE_FILE.toURI().toURL(), KEYSTORE_TYPE, KEYSTORE_PASSWORD.reverse().toCharArray())

        // Assert
        assert !keystoreIsValid
    }

    @Test
    void testShouldVerifyKeyPasswordIsValid() {
        // Arrange

        // Act
        boolean keyPasswordIsValid = KeyStoreUtils.isKeyPasswordCorrect(KEYSTORE_FILE.toURI().toURL(), KEYSTORE_TYPE, KEYSTORE_PASSWORD.toCharArray(), KEYSTORE_PASSWORD.toCharArray())

        // Assert
        assert keyPasswordIsValid
    }

    @Test
    void testShouldVerifyKeyPasswordIsNotValid() {
        // Arrange

        // Act
        boolean keyPasswordIsValid = KeyStoreUtils.isKeyPasswordCorrect(KEYSTORE_FILE.toURI().toURL(), KEYSTORE_TYPE, KEYSTORE_PASSWORD.toCharArray(), KEYSTORE_PASSWORD.reverse().toCharArray())

        // Assert
        assert !keyPasswordIsValid
    }
}
