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

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

@RunWith(JUnit4.class)
class AESSensitivePropertyProviderFactoryTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(AESSensitivePropertyProviderFactoryTest.class)

    private static final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210" * 2

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Ignore("This is resolved in PR 1216")
    @Test
    public void testShouldNotGetProviderWithoutKey() throws Exception {
        // Arrange
        SensitivePropertyProviderFactory factory = new AESSensitivePropertyProviderFactory()

        // Act
        def msg = shouldFail(SensitivePropertyProtectionException) {
            SensitivePropertyProvider provider = factory.getProvider()
        }
        logger.expected(msg)

        // Assert
        assert msg == "The provider factory cannot generate providers without a key"
    }

    @Test
    public void testShouldGetProviderWithKey() throws Exception {
        // Arrange
        SensitivePropertyProviderFactory factory = new AESSensitivePropertyProviderFactory(KEY_HEX)

        // Act
        SensitivePropertyProvider provider = factory.getProvider()

        // Assert
        assert provider instanceof AESSensitivePropertyProvider
        assert provider.@key
        assert provider.@cipher
    }

    @Ignore("This is resolved in PR 1216")
    @Test
    public void testGetProviderShouldHandleEmptyKey() throws Exception {
        // Arrange
        SensitivePropertyProviderFactory factory = new AESSensitivePropertyProviderFactory("")

        // Act
        def msg = shouldFail(SensitivePropertyProtectionException) {
            SensitivePropertyProvider provider = factory.getProvider()
        }
        logger.expected(msg)

        // Assert
        assert msg == "The provider factory cannot generate providers without a key"
    }
}