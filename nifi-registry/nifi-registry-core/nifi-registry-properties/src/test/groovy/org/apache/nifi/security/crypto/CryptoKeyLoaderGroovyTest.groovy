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
package org.apache.nifi.security.crypto

import org.apache.nifi.registry.security.crypto.CryptoKeyLoader
import org.apache.nifi.registry.security.crypto.CryptoKeyProvider
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.security.Security

@RunWith(JUnit4.class)
class CryptoKeyLoaderGroovyTest extends GroovyTestCase {

    private static final Logger logger = LoggerFactory.getLogger(CryptoKeyLoaderGroovyTest.class)

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Test
    public void testShouldExtractKeyFromBootstrapFile() throws Exception {
        // Arrange
        final String expectedKey = KEY_HEX_256

        // Act
        String key = CryptoKeyLoader.extractKeyFromBootstrapFile("src/test/resources/conf/bootstrap.conf")

        // Assert
        assert key == expectedKey
    }

    @Test
    public void testShouldNotExtractKeyFromBootstrapFileWithoutKeyLine() throws Exception {
        // Arrange

        // Act
        String key = CryptoKeyLoader.extractKeyFromBootstrapFile("src/test/resources/conf/bootstrap.with_missing_key_line.conf")

        // Assert
        assert key == CryptoKeyProvider.EMPTY_KEY
    }

    @Test
    public void testShouldNotExtractKeyFromBootstrapFileWithoutKey() throws Exception {
        // Arrange

        // Act
        String key = CryptoKeyLoader.extractKeyFromBootstrapFile("src/test/resources/conf/bootstrap.with_missing_key.conf")

        // Assert
        assert key == CryptoKeyProvider.EMPTY_KEY
    }

    @Test
    public void testShouldNotExtractKeyFromMissingBootstrapFile() throws Exception {
        // Arrange

        // Act
        def msg = shouldFail(IOException) {
            CryptoKeyLoader.extractKeyFromBootstrapFile("src/test/resources/conf/bootstrap.missing.conf")
        }
        logger.info(msg)

        // Assert
        assert msg == "Cannot read from bootstrap.conf"
    }

    @Test
    public void testShouldNotExtractKeyFromUnreadableBootstrapFile() throws Exception {
        // Arrange
        File unreadableFile = new File("src/test/resources/conf/bootstrap.unreadable_file_permissions.conf")
        Set<PosixFilePermission> originalPermissions = Files.getPosixFilePermissions(unreadableFile.toPath())
        Files.setPosixFilePermissions(unreadableFile.toPath(), [] as Set)
        try {
            assert !unreadableFile.canRead()

            // Act
            def msg = shouldFail(IOException) {
                CryptoKeyLoader.extractKeyFromBootstrapFile("src/test/resources/conf/bootstrap.unreadable_file_permissions.conf")
            }
            logger.info(msg)

            // Assert
            assert msg == "Cannot read from bootstrap.conf"
        } finally {
            // Clean up to allow for indexing, etc.
            Files.setPosixFilePermissions(unreadableFile.toPath(), originalPermissions)
        }
    }

}
