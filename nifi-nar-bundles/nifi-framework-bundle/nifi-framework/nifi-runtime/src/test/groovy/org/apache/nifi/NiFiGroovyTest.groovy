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
package org.apache.nifi

import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.AppenderBase
import org.apache.nifi.properties.AESSensitivePropertyProvider
import org.apache.nifi.properties.NiFiPropertiesLoader
import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.bridge.SLF4JBridgeHandler

import java.security.Security

@RunWith(JUnit4.class)
class NiFiGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(NiFiGroovyTest.class)

    private static String originalPropertiesPath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)

    private static final String TEST_RES_PATH = NiFiGroovyTest.getClassLoader().getResource(".").toURI().getPath()
    private static final File workDir = new File("./target/work/jetty/")

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        SLF4JBridgeHandler.install()

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        logger.info("Identified test resources path as ${TEST_RES_PATH}")
    }

    @Before
    public void setUp() throws Exception {
        if (!workDir.exists()) {
            workDir.mkdirs()
        }
    }

    @After
    public void tearDown() throws Exception {
        NiFiPropertiesLoader.@sensitivePropertyProviderFactory = null
        TestAppender.reset()
        System.setIn(System.in)
    }

    @AfterClass
    public static void tearDownOnce() {
        if (originalPropertiesPath) {
            System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, originalPropertiesPath)
        }
    }

    @Test
    public void testInitializePropertiesShouldHandleNoBootstrapKey() throws Exception {
        // Arrange
        def args = [] as String[]

        String plainPropertiesPath = "${TEST_RES_PATH}/NiFiProperties/conf/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, plainPropertiesPath)

        // Act
        NiFiProperties loadedProperties = NiFi.initializeProperties(args)

        // Assert
        assert loadedProperties.size() > 0
    }

    @Test
    public void testMainShouldHandleNoBootstrapKeyWithProtectedProperties() throws Exception {
        // Arrange
        def args = [] as String[]

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "${TEST_RES_PATH}/NiFiProperties/conf/nifi_with_sensitive_properties_protected_aes_different_key.properties")

        // Act
        NiFi.main(args)

        // Assert
        assert TestAppender.events.last().getMessage() == "Failure to launch NiFi due to java.lang.IllegalArgumentException: The bootstrap process did not provide a valid key and there are protected properties present in the properties file"
    }

    @Test
    public void testParseArgsShouldSplitCombinedArgs() throws Exception {
        // Arrange
        final String DIFFERENT_KEY = "0" * 64
        def args = ["-k ${DIFFERENT_KEY}"] as String[]

        // Act
        def parsedArgs = NiFi.parseArgs(args)

        // Assert
        assert parsedArgs.size() == 2
        assert parsedArgs == args.join(" ").split(" ") as List
    }

    @Test
    public void testMainShouldHandleBadArgs() throws Exception {
        // Arrange
        def args = ["-k"] as String[]

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "${TEST_RES_PATH}/NiFiProperties/conf/nifi_with_sensitive_properties_protected_aes.properties")

        // Act
        NiFi.main(args)

        // Assert
        assert TestAppender.events.collect {
            it.getFormattedMessage()
        }.contains("The bootstrap process passed the -k flag without a key")
        assert TestAppender.events.last().getMessage() == "Failure to launch NiFi due to java.lang.IllegalArgumentException: The bootstrap process did not provide a valid key and there are protected properties present in the properties file"
    }

    @Test
    public void testMainShouldHandleMalformedBootstrapKey() throws Exception {
        // Arrange
        def args = ["-k", "BAD KEY"] as String[]

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "${TEST_RES_PATH}/NiFiProperties/conf/nifi_with_sensitive_properties_protected_aes.properties")

        // Act
        NiFi.main(args)

        // Assert
        assert TestAppender.events.last().getMessage() == "Failure to launch NiFi due to java.lang.IllegalArgumentException: The bootstrap process did not provide a valid key and there are protected properties present in the properties file"
    }

    @Test
    public void testInitializePropertiesShouldSetBootstrapKeyFromArgs() throws Exception {
        // Arrange
        final String DIFFERENT_KEY = "0" * 64
        def args = ["-k", DIFFERENT_KEY] as String[]

        String testPropertiesPath = "${TEST_RES_PATH}/NiFiProperties/conf/nifi_with_sensitive_properties_protected_aes_different_key.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, testPropertiesPath)

        NiFiProperties unprocessedProperties = new NiFiPropertiesLoader().loadRaw(new File(testPropertiesPath))
        def protectedKeys = getProtectedKeys(unprocessedProperties)
        logger.info("Reading from raw properties file gives protected properties: ${protectedKeys}")

        // Act
        NiFiProperties properties = NiFi.initializeProperties(args)

        // Assert

        // Ensure that there were protected properties, they were encrypted using AES/GCM (128/256 bit key), and they were decrypted (raw value != retrieved value)
        assert !hasProtectedKeys(properties)
        def unprotectedProperties = decrypt(unprocessedProperties, DIFFERENT_KEY)
        getProtectedPropertyKeys(unprocessedProperties).every { k, v ->
            String rawValue = unprocessedProperties.getProperty(k)
            logger.raw("${k} -> ${rawValue}")
            String retrievedValue = properties.getProperty(k)
            logger.decrypted("${k} -> ${retrievedValue}")

            assert v =~ "aes/gcm"

            logger.assert("${retrievedValue} != ${rawValue}")
            assert retrievedValue != rawValue

            String decryptedProperty = unprotectedProperties.getProperty(k)
            logger.assert("${retrievedValue} == ${decryptedProperty}")
            assert retrievedValue == decryptedProperty
        }
    }

    private static boolean hasProtectedKeys(NiFiProperties properties) {
        properties.getPropertyKeys().any { it.endsWith(".protected") }
    }

    private static Map<String, String> getProtectedPropertyKeys(NiFiProperties properties) {
        getProtectedKeys(properties).collectEntries { String key ->
            [(key): properties.getProperty(key + ".protected")]
        }
    }

    private static Set<String> getProtectedKeys(NiFiProperties properties) {
        properties.getPropertyKeys().findAll { it.endsWith(".protected") }
    }

    private static NiFiProperties decrypt(NiFiProperties encryptedProperties, String keyHex) {
        AESSensitivePropertyProvider spp = new AESSensitivePropertyProvider(keyHex)
        def map = encryptedProperties.getPropertyKeys().collectEntries { String key ->
            if (encryptedProperties.getProperty(key + ".protected") == spp.getIdentifierKey()) {
                [(key): spp.unprotect(encryptedProperties.getProperty(key))]
            } else if (!key.endsWith(".protected")) {
                [(key): encryptedProperties.getProperty(key)]
            }
        }
        new StandardNiFiProperties(map as Properties)
    }

    @Test
    public void testShouldValidateKeys() {
        // Arrange
        final List<String> VALID_KEYS = [
                "0" * 64, // 256 bit keys
                "ABCDEF01" * 8,
                "0123" * 8, // 128 bit keys
                "0123456789ABCDEFFEDCBA9876543210",
                "0123456789ABCDEFFEDCBA9876543210".toLowerCase(),
        ]

        // Act
        def isValid = VALID_KEYS.collectEntries { String key -> [(key): NiFi.isHexKeyValid(key)] }
        logger.info("Key validity: ${isValid}")

        // Assert
        assert isValid.every { k, v -> v }
    }

    @Test
    public void testShouldNotValidateInvalidKeys() {
        // Arrange
        final List<String> VALID_KEYS = [
                "0" * 63,
                "ABCDEFG1" * 8,
                "0123" * 9,
                "0123456789ABCDEFFEDCBA987654321",
                "0123456789ABCDEF FEDCBA9876543210".toLowerCase(),
                null,
                "",
                "        "
        ]

        // Act
        def isValid = VALID_KEYS.collectEntries { String key -> [(key): NiFi.isHexKeyValid(key)] }
        logger.info("Key validity: ${isValid}")

        // Assert
        assert isValid.every { k, v -> !v }
    }
}

public class TestAppender extends AppenderBase<LoggingEvent> {
    static List<LoggingEvent> events = new ArrayList<>();

    @Override
    protected void append(LoggingEvent e) {
        synchronized (events) {
            events.add(e);
        }
    }

    public static void reset() {
        synchronized (events) {
            events.clear();
        }
    }
}