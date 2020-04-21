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
package org.apache.nifi.stateless.core.security

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import groovy.json.JsonSlurper
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

@RunWith(JUnit4.class)
class StatelessSecurityUtilityTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(StatelessSecurityUtilityTest.class)

    private static final String JSON_ARGS = """{
  "registryUrl": "http://nifi-registry-service:18080",
  "bucketId": "50ca47f9-b07a-4199-97cd-e2b519d397d1",
  "flowId": "9fbe1d70-82ec-44de-b815-c7f838af181a",
  "parameters": {
    "DB_IP": "127.0.0.1",
    "DB_NAME": "database",
    "DB_PASS": {
      "sensitive": "true",
      "value": "password"
    },
    "DB_USER": "username"
  }
}"""
    private final String MASKED_REGEX = /\[MASKED\] \([\w\/\+=]+\)/

    @BeforeClass
    static void setUpOnce() {
        Security.addProvider(new BouncyCastleProvider())

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
    void testShouldMaskSensitiveParameterInJsonObject() {
        // Arrange
        JsonObject json = new JsonParser().parse(JSON_ARGS).getAsJsonObject()
        def dbPass = json.getAsJsonObject("parameters").getAsJsonObject("DB_PASS")
        logger.info("DB password: ${dbPass.toString()}")
        def dbPassSensitive = dbPass.getAsJsonPrimitive("sensitive").getAsBoolean()
        def dbPassword = dbPass.getAsJsonPrimitive("value").getAsString()

        // Act
        String output = StatelessSecurityUtility.getLoggableRepresentationOfJsonObject(json)
        logger.info("Masked output: ${output}")

        // Assert
        assert !(output =~ dbPassword)
        def masked = output =~ MASKED_REGEX
        assert masked
    }

    @Test
    void testShouldMaskMultipleSensitiveParametersInJsonObject() {
        // Arrange
        final String MULTIPLE_SENSITIVE_JSON_ARGS = """{
  "registryUrl": "http://nifi-registry-service:18080",
  "bucketId": "50ca47f9-b07a-4199-97cd-e2b519d397d1",
  "flowId": "9fbe1d70-82ec-44de-b815-c7f838af181a",
  "parameters": {
    "DB_IP": "127.0.0.1",
    "DB_NAME": "database",
    "DB_PASS": {
      "sensitive": "true",
      "value": "password"
    },
    "DB_OTHER_PASS": {
      "sensitive": "true",
      "value": "otherPassword"
    },
    "DB_USER": "username"
  }
}"""

        JsonObject json = new JsonParser().parse(MULTIPLE_SENSITIVE_JSON_ARGS).getAsJsonObject()
        def dbPass = json.getAsJsonObject("parameters").getAsJsonObject("DB_PASS")
        logger.info("DB password: ${dbPass.toString()}")
        def dbPassword = dbPass.getAsJsonPrimitive("value").getAsString()
        def dbOtherPassword = json.getAsJsonObject("parameters").getAsJsonObject("DB_OTHER_PASS").getAsJsonPrimitive("value").getAsString()

        // Act
        String output = StatelessSecurityUtility.getLoggableRepresentationOfJsonObject(json)
        logger.info("Masked output: ${output}")

        // Assert
        assert !(output =~ dbPassword)
        assert !(output =~ dbOtherPassword)

        // Use Groovy JSON assertions
        def groovyJson = new JsonSlurper().parseText(output)
        assert groovyJson.parameters.DB_PASS.value =~ MASKED_REGEX
        assert groovyJson.parameters.DB_OTHER_PASS.value =~ MASKED_REGEX
    }

    @Test
    void testShouldNotMaskSensitiveFalseParameterInJsonObject() {
        // Arrange
        final String JSON_SENSITIVE_FALSE_ARGS = JSON_ARGS.replaceAll('"sensitive": "true"', '"sensitive": "false"')

        JsonObject json = new JsonParser().parse(JSON_SENSITIVE_FALSE_ARGS).getAsJsonObject()
        def dbPass = json.getAsJsonObject("parameters").getAsJsonObject("DB_PASS")
        logger.info("DB password: ${dbPass.toString()}")
        def dbPassSensitive = dbPass.getAsJsonPrimitive("sensitive").getAsBoolean()
        assert !dbPassSensitive
        def dbPassword = dbPass.getAsJsonPrimitive("value").getAsString()

        // Act
        String output = StatelessSecurityUtility.getLoggableRepresentationOfJsonObject(json)
        logger.info("Masked output: ${output}")

        // Assert
        assert output =~ dbPassword
        assert output == json.toString()
    }

    @Test
    void testMaskSensitiveParameterInJsonObjectShouldNotHaveSideEffects() {
        // Arrange
        JsonObject json = new JsonParser().parse(JSON_ARGS).getAsJsonObject()
        def dbPass = json.getAsJsonObject("parameters").getAsJsonObject("DB_PASS")
        logger.info("DB password: ${dbPass.toString()}")
        def dbPassSensitive = dbPass.getAsJsonPrimitive("sensitive").getAsBoolean()
        def dbPassword = dbPass.getAsJsonPrimitive("value").getAsString()

        // Act
        String output = StatelessSecurityUtility.getLoggableRepresentationOfJsonObject(json)
        logger.info("Masked output: ${output}")
        logger.info("Original JSON object after masking: ${json.toString()}")

        // Assert
        assert json.getAsJsonObject("parameters").getAsJsonObject("DB_PASS").getAsJsonPrimitive("value").getAsString() == "password"
    }

    @Test
    void testShouldDetectSensitiveStrings() {
        // Arrange
        def sensitiveStrings = [
                '"sensitive": "true"',
                '"sensitive":"true"'.toUpperCase(),
                '"sensitive"\t:\t"true"',
                '"sensitive"\n:\n\n"true"',
                '{"parameter_name": {"sensitive": "true", "value": "password"} }',
                '"password": ',
                "token",
                '"access": "some_key_value"',
                '"secret": "my_secret"'.toUpperCase()
        ]
        def safeStrings = [
                "regular_json",
                '"sensitive": "false"'
        ]

        // Act
        def sensitiveResults = sensitiveStrings.collectEntries {
            [it, StatelessSecurityUtility.isSensitive(it)]
        }
        logger.info("Sensitive results: ${sensitiveResults}")

        def safeResults = safeStrings.collectEntries {
            [it, StatelessSecurityUtility.isSensitive(it)]
        }
        logger.info("Safe results: ${safeResults}")

        // Assert
        assert sensitiveResults.every { it.value }
        assert safeResults.every { !it.value }
    }


    @Test
    void testShouldFormatJson() {
        // Arrange
        final JsonObject JSON = new JsonParser().parse(JSON_ARGS.replaceAll("\n", "")).getAsJsonObject()

        // Act
        String output = StatelessSecurityUtility.formatJson(JSON)
        logger.info("Masked output: ${output}")

        // Assert
        assert output =~ MASKED_REGEX
        assert !(output =~ "password")
    }
}
