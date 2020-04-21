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
package org.apache.nifi.stateless.runtimes

import com.google.gson.JsonObject
import com.google.gson.JsonParser
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
class ProgramTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ProgramTest.class)

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
            [it, Program.isSensitive(it)]
        }
        logger.info("Sensitive results: ${sensitiveResults}")

        def safeResults = safeStrings.collectEntries {
            [it, Program.isSensitive(it)]
        }
        logger.info("Safe results: ${safeResults}")

        // Assert
        assert sensitiveResults.every { it.value }
        assert safeResults.every { !it.value }
    }

    @Test
    void testShouldFormatArgs() {
        // Arrange
        final String[] ARGS = ["RunFromRegistry", "Once", "--json", JSON_ARGS] as String[]

        // Act
        String output = Program.formatArgs(ARGS)
        logger.info("Masked output: ${output}")

        // Assert
        assert output =~ MASKED_REGEX
        assert !(output =~ "password")
    }

    @Test
    void testShouldFormatArgsWhenVerbosityDisabled() {
        // Arrange
        final String[] ARGS = ["RunFromRegistry", "Once", "--json", JSON_ARGS] as String[]
        Program.isVerbose = false

        // Act
        String output = Program.formatArgs(ARGS)
        logger.info("Masked output: ${output}")

        // Assert
        assert output.contains("{...json...}")
        assert !(output =~ "password")

        Program.isVerbose = true
    }

    @Test
    void testShouldFormatJson() {
        // Arrange
        final JsonObject JSON = new JsonParser().parse(JSON_ARGS.replaceAll("\n", "")).getAsJsonObject()

        // Act
        String output = Program.formatJson(JSON)
        logger.info("Masked output: ${output}")

        // Assert
        assert output =~ MASKED_REGEX
        assert !(output =~ "password")
    }
}
