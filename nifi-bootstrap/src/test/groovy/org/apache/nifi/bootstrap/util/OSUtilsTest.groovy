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
package org.apache.nifi.bootstrap.util


import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class OSUtilsTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(OSUtilsTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @AfterClass
    static void tearDownOnce() throws Exception {

    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {

    }

    @Test
    void testShouldParseJavaMajorVersion8Below() {
        // Arrange
        def possibleVersionStrings = ["1.8", "1.8.0.0", "1.8.0_262"]

        // Act
        def results = possibleVersionStrings.collect {
            OSUtils.parseJavaVersion(it)
        }
        logger.info("Parsed Java versions: ${results}")

        // Assert
        assert results.every { it == 8 }
    }

    @Test
    void testShouldParseJavaMajorVersion9Plus() {
        // Arrange
        def possibleVersionStrings = [
                "11.0.6", "11.0.0", "11.12.13", "11"
        ]

        // Act
        def results = possibleVersionStrings.collect {
            OSUtils.parseJavaVersion(it)
        }
        logger.info("Parsed Java versions: ${results}")

        // Assert
        assert results.every { it == 11 }
    }
}
