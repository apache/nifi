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
package org.apache.nifi.processor

import org.apache.nifi.util.FormatUtils
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

@RunWith(JUnit4.class)
class TestFormatUtilsGroovy extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TestFormatUtilsGroovy.class)

    @BeforeClass
    public static void setUpOnce() throws Exception {
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

    /**
     * New feature test
     */
    @Test
    void testShouldConvertWeeks() {
        // Arrange
        final List WEEKS = ["1 week", "1 wk", "1 w", "1 wks", "1 weeks"]
        final long EXPECTED_DAYS = 7L

        // Act
        List days = WEEKS.collect { String week ->
            FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
        }
        logger.converted(days)

        // Assert
        assert days.every { it == EXPECTED_DAYS }
    }



    @Test
    void testShouldHandleNegativeWeeks() {
        // Arrange
        final List WEEKS = ["-1 week", "-1 wk", "-1 w", "-1 weeks", "- 1 week"]

        // Act
        List msgs = WEEKS.collect { String week ->
            shouldFail(IllegalArgumentException) {
                FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
            }
        }

        // Assert
        assert msgs.every { it =~ /Value '.*' is not a valid Time Duration/ }
    }



    /**
     * Regression test
     */
    @Test
    void testShouldHandleInvalidAbbreviations() {
        // Arrange
        final List WEEKS = ["1 work", "1 wek", "1 k"]

        // Act
        List msgs = WEEKS.collect { String week ->
            shouldFail(IllegalArgumentException) {
                FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
            }
        }

        // Assert
        assert msgs.every { it =~ /Value '.*' is not a valid Time Duration/ }

    }


    /**
     * New feature test
     */
    @Test
    void testShouldHandleNoSpaceInInput() {
        // Arrange
        final List WEEKS = ["1week", "1wk", "1w", "1wks", "1weeks"]
        final long EXPECTED_DAYS = 7L

        // Act
        List days = WEEKS.collect { String week ->
            FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
        }
        logger.converted(days)

        // Assert
        assert days.every { it == EXPECTED_DAYS }
    }
}