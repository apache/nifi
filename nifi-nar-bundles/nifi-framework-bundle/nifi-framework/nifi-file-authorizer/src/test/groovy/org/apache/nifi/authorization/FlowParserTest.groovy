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
package org.apache.nifi.authorization

import org.apache.nifi.util.FlowParser
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class FlowParserTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(FlowParserTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {

    }

    @Test
    void testShouldHandleXXEInDocumentBuilder() {
        // Arrange
        final String XXE_TEMPLATE_FILEPATH = "src/test/resources/flow-with-xxe.xml.gz"

        FlowParser fp = new FlowParser()

        // Act
        def parsedFlow = fp.parse(new File(XXE_TEMPLATE_FILEPATH))
        logger.info("Parsed ${parsedFlow.toString()}")

        // Assert

        // The existing logic logs & swallows any exceptions and returns null
        assert !parsedFlow
    }
}
