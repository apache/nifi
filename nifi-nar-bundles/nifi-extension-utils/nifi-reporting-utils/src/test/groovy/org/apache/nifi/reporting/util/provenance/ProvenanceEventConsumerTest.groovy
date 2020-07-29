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
package org.apache.nifi.reporting.util.provenance


import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class ProvenanceEventConsumerTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ProvenanceEventConsumerTest.class)

    @BeforeClass
    static void setUpOnce() {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {
        super.setUp()

    }

    @After
    void tearDown() {

    }

    @Test
    void testIsFilteringEnabled() {
        // Arrange
        ProvenanceEventConsumer noFilterPopulated = new ProvenanceEventConsumer()

        Map<String, ProvenanceEventConsumer> patternFilteredConsumers = [:]
        def regexProperties = ["componentTypeRegex", "componentTypeRegexExclude", "componentNameRegex", "componentNameRegexExclude"]
        regexProperties.each { String prop ->
            ProvenanceEventConsumer consumer = new ProvenanceEventConsumer()
            consumer."${prop}" = prop
            logger.info("Created regex-filtered PEC with ${prop} set")
            patternFilteredConsumers[prop] = consumer
        }

        Map<String, ProvenanceEventConsumer> listFilteredConsumers = [:]
        def listProperties = ["eventTypes", "eventTypesExclude", "componentIds", "componentIdsExclude"]
        listProperties.each { String prop ->
            ProvenanceEventConsumer consumer = new ProvenanceEventConsumer()
            consumer."${prop}" = [prop]
            logger.info("Created list-filtered PEC with ${prop} set")
            listFilteredConsumers[prop] = consumer
        }

        def allFilteredConsumers = patternFilteredConsumers + listFilteredConsumers
        logger.info("Created ${allFilteredConsumers.size()} filtered consumers")

        // Act
        boolean unfilteredConsumerHasFilteringEnabled = noFilterPopulated.isFilteringEnabled()
        logger.info("Unfiltered PEC has filtering enabled: ${unfilteredConsumerHasFilteringEnabled}")

        def filteredResults = allFilteredConsumers.collectEntries { prop, consumer ->
            [prop, consumer.isFilteringEnabled()]
        }
        logger.info("Filtered PEC results: ${filteredResults}")

        // Assert
        assert !unfilteredConsumerHasFilteringEnabled
        assert filteredResults.every()
    }
}
