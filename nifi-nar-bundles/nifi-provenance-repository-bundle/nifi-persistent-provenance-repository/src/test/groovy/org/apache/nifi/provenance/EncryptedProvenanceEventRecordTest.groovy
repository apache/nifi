/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.provenance

import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class EncryptedProvenanceEventRecordTest {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedProvenanceEventRecordTest.class)

    private static final String DEFAULT_COMPONENT_ID = "component1234"
    private static final String DEFAULT_COMPONENT_TYPE = "GenerateFlowFile"
    private static final String DEFAULT_TRANSIT_URI = "nifi://localhost"
    private static final String DEFAULT_SSFI = "ssffi"
    private static final String DEFAULT_UUID = "ffABC"
    private static final String DEFAULT_AIU = "nifi://alternate"
    private static final String DEFAULT_DETAILS = "Some details"
    private static final String DEFAULT_RELATIONSHIP = "success"
    private static final String DEFAULT_SQI = "qXYZ"

    private static final long DEFAULT_EVENT_TIME = System.currentTimeMillis() - 1000
    private static final long DEFAULT_ENTRY_DATE = System.currentTimeMillis() - 1000*60
    private static final ProvenanceEventType DEFAULT_EVENT_TYPE = ProvenanceEventType.RECEIVE
    private static final long DEFAULT_LINEAGE_START_DATE = System.currentTimeMillis() - 1000*3600
    private static final List<String> DEFAULT_PARENT_UUIDS = ["ffDEF", "ffGHI"]
    private static final List<String> DEFAULT_CHILDREN_UUIDS = ["ffJKL", "ffMNO"]
    private static final long DEFAULT_EVENT_DURATION = 10
    private static final Map<String, String> DEFAULT_PREV_ATTRS = ["attr1": "Some value"]
    private static final Map<String, String> DEFAULT_UP_ATTRS = ["attr1": "Some new value"]

    private static final Map DEFAULT_PROPERTIES = [componentId:DEFAULT_COMPONENT_ID,
    componentType: DEFAULT_COMPONENT_TYPE,
            transitUri: DEFAULT_TRANSIT_URI,
            sourceSystemFlowFileIdentifier: DEFAULT_SSFI,
            uuid: DEFAULT_UUID,
            alternateIdentifierUri: DEFAULT_AIU,
            details: DEFAULT_DETAILS,
            relationship: DEFAULT_RELATIONSHIP,
            sourceQueueIdentifier: DEFAULT_SQI,
            eventTime: DEFAULT_EVENT_TIME,
            entryDate: DEFAULT_ENTRY_DATE,
            eventType: DEFAULT_EVENT_TYPE,
            lineageStartDate: DEFAULT_LINEAGE_START_DATE,
            parentUuids: DEFAULT_PARENT_UUIDS,
            childrenUuids: DEFAULT_CHILDREN_UUIDS,
            eventDuration: DEFAULT_EVENT_DURATION,
            previousAttributes: DEFAULT_PREV_ATTRS,
            updatedAttributes: DEFAULT_UP_ATTRS
    ]
    
    private static final StandardProvenanceEventRecord DEFAULT_RECORD //= generateStandardRecord()

    @Before
    void setUp() {

    }

    @After
    void tearDown() {

    }

    private static StandardProvenanceEventRecord generateStandardRecord(Map properties = DEFAULT_PROPERTIES) {
        StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder()
        properties.each { k, v -> 
            builder."$k" = v
        }
        new StandardProvenanceEventRecord(builder)
    }

    @Test
    void testShouldGetEncryptableStringProperties() {
        // Arrange
        final List<String> EXPECTED_ENCRYPTABLE_STRING_PROPERTIES = EncryptedProvenanceEventRecord.ENCRYPTABLE_STRING_PROPERTIES
        logger.info("Expected encryptable String properties (${EXPECTED_ENCRYPTABLE_STRING_PROPERTIES.size()}): ${EXPECTED_ENCRYPTABLE_STRING_PROPERTIES.join(", ")}")

        // Act
        List<String> props = EncryptedProvenanceEventRecord.getEncryptableStringProperties()
        logger.info("Retrieved encryptable String properties (${props.size()}): ${props.join(", ")}")

        // Assert
        assert props == EXPECTED_ENCRYPTABLE_STRING_PROPERTIES
    }

    @Test
    void testShouldGetEncryptableNonStringProperties() {
        // Arrange
        final Map<String, Class> EXPECTED_ENCRYPTABLE_NON_STRING_PROPERTIES = EncryptedProvenanceEventRecord.ENCRYPTABLE_PROPERTIES
        logger.info("Expected encryptable non-String properties (${EXPECTED_ENCRYPTABLE_NON_STRING_PROPERTIES.size()}): ${EXPECTED_ENCRYPTABLE_NON_STRING_PROPERTIES.collect { k, v -> "${v} $k" }.join(", ")}")

        // Act
        Map<String, Class> props = EncryptedProvenanceEventRecord.getEncryptableNonStringProperties()
        logger.info("Expected encryptable non-String properties (${props.size()}): ${props.collect { k, v -> "${v} $k" }.join(", ")}")

        // Assert
        assert props == EXPECTED_ENCRYPTABLE_NON_STRING_PROPERTIES
    }

    @Test
    void testShouldGetAllEncryptablePropertyNames() {
        // Arrange
        final List<String> EXPECTED_ENCRYPTABLE_PROPERTIES = EncryptedProvenanceEventRecord.ENCRYPTABLE_STRING_PROPERTIES + EncryptedProvenanceEventRecord.ENCRYPTABLE_PROPERTIES.keySet().sort()
        logger.info("Expected encryptable String properties (${EXPECTED_ENCRYPTABLE_PROPERTIES.size()}): ${EXPECTED_ENCRYPTABLE_PROPERTIES.join(", ")}")

        // Act
        List<String> props = EncryptedProvenanceEventRecord.getAllEncryptablePropertyNames()
        logger.info("Retrieved encryptable properties (${props.size()}): ${props.join(", ")}")

        // Assert
        assert props == EXPECTED_ENCRYPTABLE_PROPERTIES
    }

    @Test
    @Ignore("Not yet implemented")
    void testShouldCopyStandardRecordToEncryptedRecord() {
        // Arrange
        final STANDARD_RECORD = DEFAULT_RECORD
        logger.info("Standard record: ${STANDARD_RECORD}")

        // Act
        EncryptedProvenanceEventRecord encryptableRecord = EncryptedProvenanceEventRecord.copy(STANDARD_RECORD)
        logger.info("Copied record: ${encryptableRecord}")

        // Assert
        assert encryptableRecord
    }
}
