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

package org.apache.nifi.processors.standard

import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Before
import org.junit.Test

class TestCalculateRecordStats {
    TestRunner runner
    MockRecordParser recordParser
    RecordSchema personSchema

    @Before
    void setup() {
        runner = TestRunners.newTestRunner(CalculateRecordStats.class)
        recordParser = new MockRecordParser()
        runner.addControllerService("recordReader", recordParser)
        runner.setProperty(CalculateRecordStats.RECORD_READER, "recordReader")
        runner.enableControllerService(recordParser)
        runner.assertValid()

        recordParser.addSchemaField("id", RecordFieldType.INT)
        List<RecordField> personFields = new ArrayList<>()
        RecordField nameField = new RecordField("name", RecordFieldType.STRING.getDataType())
        RecordField ageField = new RecordField("age", RecordFieldType.INT.getDataType())
        RecordField sportField = new RecordField("sport", RecordFieldType.STRING.getDataType())
        personFields.add(nameField)
        personFields.add(ageField)
        personFields.add(sportField)
        personSchema = new SimpleRecordSchema(personFields)
        recordParser.addSchemaField("person", RecordFieldType.RECORD)
    }

    @Test
    void testNoNullOrEmptyRecordFields() {
        def sports = [ "Soccer", "Soccer", "Soccer", "Football", "Football", "Basketball" ]
        def expectedAttributes = [
            "recordStats.sport.Soccer": "3",
            "recordStats.sport.Football": "2",
            "recordStats.sport.Basketball": "1",
            "recordStats.sport": "6",
            "record.count": "6"
        ]

        commonTest([ "sport": "/person/sport"], sports, expectedAttributes)
    }

    @Test
    void testWithNullFields() {
        def sports = [ "Soccer", null, null, "Football", null, "Basketball" ]
        def expectedAttributes = [
            "recordStats.sport.Soccer": "1",
            "recordStats.sport.Football": "1",
            "recordStats.sport.Basketball": "1",
            "recordStats.sport": "3",
            "record.count": "6"
        ]

        commonTest([ "sport": "/person/sport"], sports, expectedAttributes)
    }

    @Test
    void testWithFilters() {
        def sports = [ "Soccer", "Soccer", "Soccer", "Football", "Football", "Basketball" ]
        def expectedAttributes = [
            "recordStats.sport.Soccer": "3",
            "recordStats.sport.Basketball": "1",
            "recordStats.sport": "4",
            "record.count": "6"
        ]

        def propz = [
            "sport": "/person/sport[. != 'Football']"
        ]

        commonTest(propz, sports, expectedAttributes)
    }

    @Test
    void testWithSizeLimit() {
        runner.setProperty(CalculateRecordStats.LIMIT, "3")
        def sports = [ "Soccer", "Soccer", "Soccer", "Football", "Football",
               "Basketball", "Baseball", "Baseball", "Baseball", "Baseball",
                "Skiing", "Skiing", "Skiing", "Snowboarding"
        ]
        def expectedAttributes = [
            "recordStats.sport.Skiing": "3",
            "recordStats.sport.Soccer": "3",
            "recordStats.sport.Baseball": "4",
            "recordStats.sport": String.valueOf(sports.size()),
            "record.count": String.valueOf(sports.size())
        ]

        def propz = [
            "sport": "/person/sport"
        ]

        commonTest(propz, sports, expectedAttributes)
    }

    private void commonTest(Map procProperties, List sports, Map expectedAttributes) {
        int index = 1
        sports.each { sport ->
            recordParser.addRecord(index++, new MapRecord(personSchema, [
                "name" : "John Doe",
                "age"  : 48,
                "sport": sport
            ]))
        }

        procProperties.each { kv ->
            runner.setProperty(kv.key, kv.value)
        }

        runner.enqueue("")
        runner.run()
        runner.assertTransferCount(CalculateRecordStats.REL_FAILURE, 0)
        runner.assertTransferCount(CalculateRecordStats.REL_SUCCESS, 1)

        def flowFiles = runner.getFlowFilesForRelationship(CalculateRecordStats.REL_SUCCESS)
        def ff = flowFiles[0]
        expectedAttributes.each { kv ->
            Assert.assertNotNull("Missing ${kv.key}", ff.getAttribute(kv.key))
            Assert.assertEquals(kv.value, ff.getAttribute(kv.key))
        }
    }
}
