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
package org.apache.nifi.processors.standard;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TestLookupRecordAttribute {

    private TestRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(LookupRecordAttribute.class);
        final TestRecordLookupService service = new TestRecordLookupService(Set.of("key"));
        runner.addControllerService("service", service);
        runner.enableControllerService(service);
        runner.setProperty(LookupRecordAttribute.LOOKUP_SERVICE, "service");
        runner.setProperty(LookupRecordAttribute.LOOKUP_KEY, "${id}");
    }

    @Test
    void testMultipleAttributesAdded() {
        runner.setProperty("name.attr", "name");
        runner.setProperty("city.attr", "city");
        runner.assertValid();

        runner.enqueue("content", Map.of("id", "1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecordAttribute.REL_MATCHED, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupRecordAttribute.REL_MATCHED).getFirst();
        flowFile.assertAttributeEquals("name.attr", "Alice");
        flowFile.assertAttributeEquals("city.attr", "Paris");
    }

    @Test
    void testUnmatchedKey() {
        runner.setProperty("name.attr", "name");
        runner.enqueue("content", Map.of("id", "unknown"));
        runner.run();
        runner.assertAllFlowFilesTransferred(LookupRecordAttribute.REL_UNMATCHED, 1);
    }

    @Test
    void testEmptyValuesExcluded() {
        runner.setProperty(LookupRecordAttribute.INCLUDE_EMPTY_VALUES, "false");
        runner.setProperty("name.attr", "name");
        runner.setProperty("blank.attr", "blank");
        runner.enqueue("content", Map.of("id", "1"));
        runner.run();
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupRecordAttribute.REL_MATCHED).getFirst();
        flowFile.assertAttributeEquals("name.attr", "Alice");
        flowFile.assertAttributeNotExists("blank.attr");
    }

    @Test
    void testEmptyValuesIncluded() {
        runner.setProperty("blank.attr", "blank");
        runner.enqueue("content", Map.of("id", "1"));
        runner.run();
        runner.getFlowFilesForRelationship(LookupRecordAttribute.REL_MATCHED).getFirst().assertAttributeEquals("blank.attr", "");
    }

    @Test
    void testLookupFailure() {
        runner.setProperty("name.attr", "name");
        runner.enqueue("content", Map.of("id", "fail"));
        runner.run();
        runner.assertAllFlowFilesTransferred(LookupRecordAttribute.REL_FAILURE, 1);
    }

    @Test
    void testInvalidWithoutDynamicProperties() {
        runner.assertNotValid();
    }

    @Test
    void testInvalidServiceWithMultipleKeys() throws Exception {
        final TestRecordLookupService multiService = new TestRecordLookupService(Set.of("a", "b"));
        runner.addControllerService("multi", multiService);
        runner.enableControllerService(multiService);
        runner.setProperty(LookupRecordAttribute.LOOKUP_SERVICE, "multi");
        runner.setProperty("name.attr", "name");
        runner.assertNotValid();
    }

    private static class TestRecordLookupService extends AbstractControllerService implements RecordLookupService {
        private final Set<String> requiredKeys;

        TestRecordLookupService(final Set<String> requiredKeys) {
            this.requiredKeys = requiredKeys;
        }

        @Override
        public Optional<Record> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
            final Object key = coordinates.values().iterator().next();
            if ("fail".equals(key)) {
                throw new LookupFailureException("forced failure");
            }
            if (!"1".equals(key)) {
                return Optional.empty();
            }
            final RecordSchema schema = new SimpleRecordSchema(List.of(
                    new RecordField("name", RecordFieldType.STRING.getDataType()),
                    new RecordField("city", RecordFieldType.STRING.getDataType()),
                    new RecordField("blank", RecordFieldType.STRING.getDataType())));
            final Map<String, Object> values = new HashMap<>();
            values.put("name", "Alice");
            values.put("city", "Paris");
            values.put("blank", " ");
            return Optional.of(new MapRecord(schema, values));
        }

        @Override
        public Set<String> getRequiredKeys() {
            return requiredKeys;
        }
    }
}
