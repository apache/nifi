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
package org.apache.nifi.processors.aws.dynamodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ITPutDynamoDBRecord extends AbstractDynamoDBIT {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void partitionKeyOnlySuccess() throws JsonProcessingException {
        runDynamoDBTest(10, PARTITION_KEY_ONLY_TABLE, false, runner -> {});
    }

    @Test
    public void partitionKeySortKeySuccess() throws JsonProcessingException {
        runDynamoDBTest(10, PARTITION_AND_SORT_KEY_TABLE, true, runner -> {
            runner.setProperty(PutDynamoDBRecord.SORT_KEY_STRATEGY, PutDynamoDBRecord.SORT_BY_FIELD);
            runner.setProperty(PutDynamoDBRecord.SORT_KEY_FIELD, "sortKey");
        });
    }

    private void runDynamoDBTest(final int count, final String table, final boolean includeSortKey, final Consumer<TestRunner> runnerConfigurer) throws JsonProcessingException {
        final List<TestRecord> testRecords = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final Long sortKey = includeSortKey ? (long) i : null;
            testRecords.add(new TestRecord(PARTITION_KEY_VALUE_PREFIX + i, sortKey, UUID.randomUUID().toString(), i, Arrays.asList("1", "2", "3")));
        }

        // Put the documents in DynamoDB
        final TestRunner runner = initRunner(PutDynamoDBRecord.class);
        runner.setProperty(PutDynamoDBRecord.PARTITION_KEY_FIELD, PARTITION_KEY);
        runner.setProperty(PutDynamoDBRecord.TABLE, table);
        try {
            final JsonTreeReader jsonTreeReader = new JsonTreeReader();
            runner.addControllerService("jsonTreeReader", jsonTreeReader);
            runner.enableControllerService(jsonTreeReader);

            runner.setProperty(PutDynamoDBRecord.RECORD_READER, "jsonTreeReader");
        } catch (final InitializationException e) {
            Assertions.fail("Could not set properties");
        }
        runnerConfigurer.accept(runner);

        final String data = OBJECT_MAPPER.writeValueAsString(testRecords);
        runner.enqueue(data);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutDynamoDB.REL_SUCCESS, 1);
        assertItemsExist(count, table, includeSortKey);
    }

    private void assertItemsExist(final int count, final String table, final boolean includeSortKey) {
        final BatchGetItemResponse response = getBatchGetItems(count, table, includeSortKey);
        final List<Map<String, AttributeValue>> items = response.responses().get(table);
        assertNotNull(items);
        assertEquals(count, items.size());
        items.forEach(item -> {
            assertNotNull(item.get(PARTITION_KEY));
            assertNotNull(item.get(PARTITION_KEY).s());

            assertNotNull(item.get("testString"));
            assertNotNull(item.get("testString").s());

            assertNotNull(item.get("testNumber"));
            assertNotNull(item.get("testNumber").n());

            assertNotNull(item.get("testList"));
            assertNotNull(item.get("testList").l());
            assertEquals(3, item.get("testList").l().size());

            if (includeSortKey) {
                assertNotNull(item.get(SORT_KEY));
                assertNotNull(item.get(SORT_KEY).n());
            }
        });
    }

    private static class TestRecord {
        private String partitionKey;
        private Long sortKey;
        private String testString;
        private Integer testNumber;
        private List<String> testList;

        public TestRecord() {

        }

        public TestRecord(final String partitionKey, final Long sortKey, final String testString, final Integer testNumber, final List<String> testList) {
            this.partitionKey = partitionKey;
            this.sortKey = sortKey;
            this.testString = testString;
            this.testNumber = testNumber;
            this.testList = testList;
        }

        public String getPartitionKey() {
            return partitionKey;
        }

        public void setPartitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
        }

        public Long getSortKey() {
            return sortKey;
        }

        public void setSortKey(Long sortKey) {
            this.sortKey = sortKey;
        }

        public String getTestString() {
            return testString;
        }

        public void setTestString(String testString) {
            this.testString = testString;
        }

        public Integer getTestNumber() {
            return testNumber;
        }

        public void setTestNumber(Integer testNumber) {
            this.testNumber = testNumber;
        }

        public List<String> getTestList() {
            return testList;
        }

        public void setTestList(List<String> testList) {
            this.testList = testList;
        }
    }
}
