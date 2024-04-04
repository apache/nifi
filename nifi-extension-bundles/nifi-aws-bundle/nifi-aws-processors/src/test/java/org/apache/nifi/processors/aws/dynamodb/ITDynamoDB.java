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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITDynamoDB extends AbstractDynamoDBIT {
    private static final String JSON_TEMPLATE = "{ \"key\": \"%d\", \"value\": \"val\" }";
    private static final String HASH_KEY_ATTRIBUTE = "dynamodb.item.hash.key.value";
    private static final String RANGE_KEY_ATTRIBUTE = "dynamodb.item.range.key.value";
    private static final String JSON_DOCUMENT_FIELD = "jsonDocument";

    @Test
    public void partitionKeyOnlySuccess() {
        runDynamoDBTest(10, PARTITION_KEY_ONLY_TABLE, false, runner -> {});
    }

    @Test
    public void partitionKeySortKeySuccess() {
        runDynamoDBTest(10, PARTITION_AND_SORT_KEY_TABLE, true, runner -> {
            runner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, SORT_KEY);
            runner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE_TYPE, "number");
        });
    }

    private void runDynamoDBTest(final int count, final String table, final boolean includeSortKey, final Consumer<TestRunner> runnerConfigurer) {

        // Put the documents in DynamoDB
        final TestRunner putRunner = initRunner(PutDynamoDB.class);
        putRunner.setProperty(PutDynamoDB.BATCH_SIZE, String.valueOf(count));
        putRunner.setProperty(PutDynamoDB.TABLE, table);
        putRunner.setProperty(PutDynamoDB.HASH_KEY_NAME, PARTITION_KEY);
        putRunner.setProperty(PutDynamoDB.JSON_DOCUMENT, JSON_DOCUMENT_FIELD);
        runnerConfigurer.accept(putRunner);

        enqueue(putRunner, count, includeSortKey);
        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(PutDynamoDB.REL_SUCCESS, count);
        assertItemsExist(count, table, includeSortKey);

        // Get the documents from DynamoDB
        final TestRunner getRunner = initRunner(GetDynamoDB.class);
        getRunner.setProperty(GetDynamoDB.BATCH_SIZE, String.valueOf(count));
        getRunner.setProperty(GetDynamoDB.TABLE, table);
        getRunner.setProperty(GetDynamoDB.HASH_KEY_NAME, PARTITION_KEY);
        getRunner.setProperty(GetDynamoDB.JSON_DOCUMENT, JSON_DOCUMENT_FIELD);
        runnerConfigurer.accept(getRunner);

        putRunner.getFlowFilesForRelationship(PutDynamoDB.REL_SUCCESS).forEach(getRunner::enqueue);
        getRunner.run(1);
        getRunner.assertAllFlowFilesTransferred(GetDynamoDB.REL_SUCCESS, count);
        getRunner.getFlowFilesForRelationship(GetDynamoDB.REL_SUCCESS).forEach(ff -> {
            final String data = new String(ff.getData());
            final String hashKey = ff.getAttribute(HASH_KEY_ATTRIBUTE);
            assertNotNull(hashKey);
            assertTrue(data.contains(StringUtils.substringAfter(hashKey, PARTITION_KEY_VALUE_PREFIX)));
        });

        // Delete the documents from DynamoDB
        final TestRunner deleteRunner = initRunner(DeleteDynamoDB.class);
        deleteRunner.setProperty(DeleteDynamoDB.BATCH_SIZE, String.valueOf(count));
        deleteRunner.setProperty(DeleteDynamoDB.TABLE, table);
        deleteRunner.setProperty(DeleteDynamoDB.HASH_KEY_NAME, PARTITION_KEY);
        runnerConfigurer.accept(deleteRunner);

        getRunner.getFlowFilesForRelationship(GetDynamoDB.REL_SUCCESS).forEach(deleteRunner::enqueue);
        deleteRunner.run(1);
        deleteRunner.assertAllFlowFilesTransferred(DeleteDynamoDB.REL_SUCCESS, count);

        assertItemsDoNotExist(count, table, includeSortKey);
    }

    private void assertItemsExist(final int count, final String table, final boolean includeSortKey) {
        final BatchGetItemResponse response = getBatchGetItems(count, table, includeSortKey);
        final List<Map<String, AttributeValue>> items = response.responses().get(table);
        assertNotNull(items);
        assertEquals(count, items.size());
        items.forEach(item -> {
            assertNotNull(item.get(PARTITION_KEY));
            assertNotNull(item.get(PARTITION_KEY).s());

            assertNotNull(item.get(JSON_DOCUMENT_FIELD));
            assertNotNull(item.get(JSON_DOCUMENT_FIELD).s());
            if (includeSortKey) {
                assertNotNull(item.get(SORT_KEY));
                assertNotNull(item.get(SORT_KEY).n());
            }
        });
    }

    private void assertItemsDoNotExist(final int count, final String table, final boolean includeSortKey) {
        final BatchGetItemResponse response = getBatchGetItems(count, table, includeSortKey);
        final List<Map<String, AttributeValue>> items = response.responses().get(table);
        assertNotNull(items);
        assertEquals(0, items.size());
    }

    private static void enqueue(final TestRunner runner, final int count, final boolean includeSortKey) {
        for (int i = 0; i < count; i++) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(HASH_KEY_ATTRIBUTE, PARTITION_KEY_VALUE_PREFIX + i);
            if (includeSortKey) {
                attributes.put(RANGE_KEY_ATTRIBUTE, String.valueOf(i));
            }
            runner.enqueue(String.format(JSON_TEMPLATE, i), attributes);
        }
    }
}
