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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetDynamoDBTest extends AbstractDynamoDBTest {
    private static final String JSON_DOCUMENT_KEY = "j1";
    private static final String HASH_KEY = "hashS";
    private static final String RANGE_KEY = "rangeS";
    private static final String HASH_KEY_VALUE = "h1";
    private static final String RANGE_KEY_VALUE = "r1";
    private GetDynamoDB getDynamoDB;

    @BeforeEach
    public void setUp() {
        final Map<String, Collection<Map<String, AttributeValue>>> responses = new HashMap<>();
        responses.put(stringHashStringRangeTableName, Collections.emptyList());
        final BatchGetItemResponse response = BatchGetItemResponse.builder()
                .unprocessedKeys(getUnprocessedTableToKeysMap())
                .responses(responses)
                .build();

        client = mock(DynamoDbClient.class);

        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(response);

        getDynamoDB = new GetDynamoDB() {
            @Override
            protected DynamoDbClient getClient(final ProcessContext context) {
                return client;
            }
        };
    }

    private static Map<String, KeysAndAttributes> getUnprocessedTableToKeysMap() {
        return getUnprocessedTableToKeysMap(true);
    }

    private static Map<String, KeysAndAttributes> getUnprocessedTableToKeysMap(final boolean includeRangeKey) {
        final Map<String, KeysAndAttributes> unprocessedTableToKeysMap = new HashMap<>();
        final Map<String, AttributeValue> keyMap = new HashMap<>();
        keyMap.put(HASH_KEY, string(HASH_KEY_VALUE));
        if (includeRangeKey) {
            keyMap.put(RANGE_KEY, string(RANGE_KEY_VALUE));
        }
        unprocessedTableToKeysMap.put(stringHashStringRangeTableName, KeysAndAttributes.builder().keys(keyMap).build());
        return unprocessedTableToKeysMap;
    }

    private static Map<String, KeysAndAttributes> getEmptyUnprocessedTableToKeysMap() {
        final Map<String, KeysAndAttributes> unprocessedTableToKeysMap = new HashMap<>();
        final Map<String, AttributeValue> keyMap = new HashMap<>();
        unprocessedTableToKeysMap.put(stringHashStringRangeTableName, KeysAndAttributes.builder().keys(keyMap).build());
        return unprocessedTableToKeysMap;
    }

    @Test
    public void testStringHashStringRangeGetUnprocessed() {
        final TestRunner getRunner = createRunner();
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        // No actual items returned
        assertVerificationResults(getRunner, 0, 0, true);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_UNPROCESSED, 1);

        final List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_UNPROCESSED);
        for (MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_UNPROCESSED);
        }
    }

    private TestRunner createRunner() {
        return createRunner(getDynamoDB);
    }

    private TestRunner createRunner(final GetDynamoDB dynamoDB) {
        final TestRunner getRunner = TestRunners.newTestRunner(dynamoDB);
        AuthUtils.enableAccessKey(getRunner, "abcd", "defg");

        getRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        getRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, HASH_KEY);
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, RANGE_KEY);
        getRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, RANGE_KEY_VALUE);
        getRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, HASH_KEY_VALUE);
        getRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, JSON_DOCUMENT_KEY);
        return getRunner;
    }

    @Test
    public void testStringHashStringRangeGetJsonObjectNull() {
        final Map<String, List<Map<String, AttributeValue>>> responses = generateResponses(null);

        final BatchGetItemResponse response = BatchGetItemResponse.builder()
                .unprocessedKeys(getUnprocessedTableToKeysMap())
                .responses(responses)
                .build();

        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(response);
        final TestRunner getRunner = createRunner();
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        assertVerificationResults(getRunner, 1, 0);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

        final List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals(0L, flowFile.getSize());
        }

    }

    @Test
    public void testStringHashStringRangeGetJsonObjectValid() {
        final String jsonDocument = "{\"name\": \"john\"}";
        final Map<String, List<Map<String, AttributeValue>>> responses = generateResponses(jsonDocument);

        final BatchGetItemResponse response = BatchGetItemResponse.builder()
                .unprocessedKeys(getEmptyUnprocessedTableToKeysMap())
                .responses(responses)
                .build();

        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(response);

        final TestRunner getRunner = createRunner();
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);
        assertVerificationResults(getRunner, 1, 1);
        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);
    }

    @Test
    public void testStringHashStringRangeGetThrowsServiceException() {
        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenThrow(getSampleAwsServiceException());
        final TestRunner getRunner = createRunner();
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        assertVerificationFailure(getRunner);
        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals("Test AWS Service Exception (Service: Dynamo DB, Status Code: 0, Request ID: TestRequestId-1234567890)",
                    flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        }

    }

    @Test
    public void testStringHashStringRangeGetThrowsRuntimeException() {
        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenThrow(new RuntimeException("runtimeException"));

        final TestRunner getRunner = createRunner();
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        assertVerificationFailure(getRunner);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals("runtimeException", flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        }
    }

    @Test
    public void testStringHashStringRangeGetThrowsSdkException() {
        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenThrow(SdkException.builder().message("sdkException").build());

        final TestRunner getRunner = createRunner();
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        assertVerificationFailure(getRunner);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals("sdkException", flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        }

    }

    @Test
    public void testStringHashStringRangeGetNotFound() {
        final BatchGetItemResponse notFoundResponse = BatchGetItemResponse.builder().build();

        final TestRunner getRunner = createRunner();
        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(notFoundResponse);
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        assertVerificationResults(getRunner, 0, 0);

        getRunner.assertAllFlowFilesTransferred(GetDynamoDB.REL_NOT_FOUND, 1);

        List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_UNPROCESSED);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_NOT_FOUND));
        }

    }

    @Test
    public void testStringHashStringRangeGetOnlyHashFailure() {
        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenThrow(getSampleAwsServiceException());
        final TestRunner getRunner = createRunner();
        getRunner.removeProperty(GetDynamoDB.RANGE_KEY_NAME);
        getRunner.removeProperty(GetDynamoDB.RANGE_KEY_VALUE);

        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        assertVerificationFailure(getRunner);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            validateServiceExceptionAttributes(flowFile);
        }

    }

    @Test
    public void testStringHashStringRangeGetNoHashValueFailure() {
        final TestRunner getRunner = createRunner();
        getRunner.removeProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE);
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_HASH_KEY_VALUE_ERROR));
        }

    }

    @Test
    public void testStringHashStringRangeGetOnlyHashWithRangeValueNoRangeNameFailure() {
        final TestRunner getRunner = createRunner();
        getRunner.removeProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME);
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }
    }

    @Test
    public void testStringHashStringRangeGetOnlyHashWithRangeNameNoRangeValueFailure() {
        final TestRunner getRunner = createRunner();
        getRunner.removeProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE);
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }
    }

    @Test
    public void testStringHashStringNoRangeGetUnprocessed() {

        final BatchGetItemResponse response = BatchGetItemResponse.builder()
                .unprocessedKeys(getUnprocessedTableToKeysMap(false))
                .build();

        when(client.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(response);

        final TestRunner getRunner = createRunner();
        getRunner.removeProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME);
        getRunner.removeProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE);
        getRunner.enqueue(new byte[] {});

        getRunner.run(1);

        getRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_UNPROCESSED, 1);

        List<MockFlowFile> flowFiles = getRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_UNPROCESSED);
        for (MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_UNPROCESSED));
        }
    }

    private void assertVerificationFailure(final TestRunner runner) {
        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor())
                .verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(3, results.size());
        assertEquals(SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(SUCCESSFUL, results.get(1).getOutcome());
        assertEquals(FAILED, results.get(2).getOutcome());
    }

    private void assertVerificationResults(final TestRunner runner, final int expectedTotalCount, final int expectedJsonDocumentCount) {
        assertVerificationResults(runner, expectedTotalCount, expectedJsonDocumentCount, false);
    }

    private void assertVerificationResults(final TestRunner runner, final int expectedTotalCount, final int expectedJsonDocumentCount, final boolean hasUnprocessedItems) {
        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor())
                .verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(3, results.size());
        results.forEach(result -> assertEquals(SUCCESSFUL, result.getOutcome()));
        if (expectedTotalCount == 0 && !hasUnprocessedItems) {
            assertEquals("Successfully issued request, although no items were returned from DynamoDB", results.get(2).getExplanation());
        } else {
            assertTrue(results.get(2).getExplanation().contains("retrieved " + expectedTotalCount + " items"));
            assertTrue(results.get(2).getExplanation().contains(expectedJsonDocumentCount + " JSON"));
        }
    }

    private static Map<String, List<Map<String, AttributeValue>>> generateResponses(final String jsonValue) {
        final Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<>();
        final List<Map<String, AttributeValue>> items = new ArrayList<>();
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put(JSON_DOCUMENT_KEY, string(jsonValue));
        item.put(HASH_KEY, string(HASH_KEY_VALUE));
        item.put(RANGE_KEY, string(RANGE_KEY_VALUE));
        items.add(item);
        responses.put(stringHashStringRangeTableName, items);
        return responses;
    }
}
