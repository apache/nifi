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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PutDynamoDBTest extends AbstractDynamoDBTest {
    private static final byte[] HELLO_2_BYTES = "{\"hell\": 2}".getBytes(StandardCharsets.UTF_8);
    protected PutDynamoDB putDynamoDB;

    @BeforeEach
    public void setUp() {
        final Map<String, Collection<Map<String, AttributeValue>>> responses = new HashMap<>();
        responses.put(stringHashStringRangeTableName, Collections.emptyList());
        final BatchWriteItemResponse response = BatchWriteItemResponse.builder().build();

        client = mock(DynamoDbClient.class);

        when(client.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(response);

        putDynamoDB = new PutDynamoDB() {
            @Override
            protected DynamoDbClient getClient(final ProcessContext context) {
                return client;
            }
        };
    }

    private TestRunner createRunner() throws InitializationException {
        return createRunner(putDynamoDB);
    }

    private TestRunner createRunner(final PutDynamoDB processor) {
        final TestRunner putRunner = TestRunners.newTestRunner(processor);
        AuthUtils.enableAccessKey(putRunner, "abcd", "cdef");

        putRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        putRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        putRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        putRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        putRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "document");
        return putRunner;
    }

    @Test
    public void testStringHashStringRangePutOnlyHashFailure() throws InitializationException {
        when(client.batchWriteItem(any(BatchWriteItemRequest.class))).thenThrow(getSampleAwsServiceException());

        final TestRunner putRunner = createRunner();
        putRunner.enqueue(HELLO_2_BYTES);

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            validateServiceExceptionAttributes(flowFile);
        }

    }

    @Test
    public void testStringHashStringRangePutNoHashValueFailure() {
        final TestRunner putRunner = createRunner(new PutDynamoDB());
        putRunner.removeProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE);
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        putRunner.enqueue(HELLO_2_BYTES);

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_HASH_KEY_VALUE_ERROR));
        }
    }

    @Test
    public void testStringHashStringRangePutOnlyHashWithRangeValueNoRangeNameFailure() throws InitializationException {
        final TestRunner putRunner = createRunner(new PutDynamoDB());
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        putRunner.enqueue(new byte[] {});

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }
    }

    @Test
    public void testStringHashStringRangePutOnlyHashWithRangeNameNoRangeValueFailure() throws InitializationException {
        final TestRunner putRunner = createRunner(new PutDynamoDB());

        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");
        putRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "j1");
        putRunner.enqueue(new byte[] {});

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }
    }

    @Test
    public void testStringHashStringRangePutSuccessfulWithMock() throws InitializationException {
        final TestRunner putRunner = createRunner();
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

        final List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals(document, new String(flowFile.toByteArray()));
        }
    }

    @Test
    public void testStringHashStringRangePutOneSuccessfulOneSizeFailureWithMockBatchSize1() throws InitializationException {
        final TestRunner putRunner = createRunner();
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());

        byte[] item = new byte[PutDynamoDB.DYNAMODB_MAX_ITEM_SIZE + 1];
        Arrays.fill(item, (byte) 'a');
        String document2 = new String(item);
        putRunner.enqueue(document2.getBytes());

        putRunner.run(2, true, true);

        final List<MockFlowFile> flowFilesFailed = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFilesFailed) {
            flowFile.assertAttributeExists(PutDynamoDB.AWS_DYNAMO_DB_ITEM_SIZE_ERROR);
            assertEquals(item.length, flowFile.getSize());
        }

        final List<MockFlowFile> flowFilesSuccessful = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFilesSuccessful) {
            assertEquals(document, new String(flowFile.toByteArray()));
        }
    }

    @Test
    public void testStringHashStringRangePutOneSuccessfulOneSizeFailureWithMockBatchSize5() throws InitializationException {
        final TestRunner putRunner = createRunner();
        putRunner.setProperty(AbstractDynamoDBProcessor.BATCH_SIZE, "5");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());

        byte[] item = new byte[PutDynamoDB.DYNAMODB_MAX_ITEM_SIZE + 1];
        Arrays.fill(item, (byte) 'a');
        String document2 = new String(item);
        putRunner.enqueue(document2.getBytes());

        putRunner.run(1);

        final List<MockFlowFile> flowFilesFailed = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFilesFailed) {
            flowFile.assertAttributeExists(PutDynamoDB.AWS_DYNAMO_DB_ITEM_SIZE_ERROR);
            assertEquals(item.length, flowFile.getSize());
        }

        final List<MockFlowFile> flowFilesSuccessful = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFilesSuccessful) {
            assertEquals(document, new String(flowFile.toByteArray()));
        }
    }

    @Test
    public void testStringHashStringRangePutFailedWithItemSizeGreaterThan400Kb() throws InitializationException {
        final TestRunner putRunner = createRunner();
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        byte[] item = new byte[PutDynamoDB.DYNAMODB_MAX_ITEM_SIZE + 1];
        Arrays.fill(item, (byte) 'a');
        String document = new String(item);
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        assertEquals(1, flowFiles.size());
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeExists(PutDynamoDB.AWS_DYNAMO_DB_ITEM_SIZE_ERROR);
            assertEquals(item.length, flowFile.getSize());
        }
    }

    @Test
    public void testStringHashStringRangePutThrowsServiceException() throws InitializationException {
        when(client.batchWriteItem(any(BatchWriteItemRequest.class))).thenThrow(getSampleAwsServiceException());

        final TestRunner putRunner = createRunner();
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals("Test AWS Service Exception (Service: Dynamo DB, Status Code: 0, Request ID: TestRequestId-1234567890)",
                    flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        }

    }

    @Test
    public void testStringHashStringRangePutThrowsSdkException() throws InitializationException {
        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenThrow(SdkException.builder().message("sdkException").build());
        final TestRunner putRunner = createRunner();
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals("sdkException", flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        }

    }

    @Test
    public void testStringHashStringRangePutThrowsRuntimeException() throws InitializationException {
        when(client.batchWriteItem(any(BatchWriteItemRequest.class))).thenThrow(new RuntimeException("runtimeException"));
        final TestRunner putRunner = createRunner();

        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        String document = "{\"name\":\"john\"}";
        putRunner.enqueue(document.getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = putRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals("runtimeException", flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        }

    }

    @Test
    public void testStringHashStringRangePutSuccessfulWithMockOneUnprocessed() throws InitializationException {
        final Map<String, Collection<WriteRequest>> unprocessedTableToKeysMap = new HashMap<>();
        final Map<String, AttributeValue> keyMap = new HashMap<>();
        keyMap.put("hashS", string("h1"));
        keyMap.put("rangeS", string("r1"));
        final WriteRequest writeRequest = WriteRequest.builder().putRequest(PutRequest.builder().item(keyMap).build()).build();

        unprocessedTableToKeysMap.put(stringHashStringRangeTableName, Collections.singletonList(writeRequest));

        final BatchWriteItemResponse response = BatchWriteItemResponse.builder()
                .unprocessedItems(unprocessedTableToKeysMap)
                .build();

        when(client.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(response);

        final TestRunner putRunner = createRunner();
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        putRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        putRunner.setProperty(AbstractDynamoDBProcessor.JSON_DOCUMENT, "j2");
        putRunner.enqueue("{\"hello\":\"world\"}".getBytes());

        putRunner.run(1);

        putRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_UNPROCESSED, 1);

    }

}
