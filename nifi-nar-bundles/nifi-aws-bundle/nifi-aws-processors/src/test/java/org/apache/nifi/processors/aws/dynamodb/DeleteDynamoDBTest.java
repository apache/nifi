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
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteDynamoDBTest extends AbstractDynamoDBTest {

    protected DeleteDynamoDB deleteDynamoDB;

    @BeforeEach
    public void setUp() {
        client = mock(DynamoDbClient.class);

        deleteDynamoDB = new DeleteDynamoDB() {
            @Override
            protected DynamoDbClient getClient(final ProcessContext context) {
                return client;
            }
        };

        when(client.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(BatchWriteItemResponse.builder().build());
    }

    private TestRunner createRunner() throws InitializationException {
        return createRunner(deleteDynamoDB);
    }

    private TestRunner createRunner(final DeleteDynamoDB processor) throws InitializationException {
        final TestRunner deleteRunner = TestRunners.newTestRunner(processor);
        AuthUtils.enableAccessKey(deleteRunner, "abcd", "cdef");

        deleteRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.TABLE, stringHashStringRangeTableName);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_NAME, "hashS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "h1");

        return deleteRunner;
    }

    @Test
    public void testStringHashStringRangeDeleteOnlyHashFailure() throws InitializationException {
        // When writing, mock thrown service exception from AWS
        when(client.batchWriteItem(any(BatchWriteItemRequest.class))).thenThrow(getSampleAwsServiceException());

        final TestRunner deleteRunner = createRunner();

        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            validateServiceExceptionAttributes(flowFile);
        }

    }

    @Test
    public void testStringHashStringRangeDeleteSuccessfulWithMock() throws InitializationException {
        final TestRunner deleteRunner = createRunner();
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);

    }

    @Test
    public void testStringHashStringRangeDeleteSuccessfulWithMockOneUnprocessed() throws InitializationException {
        final Map<String, List<WriteRequest>> unprocessed = new HashMap<>();
        final DeleteRequest delete = DeleteRequest.builder().key(Map
                .of(
                        "hashS", string("h1"),
                        "rangeS", string("r1")
                )).build();
        final WriteRequest write = WriteRequest.builder().deleteRequest(delete).build();
        final List<WriteRequest> writes = new ArrayList<>();
        writes.add(write);
        unprocessed.put(stringHashStringRangeTableName, writes);
        final BatchWriteItemResponse response = BatchWriteItemResponse.builder().unprocessedItems(unprocessed).build();
        when(client.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(response);
        final TestRunner deleteRunner = createRunner();
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_UNPROCESSED, 1);

    }

    @Test
    public void testStringHashStringRangeDeleteNoHashValueFailure() throws InitializationException {
        final TestRunner deleteRunner = createRunner(new DeleteDynamoDB());

        deleteRunner.setProperty(AbstractDynamoDBProcessor.REGION, REGION);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        deleteRunner.removeProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE);
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_HASH_KEY_VALUE_ERROR));
        }

    }

    @Test
    public void testStringHashStringRangeDeleteOnlyHashWithRangeValueNoRangeNameFailure() throws InitializationException {
        final TestRunner deleteRunner = createRunner(new DeleteDynamoDB());

        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }

    }

    @Test
    public void testStringHashStringRangeDeleteOnlyHashWithRangeNameNoRangeValueFailure() throws InitializationException {
        final TestRunner deleteRunner = createRunner(new DeleteDynamoDB());
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);

        final List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR));
        }
    }

    @Test
    public void testStringHashStringRangeDeleteNonExistentHashSuccess() throws InitializationException {
        final TestRunner deleteRunner = createRunner();

        deleteRunner.setProperty(AbstractDynamoDBProcessor.HASH_KEY_VALUE, "nonexistent");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);
    }

    @Test
    public void testStringHashStringRangeDeleteNonExistentRangeSuccess() throws InitializationException {
        final TestRunner deleteRunner = createRunner();
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "nonexistent");
        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_SUCCESS, 1);
    }

    @Test
    public void testStringHashStringRangeDeleteThrowsServiceException() throws InitializationException {

        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenThrow(getSampleAwsServiceException());
        final TestRunner deleteRunner = createRunner();
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");

        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals("Test AWS Service Exception (Service: Dynamo DB, Status Code: 0, Request ID: TestRequestId-1234567890)",
                    flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        }

    }

    @Test
    public void testStringHashStringRangeDeleteThrowsClientException() throws InitializationException {
        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenThrow(SdkException.builder().message("sdkException").build());

        final TestRunner deleteRunner = createRunner(deleteDynamoDB);
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_NAME, "rangeS");
        deleteRunner.setProperty(AbstractDynamoDBProcessor.RANGE_KEY_VALUE, "r1");

        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (final MockFlowFile flowFile : flowFiles) {
            assertEquals("sdkException", flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        }

    }

    @Test
    public void testStringHashStringRangeDeleteThrowsRuntimeException() throws InitializationException {
        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenThrow(new RuntimeException("runtimeException"));

        final TestRunner deleteRunner = createRunner();

        deleteRunner.enqueue(new byte[] {});

        deleteRunner.run(1);

        deleteRunner.assertAllFlowFilesTransferred(AbstractDynamoDBProcessor.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = deleteRunner.getFlowFilesForRelationship(AbstractDynamoDBProcessor.REL_FAILURE);
        for (MockFlowFile flowFile : flowFiles) {
            assertEquals("runtimeException", flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        }
    }
}
