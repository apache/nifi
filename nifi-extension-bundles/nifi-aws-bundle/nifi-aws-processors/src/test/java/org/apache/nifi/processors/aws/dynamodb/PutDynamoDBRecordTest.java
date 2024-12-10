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

import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class PutDynamoDBRecordTest {
    private static final JsonTreeReader RECORD_READER = new JsonTreeReader();
    private static final String TABLE_NAME = "table";

    @Mock
    private DynamoDbClient client;

    @Mock
    private AWSCredentialsProviderService credentialsProviderService;

    private ArgumentCaptor<BatchWriteItemRequest> captor;

    private PutDynamoDBRecord testSubject;

    @BeforeEach
    public void setUp() {
        captor = ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        when(credentialsProviderService.getIdentifier()).thenReturn("credentialProviderService");

        final BatchWriteItemResponse response = mock(BatchWriteItemResponse.class);
        final Map<String, List<WriteRequest>> unprocessedItems = new HashMap<>();
        when(response.unprocessedItems()).thenReturn(unprocessedItems);
        when(client.batchWriteItem(captor.capture())).thenReturn(response);

        testSubject = new PutDynamoDBRecord() {
            @Override
            protected DynamoDbClient getClient(final ProcessContext context) {
                return client;
            }
        };
    }

    @Test
    public void testEmptyInput() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.run();

        assertTrue(captor.getAllValues().isEmpty());
    }

    @Test
    public void testSingleInput() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/singleInput.json"));
        runner.run();

        final BatchWriteItemRequest result = captor.getValue();
        assertTrue(result.hasRequestItems());
        assertNotNull(result.requestItems().get(TABLE_NAME));
        assertItemsConvertedProperly(result.requestItems().get(TABLE_NAME), 1);
        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testMultipleInputs() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleInputs.json"));
        runner.run();

        final BatchWriteItemRequest result = captor.getValue();
        assertTrue(result.hasRequestItems());
        assertNotNull(result.requestItems().get(TABLE_NAME));
        assertItemsConvertedProperly(result.requestItems().get(TABLE_NAME), 3);
        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testMultipleChunks() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleChunks.json"));
        runner.run();

        final List<BatchWriteItemRequest> results = captor.getAllValues();
        Assertions.assertEquals(2, results.size());

        final BatchWriteItemRequest result1 = results.getFirst();
        assertTrue(result1.hasRequestItems());
        assertNotNull(result1.requestItems().get(TABLE_NAME));
        assertItemsConvertedProperly(result1.requestItems().get(TABLE_NAME), 25);

        final BatchWriteItemRequest result2 = results.get(1);
        assertTrue(result2.hasRequestItems());
        assertNotNull(result2.requestItems().get(TABLE_NAME));
        assertItemsConvertedProperly(result2.requestItems().get(TABLE_NAME), 4);

        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDynamoDBRecord.REL_SUCCESS).getFirst();
        Assertions.assertEquals("2", flowFile.getAttribute(PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE));
    }

    @Test
    public void testThroughputIssue() throws Exception {
        final TestRunner runner = getTestRunner();
        setExceedThroughputAtGivenChunk(2);

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleChunks.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_UNPROCESSED, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDynamoDBRecord.REL_UNPROCESSED).getFirst();
        Assertions.assertEquals("1", flowFile.getAttribute(PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE));
    }

    @Test
    public void testRetryAfterUnprocessed() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleChunks.json"), Collections.singletonMap(PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE, "1"));
        runner.run();

        Assertions.assertEquals(1, captor.getAllValues().size());
        Assertions.assertEquals(4, captor.getValue().requestItems().get(TABLE_NAME).size());

        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDynamoDBRecord.REL_SUCCESS).getFirst();
        Assertions.assertEquals("2", flowFile.getAttribute(PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE));
    }

    @Test
    public void testErrorDuringInsertion() throws Exception {
        final TestRunner runner = getTestRunner();
        setServerError();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleInputs.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_FAILURE, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDynamoDBRecord.REL_FAILURE).getFirst();
        Assertions.assertEquals("0", flowFile.getAttribute(PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE));
    }

    @Test
    public void testGeneratedPartitionKey() throws Exception {
        final TestRunner runner = getTestRunner();
        runner.setProperty(PutDynamoDBRecord.PARTITION_KEY_STRATEGY, PutDynamoDBRecord.PARTITION_GENERATED);
        runner.setProperty(PutDynamoDBRecord.PARTITION_KEY_FIELD, "generated");

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/singleInput.json"));
        runner.run();

        final BatchWriteItemRequest result = captor.getValue();
        Assertions.assertEquals(1, result.requestItems().get(TABLE_NAME).size());

        final Map<String, AttributeValue> item = result.requestItems().get(TABLE_NAME).getFirst().putRequest().item();
        Assertions.assertEquals(4, item.size());
        Assertions.assertEquals(string("P0"), item.get("partition"));
        assertTrue(item.containsKey("generated"));

        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testGeneratedSortKey() throws Exception {
        final TestRunner runner = getTestRunner();
        runner.setProperty(PutDynamoDBRecord.SORT_KEY_STRATEGY, PutDynamoDBRecord.SORT_BY_SEQUENCE);
        runner.setProperty(PutDynamoDBRecord.SORT_KEY_FIELD, "sort");

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleChunks.json"));
        runner.run();

        final List<Map<String, AttributeValue>> items = new ArrayList<>();
        captor.getAllValues().forEach(capture -> capture.requestItems().get(TABLE_NAME).stream()
                .map(WriteRequest::putRequest)
                .map(PutRequest::item)
                .forEach(items::add));

        Assertions.assertEquals(29, items.size());

        for (int sortKeyValue = 0; sortKeyValue < 29; sortKeyValue++) {
            final AttributeValue expectedValue = AttributeValue.builder().n(String.valueOf(sortKeyValue + 1)).build();
            Assertions.assertEquals(expectedValue, items.get(sortKeyValue).get("sort"));
        }
    }

    @Test
    public void testPartitionFieldIsMissing() throws Exception {
        final TestRunner runner = getTestRunner();
        runner.setProperty(PutDynamoDBRecord.PARTITION_KEY_FIELD, "unknownField");

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/singleInput.json"));
        runner.run();

        verify(client, never()).batchWriteItem(any(BatchWriteItemRequest.class));
        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_FAILURE, 1);
    }

    @Test
    public void testPartiallySuccessfulInsert() throws Exception {
        final TestRunner runner = getTestRunner();
        setInsertionError();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleInputs.json"));
        runner.run();

        verify(client, times(1)).batchWriteItem(any(BatchWriteItemRequest.class));
        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_FAILURE, 1);
    }

    @Test
    public void testNonRecordOrientedInput() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/nonRecordOriented.txt"));
        runner.run();


        assertTrue(captor.getAllValues().isEmpty());
        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_FAILURE, 1);
    }

    private TestRunner getTestRunner() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(testSubject);

        runner.addControllerService("recordReader", RECORD_READER);
        runner.addControllerService("credentialProviderService", credentialsProviderService);

        runner.enableControllerService(RECORD_READER);
        runner.enableControllerService(credentialsProviderService);

        runner.setProperty(PutDynamoDBRecord.RECORD_READER, "recordReader");
        runner.setProperty(PutDynamoDBRecord.AWS_CREDENTIALS_PROVIDER_SERVICE, "credentialProviderService");
        runner.setProperty(PutDynamoDBRecord.TABLE, TABLE_NAME);
        runner.setProperty(PutDynamoDBRecord.PARTITION_KEY_STRATEGY, PutDynamoDBRecord.PARTITION_BY_FIELD);
        runner.setProperty(PutDynamoDBRecord.PARTITION_KEY_FIELD, "partition");
        runner.setProperty(PutDynamoDBRecord.SORT_KEY_FIELD, PutDynamoDBRecord.SORT_NONE);
        return runner;
    }

    private void assertItemsConvertedProperly(final Collection<WriteRequest> writeRequests, final int expectedNumberOfItems) {
        Assertions.assertEquals(expectedNumberOfItems, writeRequests.size());
        int index = 0;

        for (final WriteRequest writeRequest : writeRequests) {
            final PutRequest putRequest = writeRequest.putRequest();
            assertNotNull(putRequest);
            final Map<String, AttributeValue> item = putRequest.item();
            Assertions.assertEquals(3, item.size());
            Assertions.assertEquals(string("new"), item.get("value"));

            Assertions.assertEquals(number(index), item.get("size"));
            Assertions.assertEquals(string("P" + index), item.get("partition"));
            index++;
        }
    }

    private void setInsertionError() {
        final BatchWriteItemResponse outcome = mock(BatchWriteItemResponse.class);
        final Map<String, List<WriteRequest>> unprocessedItems = new HashMap<>();
        final List<WriteRequest> writeResults = Collections.singletonList(mock(WriteRequest.class));
        unprocessedItems.put("test", writeResults);
        when(outcome.unprocessedItems()).thenReturn(unprocessedItems);
        when(outcome.hasUnprocessedItems()).thenReturn(true);
        when(client.batchWriteItem(captor.capture())).thenReturn(outcome);
    }

    private void setServerError() {
        when(client.batchWriteItem(captor.capture())).thenThrow(AwsServiceException.builder().message("Error")
                .awsErrorDetails(AwsErrorDetails.builder().errorMessage("Error").errorCode("Code").build()).build());
    }

    private void setExceedThroughputAtGivenChunk(final int chunkToFail) {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);

        when(client.batchWriteItem(captor.capture())).then(invocationOnMock -> {
            final int calls = numberOfCalls.incrementAndGet();

            if (calls >= chunkToFail) {
                throw ProvisionedThroughputExceededException.builder().message("Throughput exceeded")
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("error code").errorMessage("error message").build()).build();
            } else {
                return mock(BatchWriteItemResponse.class);
            }
        });
    }

    protected static AttributeValue string(final String s) {
        return AttributeValue.builder().s(s).build();
    }

    protected static AttributeValue number(final Number number) {
        return AttributeValue.builder().n(String.valueOf(number)).build();
    }
}
