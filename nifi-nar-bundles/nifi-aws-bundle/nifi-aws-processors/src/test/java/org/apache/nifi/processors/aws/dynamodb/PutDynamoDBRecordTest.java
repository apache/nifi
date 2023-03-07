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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
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
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class PutDynamoDBRecordTest {
    private static final JsonTreeReader RECORD_READER = new JsonTreeReader();
    private static final String TABLE_NAME = "table";

    @Mock
    private DynamoDB mockDynamoDB;

    @Mock
    private AWSCredentialsProviderService credentialsProviderService;

    private ArgumentCaptor<TableWriteItems> captor;

    private PutDynamoDBRecord testSubject;

    @BeforeEach
    public void setUp() {
        captor = ArgumentCaptor.forClass(TableWriteItems.class);
        Mockito.when(credentialsProviderService.getIdentifier()).thenReturn("credentialProviderService");

        final BatchWriteItemOutcome outcome = Mockito.mock(BatchWriteItemOutcome.class);
        final Map<String, List<WriteRequest>> unprocessedItems = new HashMap<>();
        Mockito.when(outcome.getUnprocessedItems()).thenReturn(unprocessedItems);
        Mockito.when(mockDynamoDB.batchWriteItem(captor.capture())).thenReturn(outcome);

        testSubject = new PutDynamoDBRecord() {
            @Override
            protected DynamoDB getDynamoDB(ProcessContext context) {
                return mockDynamoDB;
            }
        };
    }

    @Test
    public void testEmptyInput() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.run();

        Assertions.assertTrue(captor.getAllValues().isEmpty());
    }

    @Test
    public void testSingleInput() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/singleInput.json"));
        runner.run();

        final TableWriteItems result = captor.getValue();
        Assertions.assertEquals(TABLE_NAME, result.getTableName());
        assertItemsConvertedProperly(result.getItemsToPut(), 1);
        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testMultipleInputs() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleInputs.json"));
        runner.run();

        final TableWriteItems result = captor.getValue();
        Assertions.assertEquals(TABLE_NAME, result.getTableName());
        assertItemsConvertedProperly(result.getItemsToPut(), 3);
        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testMultipleChunks() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleChunks.json"));
        runner.run();

        final List<TableWriteItems> results = captor.getAllValues();
        Assertions.assertEquals(2, results.size());

        final TableWriteItems result1 = results.get(0);
        Assertions.assertEquals(TABLE_NAME, result1.getTableName());
        assertItemsConvertedProperly(result1.getItemsToPut(), 25);

        final TableWriteItems result2 = results.get(1);
        Assertions.assertEquals(TABLE_NAME, result2.getTableName());
        assertItemsConvertedProperly(result2.getItemsToPut(), 4);

        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDynamoDBRecord.REL_SUCCESS).get(0);
        Assertions.assertEquals("2", flowFile.getAttribute(PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE));
    }

    @Test
    public void testThroughputIssue() throws Exception {
        final TestRunner runner = getTestRunner();
        setExceedThroughputAtGivenChunk(2);

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleChunks.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_UNPROCESSED, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDynamoDBRecord.REL_UNPROCESSED).get(0);
        Assertions.assertEquals("1", flowFile.getAttribute(PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE));
    }

    @Test
    public void testRetryAfterUnprocessed() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleChunks.json"), Collections.singletonMap(PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE, "1"));
        runner.run();

        Assertions.assertEquals(1, captor.getAllValues().size());
        Assertions.assertEquals(4, captor.getValue().getItemsToPut().size());

        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDynamoDBRecord.REL_SUCCESS).get(0);
        Assertions.assertEquals("2", flowFile.getAttribute(PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE));
    }

    @Test
    public void testErrorDuringInsertion() throws Exception {
        final TestRunner runner = getTestRunner();
        setServerError();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleInputs.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_FAILURE, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutDynamoDBRecord.REL_FAILURE).get(0);
        Assertions.assertEquals("0", flowFile.getAttribute(PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE));
    }

    @Test
    public void testGeneratedPartitionKey() throws Exception {
        final TestRunner runner = getTestRunner();
        runner.setProperty(PutDynamoDBRecord.PARTITION_KEY_STRATEGY, PutDynamoDBRecord.PARTITION_GENERATED);
        runner.setProperty(PutDynamoDBRecord.PARTITION_KEY_FIELD, "generated");

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/singleInput.json"));
        runner.run();

        final TableWriteItems result = captor.getValue();
        Assertions.assertEquals(1, result.getItemsToPut().size());

        final Item item = result.getItemsToPut().iterator().next();
        Assertions.assertEquals(4, item.asMap().size());
        Assertions.assertEquals("P0", item.get("partition"));
        Assertions.assertTrue(item.hasAttribute("generated"));

        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testGeneratedSortKey() throws Exception {
        final TestRunner runner = getTestRunner();
        runner.setProperty(PutDynamoDBRecord.SORT_KEY_STRATEGY, PutDynamoDBRecord.SORT_BY_SEQUENCE);
        runner.setProperty(PutDynamoDBRecord.SORT_KEY_FIELD, "sort");

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleChunks.json"));
        runner.run();

        final List<Item> items = new ArrayList<>();
        captor.getAllValues().forEach(capture -> items.addAll(capture.getItemsToPut()));

        Assertions.assertEquals(29, items.size());

        for (int sortKeyValue = 0; sortKeyValue < 29; sortKeyValue++) {
            Assertions.assertEquals(new BigDecimal(sortKeyValue + 1), items.get(sortKeyValue).get("sort"));
        }
    }

    @Test
    public void testPartitionFieldIsMissing() throws Exception {
        final TestRunner runner = getTestRunner();
        runner.setProperty(PutDynamoDBRecord.PARTITION_KEY_FIELD, "unknownField");

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/singleInput.json"));
        runner.run();

        Mockito.verify(mockDynamoDB, Mockito.never()).batchWriteItem(Mockito.any(TableWriteItems.class));
        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_FAILURE, 1);
    }

    @Test
    public void testPartiallySuccessfulInsert() throws Exception {
        final TestRunner runner = getTestRunner();
        setInsertionError();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/multipleInputs.json"));
        runner.run();

        Mockito.verify(mockDynamoDB, Mockito.times(1)).batchWriteItem(Mockito.any(TableWriteItems.class));
        runner.assertAllFlowFilesTransferred(PutDynamoDBRecord.REL_FAILURE, 1);
    }

    @Test
    public void testNonRecordOrientedInput() throws Exception {
        final TestRunner runner = getTestRunner();

        runner.enqueue(new FileInputStream("src/test/resources/dynamodb/nonRecordOriented.txt"));
        runner.run();


        Assertions.assertTrue(captor.getAllValues().isEmpty());
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

    private void assertItemsConvertedProperly(final Collection<Item> items, final int expectedNumberOfItems) {
        Assertions.assertEquals(expectedNumberOfItems, items.size());
        int index = 0;

        for (final Item item : items) {
            Assertions.assertEquals(3, item.asMap().size());
            Assertions.assertEquals("new", item.get("value"));

            Assertions.assertEquals(new BigDecimal(index), item.get("size"));
            Assertions.assertEquals("P" + index, item.get("partition"));
            index++;
        }
    }

    private void setInsertionError() {
        final BatchWriteItemOutcome outcome = Mockito.mock(BatchWriteItemOutcome.class);
        final Map<String, List<WriteRequest>> unprocessedItems = new HashMap<>();
        final List<WriteRequest> writeResults = Arrays.asList(Mockito.mock(WriteRequest.class));
        unprocessedItems.put("test", writeResults);
        Mockito.when(outcome.getUnprocessedItems()).thenReturn(unprocessedItems);
        Mockito.when(mockDynamoDB.batchWriteItem(captor.capture())).thenReturn(outcome);
    }

    private void setServerError() {
        Mockito.when(mockDynamoDB.batchWriteItem(captor.capture())).thenThrow(new AmazonServiceException("Error"));
    }

    private void setExceedThroughputAtGivenChunk(final int chunkToFail) {
        final AtomicInteger numberOfCalls = new AtomicInteger(0);

        Mockito.when(mockDynamoDB.batchWriteItem(captor.capture())).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                final int calls = numberOfCalls.incrementAndGet();

                if (calls >= chunkToFail) {
                    throw new ProvisionedThroughputExceededException("Throughput exceeded");
                } else {
                    return Mockito.mock(BatchWriteItemOutcome.class);
                }
            }
        });
    }
}
