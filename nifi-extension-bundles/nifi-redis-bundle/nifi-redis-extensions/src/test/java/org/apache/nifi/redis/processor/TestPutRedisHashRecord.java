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
package org.apache.nifi.redis.processor;


import org.apache.nifi.redis.service.RedisConnectionPoolService;
import org.apache.nifi.redis.util.RedisUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisHashCommands;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestPutRedisHashRecord {

    private TestRunner runner;
    private MockRecordParser parser;

    private MockRedisConnectionPoolService connectionPoolService;

    @BeforeEach
    public void setup() throws Exception {
        PutRedisHashRecord processor = new PutRedisHashRecord();
        runner = TestRunners.newTestRunner(processor);
        parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.setProperty(PutRedisHashRecord.RECORD_READER_FACTORY, "parser");

        connectionPoolService = new MockRedisConnectionPoolService();
        connectionPoolService.setFailAfterN(0);
        try {
            runner.addControllerService("connectionPool", connectionPoolService);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.setProperty(connectionPoolService, RedisUtils.CONNECTION_STRING, "localhost:6379");
        runner.setProperty(RedisUtils.REDIS_CONNECTION_POOL, "connectionPool");
        // Tests should provide a field named 'hash' with unique values per record, unless testing failure conditions
        runner.setProperty(PutRedisHashRecord.HASH_VALUE_RECORD_PATH, "/hash");
    }

    @Test
    public void testPutRecords() {
        runner.assertNotValid();
        runner.enableControllerService(connectionPoolService);
        parser.addSchemaField("hash", RecordFieldType.STRING);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("age", RecordFieldType.INT);

        parser.addRecord("abc", "John Doe", 48);
        parser.addRecord("def", "Jane Doe", 47);
        parser.addRecord("ghi", "Jimmy Doe", 14);

        runner.enqueue("hello".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutRedisHashRecord.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(PutRedisHashRecord.REL_SUCCESS);
        assertEquals(1, result.size());
        MockFlowFile ff = result.getFirst();
        ff.assertAttributeEquals(PutRedisHashRecord.SUCCESS_RECORD_COUNT, "3");
        // Verify the content is untouched
        ff.assertContentEquals("hello");
    }

    @Test
    public void testPutRecordsFailAfterN() {
        runner.assertNotValid();
        connectionPoolService.setFailAfterN(5);
        runner.enableControllerService(connectionPoolService);
        parser.addSchemaField("hash", RecordFieldType.STRING);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("age", RecordFieldType.INT);

        parser.addRecord("abc", "John Doe", 48);
        parser.addRecord("def", "Jane Doe", 47);
        parser.addRecord("ghi", "Jimmy Doe", 14);

        runner.enqueue("hello".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutRedisHashRecord.REL_FAILURE, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(PutRedisHashRecord.REL_FAILURE);
        assertEquals(1, result.size());
        MockFlowFile ff = result.getFirst();
        ff.assertAttributeEquals(PutRedisHashRecord.SUCCESS_RECORD_COUNT, "1");
        // Verify the content is untouched
        ff.assertContentEquals("hello");
    }

    @Test
    public void testPutRecordsNoHash() {
        runner.assertNotValid();
        runner.setProperty(PutRedisHashRecord.HASH_VALUE_RECORD_PATH, "/invalid_path");
        runner.assertValid(connectionPoolService);
        runner.enableControllerService(connectionPoolService);
        parser.addSchemaField("hash", RecordFieldType.STRING);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("age", RecordFieldType.INT);

        parser.addRecord("abc", "John Doe", 48);

        runner.enqueue("hello".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutRedisHashRecord.REL_FAILURE, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(PutRedisHashRecord.REL_FAILURE);
        assertEquals(1, result.size());
        MockFlowFile ff = result.getFirst();
        ff.assertAttributeEquals(PutRedisHashRecord.SUCCESS_RECORD_COUNT, "0");
        // Verify the content is untouched
        ff.assertContentEquals("hello");
    }

    @Test
    public void testPutRecordsNullValue() {
        runner.assertNotValid();
        runner.enableControllerService(connectionPoolService);
        parser.addSchemaField("hash", RecordFieldType.STRING);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("age", RecordFieldType.INT);

        parser.addRecord("abc", "John Doe", 48);
        parser.addRecord("def", "Jane Doe", null);

        runner.enqueue("hello".getBytes(StandardCharsets.UTF_8));
        runner.run();

        // FlowFile should be routed to success but with only one record
        runner.assertAllFlowFilesTransferred(PutRedisHashRecord.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(PutRedisHashRecord.REL_SUCCESS);
        assertEquals(1, result.size());
        MockFlowFile ff = result.getFirst();
        // Both records are transferred successfully, but the null value was not put into Redis
        ff.assertAttributeEquals(PutRedisHashRecord.SUCCESS_RECORD_COUNT, "2");
        // Verify the content is untouched
        ff.assertContentEquals("hello");
    }

    private static class MockRedisConnectionPoolService extends RedisConnectionPoolService {

        private final Map<String, Map<String, String>> hashStore = new HashMap<>();
        private int failAfterN = 0;
        private int currentFailures = 0;

        @Override
        public RedisConnection getConnection() {
            currentFailures = 0;
            RedisConnection mockRedisConnection = mock(RedisConnection.class);
            RedisHashCommands hashCommands = mock(RedisHashCommands.class);
            when(hashCommands.hSet(any(byte[].class), any(byte[].class), any(byte[].class))).thenAnswer((Answer<Boolean>) invocationOnMock -> {
                currentFailures++;
                if (failAfterN > 0 && currentFailures > failAfterN) {
                    throw new UncheckedIOException(new IOException("error during hset"));
                }
                final byte[] hashValue = invocationOnMock.getArgument(0);
                final byte[] keyValue = invocationOnMock.getArgument(1);
                final byte[] valueValue = invocationOnMock.getArgument(2);

                if (hashValue == null || keyValue == null || valueValue == null) {
                    throw new NullPointerException("hash, key, and value must not be null");
                }

                final String hashString = new String(hashValue);

                Map<String, String> kvMap = hashStore.get(hashString);
                if (kvMap == null) {
                    kvMap = new HashMap<>();
                }
                kvMap.put(new String(keyValue), new String(valueValue));
                hashStore.put(hashString, kvMap);

                return Boolean.TRUE;
            });
            when(mockRedisConnection.hashCommands()).thenReturn(hashCommands);
            return mockRedisConnection;
        }

        public void setFailAfterN(int triesBeforeFailure) {
            failAfterN = Math.max(triesBeforeFailure, 0);
        }
    }
}