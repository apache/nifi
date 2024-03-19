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

package org.apache.nifi.processors.cassandra;

import org.apache.nifi.cassandra.api.CQLExecutionService;
import org.apache.nifi.cassandra.api.UpdateMethod;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PutCQLRecordTest {
    private TestRunner runner;
    private CQLExecutionService service;
    private MockRecordParser mockReader;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutCQLRecord.class);
        service = Mockito.mock(CQLExecutionService.class);
        mockReader = new MockRecordParser();
        mockReader.addSchemaField("message", RecordFieldType.STRING);
        mockReader.addSchemaField("sender", RecordFieldType.STRING);

        when(service.getIdentifier()).thenReturn("executionService");

        runner.setProperty(PutCQLRecord.CONNECTION_PROVIDER_SERVICE, service.getIdentifier());
        runner.setProperty(PutCQLRecord.TABLE, "message");
        runner.setProperty(PutCQLRecord.RECORD_READER_FACTORY, "reader");

        runner.addControllerService("reader", mockReader);
        runner.enableControllerService(mockReader);

        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);
    }

    @Test
    void testInsert() {
        final int recordCount = 1000;
        final int batchCount = 10;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(batchCount))
                .insert(eq("message"), anyList());
    }

    @ParameterizedTest
    @CsvSource({ "DECREMENT,COUNTER", "INCREMENT,COUNTER", "SET,LOGGED" })
    void testUpdate(UpdateMethod updateMethod, String batchStatementType) {
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, PutCQLRecord.UPDATE_TYPE.getValue());
        runner.setProperty(PutCQLRecord.UPDATE_METHOD, updateMethod.name());
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, batchStatementType);

        final int recordCount = 1050;
        final int batchCount = 11;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(batchCount))
                .update(eq("message"), anyList(), eq(List.of("sender")), eq(updateMethod));
    }

    @Test
    void testErrorHandler() {
        final int recordCount = 1;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        doThrow(new ProcessException("Test"))
                .when(service)
                .insert(anyString(), anyList());
        runner.enqueue("");
        runner.run();
        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);
    }
}
