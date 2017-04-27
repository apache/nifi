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

package org.apache.nifi.util.hive;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.InvalidTable;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HiveWriterTest {
    private HiveEndPoint hiveEndPoint;
    private int txnsPerBatch;
    private boolean autoCreatePartitions;
    private int callTimeout;
    private ExecutorService executorService;
    private UserGroupInformation userGroupInformation;
    private HiveConf hiveConf;
    private HiveWriter hiveWriter;
    private StreamingConnection streamingConnection;
    private RecordWriter recordWriter;
    private Callable<RecordWriter> recordWriterCallable;
    private TransactionBatch transactionBatch;

    @Before
    public void setup() throws Exception {
        hiveEndPoint = mock(HiveEndPoint.class);
        txnsPerBatch = 100;
        autoCreatePartitions = true;
        callTimeout = 0;
        executorService = mock(ExecutorService.class);
        streamingConnection = mock(StreamingConnection.class);
        transactionBatch = mock(TransactionBatch.class);
        userGroupInformation = mock(UserGroupInformation.class);
        hiveConf = mock(HiveConf.class);
        recordWriter = mock(RecordWriter.class);
        recordWriterCallable = mock(Callable.class);
        when(recordWriterCallable.call()).thenReturn(recordWriter);

        when(hiveEndPoint.newConnection(autoCreatePartitions, hiveConf, userGroupInformation)).thenReturn(streamingConnection);
        when(streamingConnection.fetchTransactionBatch(txnsPerBatch, recordWriter)).thenReturn(transactionBatch);
        when(executorService.submit(isA(Callable.class))).thenAnswer(invocation -> {
            Future future = mock(Future.class);
            Answer<Object> answer = i -> ((Callable) invocation.getArguments()[0]).call();
            when(future.get()).thenAnswer(answer);
            when(future.get(anyLong(), any(TimeUnit.class))).thenAnswer(answer);
            return future;
        });
        when(userGroupInformation.doAs(isA(PrivilegedExceptionAction.class))).thenAnswer(invocation -> {
            try {
                try {
                    return ((PrivilegedExceptionAction) invocation.getArguments()[0]).run();
                } catch (UncheckedExecutionException e) {
                    // Creation of strict json writer will fail due to external deps, this gives us chance to catch it
                    for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                        if (stackTraceElement.toString().startsWith("org.apache.hive.hcatalog.streaming.StrictJsonWriter.<init>(")) {
                            return recordWriterCallable.call();
                        }
                    }
                    throw e;
                }
            } catch (IOException | Error | RuntimeException | InterruptedException e) {
                throw e;
            } catch (Throwable e) {
                throw new UndeclaredThrowableException(e);
            }
        });

        initWriter();
    }

    private void initWriter() throws Exception {
        hiveWriter = new HiveWriter(hiveEndPoint, txnsPerBatch, autoCreatePartitions, callTimeout, executorService, userGroupInformation, hiveConf);
    }

    @Test
    public void testNormal() {
        assertNotNull(hiveWriter);
    }

    @Test(expected = HiveWriter.ConnectFailure.class)
    public void testNewConnectionInvalidTable() throws Exception {
        hiveEndPoint = mock(HiveEndPoint.class);
        InvalidTable invalidTable = new InvalidTable("badDb", "badTable");
        when(hiveEndPoint.newConnection(autoCreatePartitions, hiveConf, userGroupInformation)).thenThrow(invalidTable);
        try {
            initWriter();
        } catch (HiveWriter.ConnectFailure e) {
            assertEquals(invalidTable, e.getCause());
            throw e;
        }
    }

    @Test(expected = HiveWriter.ConnectFailure.class)
    public void testRecordWriterStreamingException() throws Exception {
        recordWriterCallable = mock(Callable.class);
        StreamingException streamingException = new StreamingException("Test Exception");
        when(recordWriterCallable.call()).thenThrow(streamingException);
        try {
            initWriter();
        } catch (HiveWriter.ConnectFailure e) {
            assertEquals(streamingException, e.getCause());
            throw e;
        }
    }
}
