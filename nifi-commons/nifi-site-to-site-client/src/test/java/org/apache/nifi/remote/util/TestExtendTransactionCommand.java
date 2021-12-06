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
package org.apache.nifi.remote.util;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.SocketTimeoutException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestExtendTransactionCommand {
    private static final String TRANSACTION_URL = "https://localhost:8443/nifi-api/transaction-id";

    private static final TransactionResultEntity RESULT_ENTITY = new TransactionResultEntity();

    @Mock
    private SiteToSiteRestApiClient client;

    @Mock
    private EventReporter eventReporter;

    private ExtendTransactionCommand command;

    @BeforeEach
    public void setCommand() {
        command = new ExtendTransactionCommand(client, TRANSACTION_URL, eventReporter);
    }

    @Test
    public void testRun() throws IOException {
        when(client.extendTransaction(eq(TRANSACTION_URL))).thenReturn(RESULT_ENTITY);

        command.run();

        verifyNoInteractions(eventReporter);
    }

    @Test
    public void testRunIllegalStateExceptionClientNotClosed() throws IOException {
        when(client.extendTransaction(eq(TRANSACTION_URL))).thenThrow(new IllegalStateException());

        command.run();

        verifyNoInteractions(eventReporter);
        verify(client, never()).close();
    }

    @Test
    public void testRunSocketTimeoutExceptionClientClosed() throws IOException {
        when(client.extendTransaction(eq(TRANSACTION_URL))).thenThrow(new SocketTimeoutException());

        command.run();

        verify(eventReporter).reportEvent(eq(Severity.WARNING), anyString(), anyString());
        verify(client).close();
    }
}
