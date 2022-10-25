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

package org.apache.nifi.toolkit.s2s;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.DataPacket;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SiteToSiteSenderTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Mock
    SiteToSiteClient siteToSiteClient;
    @Mock
    Transaction transaction;
    @Mock
    TransactionCompletion transactionCompletion;
    ByteArrayOutputStream data;
    private final Supplier<SiteToSiteSender> senderSupplier = () -> new SiteToSiteSender(siteToSiteClient, new ByteArrayInputStream(data.toByteArray()));

    @BeforeEach
    public void setup() throws IOException {
        data = new ByteArrayOutputStream();
        when(siteToSiteClient.createTransaction(TransferDirection.SEND)).thenReturn(transaction);
    }

    @Test
    public void testEmptyList() throws IOException {
        setTransactionCompletion();

        objectMapper.writeValue(data, Collections.emptyList());
        assertEquals(transactionCompletion, senderSupplier.get().sendFiles());
        verify(transaction, never()).send(any(DataPacket.class));
        verify(transaction).complete();
        verifyNoMoreInteractions(siteToSiteClient, transaction, transactionCompletion);
    }

    @Test
    public void testSingleElement() throws IOException {
        setTransactionCompletion();

        DataPacketDto dataPacketDto = new DataPacketDto("test-data".getBytes(StandardCharsets.UTF_8)).putAttribute("key", "value");
        objectMapper.writeValue(data, Arrays.stream(new DataPacketDto[]{dataPacketDto}).collect(Collectors.toList()));
        assertEquals(transactionCompletion, senderSupplier.get().sendFiles());
        verify(transaction).send(dataPacketDto.toDataPacket());
        verify(transaction).complete();
        verifyNoMoreInteractions(siteToSiteClient, transaction, transactionCompletion);
    }

    @Test
    public void testMultipleElements() throws IOException {
        setTransactionCompletion();

        DataPacketDto dataPacketDto = new DataPacketDto("test-data".getBytes(StandardCharsets.UTF_8)).putAttribute("key", "value");
        DataPacketDto dataPacketDto2 = new DataPacketDto("test-data2".getBytes(StandardCharsets.UTF_8)).putAttribute("key2", "value2");
        objectMapper.writeValue(data, Arrays.stream(new DataPacketDto[]{dataPacketDto, dataPacketDto2}).collect(Collectors.toList()));
        assertEquals(transactionCompletion, senderSupplier.get().sendFiles());
        verify(transaction).send(dataPacketDto.toDataPacket());
        verify(transaction).send(dataPacketDto2.toDataPacket());
        verify(transaction).complete();
        verifyNoMoreInteractions(siteToSiteClient, transaction, transactionCompletion);
    }

    @Test
    public void testIOException() throws IOException {
        IOException test = new IOException("test");
        DataPacketDto dataPacketDto = new DataPacketDto("test-data".getBytes(StandardCharsets.UTF_8)).putAttribute("key", "value");
        objectMapper.writeValue(data, Arrays.stream(new DataPacketDto[]{dataPacketDto}).collect(Collectors.toList()));
        doThrow(test).when(transaction).send(any(DataPacket.class));

        assertThrows(IOException.class, () -> senderSupplier.get().sendFiles());
    }

    @Test
    public void testRuntimeException() throws IOException {
        RuntimeException test = new RuntimeException("test");
        DataPacketDto dataPacketDto = new DataPacketDto("test-data".getBytes(StandardCharsets.UTF_8)).putAttribute("key", "value");
        objectMapper.writeValue(data, Arrays.stream(new DataPacketDto[]{dataPacketDto}).collect(Collectors.toList()));
        doThrow(test).when(transaction).send(any(DataPacket.class));

        assertThrows(IOException.class, () -> senderSupplier.get().sendFiles());
    }

    private void setTransactionCompletion() throws IOException {
        when(transaction.complete()).thenAnswer(invocation -> {
            verify(siteToSiteClient).createTransaction(TransferDirection.SEND);
            verify(transaction).confirm();
            return transactionCompletion;
        });
    }
}
