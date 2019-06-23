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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonInclude.Value;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SiteToSiteReceiverTest {
    private final ObjectMapper objectMapper = new ObjectMapper().setDefaultPropertyInclusion(Value.construct(Include.NON_NULL, Include.ALWAYS));;
    @Mock
    SiteToSiteClient siteToSiteClient;
    @Mock
    Transaction transaction;
    @Mock
    TransactionCompletion transactionCompletion;
    ByteArrayOutputStream data;
    private final Supplier<SiteToSiteReceiver> receiverSupplier = () -> new SiteToSiteReceiver(siteToSiteClient, data);
    ByteArrayOutputStream expectedData;

    @Before
    public void setup() throws IOException {
        data = new ByteArrayOutputStream();
        expectedData = new ByteArrayOutputStream();
        when(siteToSiteClient.createTransaction(TransferDirection.RECEIVE)).thenReturn(transaction);
        when(transaction.complete()).thenAnswer(invocation -> {
            verify(siteToSiteClient).createTransaction(TransferDirection.RECEIVE);
            verify(transaction).confirm();
            return transactionCompletion;
        });
    }

    @Test
    public void testEmpty() throws IOException {
        assertEquals(transactionCompletion, receiverSupplier.get().receiveFiles());

        objectMapper.writeValue(expectedData, Collections.emptyList());
        assertEquals(expectedData.toString(), data.toString());
    }

    @Test
    public void testSingle() throws IOException {
        DataPacketDto dataPacketDto = new DataPacketDto("test-data".getBytes(StandardCharsets.UTF_8)).putAttribute("key", "value");
        when(transaction.receive()).thenReturn(dataPacketDto.toDataPacket()).thenReturn(null);

        assertEquals(transactionCompletion, receiverSupplier.get().receiveFiles());

        objectMapper.writeValue(expectedData, Arrays.asList(dataPacketDto));
        assertEquals(expectedData.toString(), data.toString());
    }

    @Test
    public void testMulti() throws IOException {
        DataPacketDto dataPacketDto = new DataPacketDto("test-data".getBytes(StandardCharsets.UTF_8)).putAttribute("key", "value");
        DataPacketDto dataPacketDto2 = new DataPacketDto("test-data2".getBytes(StandardCharsets.UTF_8)).putAttribute("key2", "value2");
        when(transaction.receive()).thenReturn(dataPacketDto.toDataPacket()).thenReturn(dataPacketDto2.toDataPacket()).thenReturn(null);

        assertEquals(transactionCompletion, receiverSupplier.get().receiveFiles());

        objectMapper.writeValue(expectedData, Arrays.asList(dataPacketDto, dataPacketDto2));
        assertEquals(expectedData.toString(), data.toString());
    }
}
