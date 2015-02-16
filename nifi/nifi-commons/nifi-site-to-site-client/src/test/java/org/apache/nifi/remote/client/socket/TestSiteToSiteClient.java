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
package org.apache.nifi.remote.client.socket;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.StreamUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestSiteToSiteClient {

    @Test
    //@Ignore("For local testing only; not really a unit test but a manual test")
    public void testReceive() throws IOException {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");
        
        final SiteToSiteClient client = new SiteToSiteClient.Builder()
            .url("http://localhost:8080/nifi")
            .portName("cba")
            .requestBatchCount(1)
            .build();
        
        try {
            final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
            Assert.assertNotNull(transaction);
            
            final DataPacket packet = transaction.receive();
            Assert.assertNotNull(packet);
            
            final InputStream in = packet.getData();
            final long size = packet.getSize();
            final byte[] buff = new byte[(int) size];
            
            StreamUtils.fillBuffer(in, buff);
            
            Assert.assertNull(transaction.receive());
            
            transaction.confirm();
            transaction.complete();
        } finally {
            client.close();
        }
    }
    
    
    @Test
    //@Ignore("For local testing only; not really a unit test but a manual test")
    public void testSend() throws IOException {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");
        
        final SiteToSiteClient client = new SiteToSiteClient.Builder()
            .url("http://localhost:8080/nifi")
            .portName("input")
            .build();
    
        try {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);
            Assert.assertNotNull(transaction);
            
            final Map<String, String> attrs = new HashMap<>();
            attrs.put("site-to-site", "yes, please!");
            final byte[] bytes = "Hello".getBytes();
            final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            final DataPacket packet = new StandardDataPacket(attrs, bais, bytes.length);
            transaction.send(packet);
            
            transaction.confirm();
            transaction.complete();
        } finally {
            client.close();
        }
    }
    
}
