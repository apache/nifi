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
package org.apache.nifi.remote.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.StreamUtils;

public class SiteToSiteTestUtils {
    public static DataPacket createDataPacket(String contents) {
        try {
            byte[] bytes = contents.getBytes("UTF-8");
            ByteArrayInputStream is = new ByteArrayInputStream(bytes);
            return new StandardDataPacket(new HashMap<>(), is, bytes.length);
        } catch (UnsupportedEncodingException e){
            throw new RuntimeException(e);
        }
    }

    public static String readContents(DataPacket packet) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream((int) packet.getSize());
        StreamUtils.copy(packet.getData(), os);
        return new String(os.toByteArray(), "UTF-8");
    }

    public static void execReceiveZeroFlowFile(Transaction transaction) throws IOException {
        assertEquals(Transaction.TransactionState.TRANSACTION_STARTED, transaction.getState());

        DataPacket packet = transaction.receive();
        assertNull(packet);

        transaction.confirm();
        assertEquals(Transaction.TransactionState.TRANSACTION_CONFIRMED, transaction.getState());

        TransactionCompletion completion = transaction.complete();
        assertEquals(Transaction.TransactionState.TRANSACTION_COMPLETED, transaction.getState());
        assertFalse("Should NOT be backoff", completion.isBackoff());
        assertEquals(0, completion.getDataPacketsTransferred());
    }

    public static void execReceiveOneFlowFile(Transaction transaction) throws IOException {
        assertEquals(Transaction.TransactionState.TRANSACTION_STARTED, transaction.getState());

        DataPacket packet = transaction.receive();
        assertNotNull(packet);
        assertEquals("contents on server 1", readContents(packet));
        assertEquals(Transaction.TransactionState.DATA_EXCHANGED, transaction.getState());

        packet = transaction.receive();
        assertNull(packet);

        transaction.confirm();
        assertEquals(Transaction.TransactionState.TRANSACTION_CONFIRMED, transaction.getState());

        TransactionCompletion completion = transaction.complete();
        assertEquals(Transaction.TransactionState.TRANSACTION_COMPLETED, transaction.getState());
        assertFalse("Should NOT be backoff", completion.isBackoff());
        assertEquals(1, completion.getDataPacketsTransferred());
    }

    public static void execReceiveTwoFlowFiles(Transaction transaction) throws IOException {
        DataPacket packet = transaction.receive();
        assertNotNull(packet);
        assertEquals("contents on server 1", readContents(packet));
        assertEquals(Transaction.TransactionState.DATA_EXCHANGED, transaction.getState());

        packet = transaction.receive();
        assertNotNull(packet);
        assertEquals("contents on server 2", readContents(packet));
        assertEquals(Transaction.TransactionState.DATA_EXCHANGED, transaction.getState());

        packet = transaction.receive();
        assertNull(packet);

        transaction.confirm();
        assertEquals(Transaction.TransactionState.TRANSACTION_CONFIRMED, transaction.getState());

        TransactionCompletion completion = transaction.complete();
        assertEquals(Transaction.TransactionState.TRANSACTION_COMPLETED, transaction.getState());
        assertFalse("Should NOT be backoff", completion.isBackoff());
        assertEquals(2, completion.getDataPacketsTransferred());
    }

    public static void execReceiveWithInvalidChecksum(Transaction transaction) throws IOException {
        assertEquals(Transaction.TransactionState.TRANSACTION_STARTED, transaction.getState());

        DataPacket packet = transaction.receive();
        assertNotNull(packet);
        assertEquals("contents on server 1", readContents(packet));
        assertEquals(Transaction.TransactionState.DATA_EXCHANGED, transaction.getState());

        packet = transaction.receive();
        assertNotNull(packet);
        assertEquals("contents on server 2", readContents(packet));
        assertEquals(Transaction.TransactionState.DATA_EXCHANGED, transaction.getState());

        packet = transaction.receive();
        assertNull(packet);

        try {
            transaction.confirm();
            fail();
        } catch (IOException e){
            assertTrue(e.getMessage().contains("Received a BadChecksum response"));
            assertEquals(Transaction.TransactionState.ERROR, transaction.getState());
        }

        try {
            transaction.complete();
            fail("It's not confirmed.");
        } catch (IllegalStateException e){
            assertEquals(Transaction.TransactionState.ERROR, transaction.getState());
        }
    }

    public static void execSendZeroFlowFile(Transaction transaction) throws IOException {
        assertEquals(Transaction.TransactionState.TRANSACTION_STARTED, transaction.getState());

        try {
            transaction.confirm();
            fail("Nothing has been sent.");
        } catch (IllegalStateException e){
        }

        try {
            transaction.complete();
            fail("Nothing has been sent.");
        } catch (IllegalStateException e){
        }
    }

    public static void execSendOneFlowFile(Transaction transaction) throws IOException {
        assertEquals(Transaction.TransactionState.TRANSACTION_STARTED, transaction.getState());

        DataPacket packet = createDataPacket("contents on client 1");
        transaction.send(packet);

        transaction.confirm();
        assertEquals(Transaction.TransactionState.TRANSACTION_CONFIRMED, transaction.getState());

        TransactionCompletion completion = transaction.complete();
        assertEquals(Transaction.TransactionState.TRANSACTION_COMPLETED, transaction.getState());
        assertFalse("Should NOT be backoff", completion.isBackoff());
        assertEquals(1, completion.getDataPacketsTransferred());
    }

    public static void execSendTwoFlowFiles(Transaction transaction) throws IOException {
        assertEquals(Transaction.TransactionState.TRANSACTION_STARTED, transaction.getState());

        DataPacket packet = createDataPacket("contents on client 1");
        transaction.send(packet);

        packet = createDataPacket("contents on client 2");
        transaction.send(packet);

        transaction.confirm();
        assertEquals(Transaction.TransactionState.TRANSACTION_CONFIRMED, transaction.getState());

        TransactionCompletion completion = transaction.complete();
        assertEquals(Transaction.TransactionState.TRANSACTION_COMPLETED, transaction.getState());
        assertFalse("Should NOT be backoff", completion.isBackoff());
        assertEquals(2, completion.getDataPacketsTransferred());
    }

    public static void execSendWithInvalidChecksum(Transaction transaction) throws IOException {
        assertEquals(Transaction.TransactionState.TRANSACTION_STARTED, transaction.getState());

        DataPacket packet = createDataPacket("contents on client 1");
        transaction.send(packet);

        packet = createDataPacket("contents on client 2");
        transaction.send(packet);


        try {
            transaction.confirm();
            fail();
        } catch (IOException e){
            assertTrue(e.getMessage().contains("peer calculated CRC32 Checksum as Different checksum"));
            assertEquals(Transaction.TransactionState.ERROR, transaction.getState());
        }

        try {
            transaction.complete();
            fail("It's not confirmed.");
        } catch (IllegalStateException e){
            assertEquals(Transaction.TransactionState.ERROR, transaction.getState());
        }
    }

    public static void execSendButDestinationFull(Transaction transaction) throws IOException {
        assertEquals(Transaction.TransactionState.TRANSACTION_STARTED, transaction.getState());

        DataPacket packet = createDataPacket("contents on client 1");
        transaction.send(packet);

        packet = createDataPacket("contents on client 2");
        transaction.send(packet);

        transaction.confirm();
        assertEquals(Transaction.TransactionState.TRANSACTION_CONFIRMED, transaction.getState());

        TransactionCompletion completion = transaction.complete();
        assertEquals(Transaction.TransactionState.TRANSACTION_COMPLETED, transaction.getState());
        assertTrue("Should be backoff", completion.isBackoff());
        assertEquals(2, completion.getDataPacketsTransferred());
    }

}
