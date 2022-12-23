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
package org.apache.nifi.remote;

import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.remote.protocol.FlowFileTransaction;
import org.apache.nifi.remote.protocol.HandshakeProperties;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHttpRemoteSiteListener {

    @BeforeAll
    public static void setup() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.remote", "DEBUG");
    }

    @Test
    public void testNormalTransactionProgress() {
        HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance(new NiFiProperties());
        String transactionId = transactionManager.createTransaction();

        assertTrue(transactionManager.isTransactionActive(transactionId),"Transaction should be active.");

        ProcessSession processSession = Mockito.mock(ProcessSession.class);
        FlowFileTransaction transaction = new FlowFileTransaction(processSession, null, null, 0, null, null);
        transactionManager.holdTransaction(transactionId, transaction, new HandshakeProperties());

        assertNotNull(transactionManager.getHandshakenProperties(transactionId));

        transaction = transactionManager.finalizeTransaction(transactionId);
        assertNotNull(transaction);

        assertFalse(transactionManager.isTransactionActive(transactionId),"Transaction should not be active anymore.");

    }

    @Test
    public void testDuplicatedTransactionId() {
        HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance(new NiFiProperties());
        String transactionId = transactionManager.createTransaction();

        assertTrue(transactionManager.isTransactionActive(transactionId),"Transaction should be active.");

        ProcessSession processSession = Mockito.mock(ProcessSession.class);
        FlowFileTransaction transaction = new FlowFileTransaction(processSession, null, null, 0, null, null);
        transactionManager.holdTransaction(transactionId, transaction, null);

        assertThrows(IllegalStateException.class,
                () -> transactionManager.holdTransaction(transactionId, transaction, null),
                "The same transaction id can't hold another transaction");
    }

    @Test
    public void testNoneExistingTransaction() {
        HttpRemoteSiteListener transactionManager = HttpRemoteSiteListener.getInstance(new NiFiProperties());

        final String transactionId = "does-not-exist-1";
        assertFalse(transactionManager.isTransactionActive(transactionId),"Transaction should not be active.");

        ProcessSession processSession = Mockito.mock(ProcessSession.class);
        FlowFileTransaction transaction = new FlowFileTransaction(processSession, null, null, 0, null, null);
        assertDoesNotThrow(() -> transactionManager.holdTransaction(transactionId, transaction, null),
                "Transaction can be held even if the transaction id is not valid anymore,"
                        + " in order to support large file or slow network.");

        assertThrows(IllegalStateException.class, () -> transactionManager.finalizeTransaction("does-not-exist-2"),
                "But transaction should not be finalized if it isn't active.");
    }
}
