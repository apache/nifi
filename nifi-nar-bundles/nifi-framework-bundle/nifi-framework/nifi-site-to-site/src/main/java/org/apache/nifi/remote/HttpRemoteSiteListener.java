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

import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.remote.protocol.FlowFileTransaction;
import org.apache.nifi.remote.protocol.HandshakeProperties;
import org.apache.nifi.remote.protocol.http.HttpFlowFileServerProtocol;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nifi.util.NiFiProperties.DEFAULT_SITE_TO_SITE_HTTP_TRANSACTION_TTL;
import static org.apache.nifi.util.NiFiProperties.SITE_TO_SITE_HTTP_TRANSACTION_TTL;

public class HttpRemoteSiteListener implements RemoteSiteListener {

    private static final Logger logger = LoggerFactory.getLogger(HttpRemoteSiteListener.class);
    private final int transactionTtlSec;
    private static HttpRemoteSiteListener instance;

    private final Map<String, TransactionWrapper> transactions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService taskExecutor;
    private ProcessGroup rootGroup;
    private ScheduledFuture<?> transactionMaintenanceTask;

    private HttpRemoteSiteListener(final NiFiProperties nifiProperties) {
        super();
        taskExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName("Http Site-to-Site Transaction Maintenance");
                thread.setDaemon(true);
                return thread;
            }
        });

        int txTtlSec;
        try {
            final String snapshotFrequency = nifiProperties.getProperty(SITE_TO_SITE_HTTP_TRANSACTION_TTL, DEFAULT_SITE_TO_SITE_HTTP_TRANSACTION_TTL);
            txTtlSec = (int) FormatUtils.getTimeDuration(snapshotFrequency, TimeUnit.SECONDS);
        } catch (final Exception e) {
            txTtlSec = (int) FormatUtils.getTimeDuration(DEFAULT_SITE_TO_SITE_HTTP_TRANSACTION_TTL, TimeUnit.SECONDS);
            logger.warn("Failed to parse {} due to {}, use default as {} secs.",
                    SITE_TO_SITE_HTTP_TRANSACTION_TTL, e.getMessage(), txTtlSec);
        }
        transactionTtlSec = txTtlSec;
    }

    public static HttpRemoteSiteListener getInstance(final NiFiProperties nifiProperties) {
        if (instance == null) {
            synchronized (HttpRemoteSiteListener.class) {
                if (instance == null) {
                    instance = new HttpRemoteSiteListener(nifiProperties);
                }
            }
        }
        return instance;
    }

    private class TransactionWrapper {

        private final FlowFileTransaction transaction;
        private final HandshakeProperties handshakeProperties;
        private long lastCommunicationAt;

        private TransactionWrapper(final FlowFileTransaction transaction, final HandshakeProperties handshakeProperties) {
            this.transaction = transaction;
            this.handshakeProperties = handshakeProperties;
            this.lastCommunicationAt = System.currentTimeMillis();
        }

        private boolean isExpired() {
            long elapsedMillis = System.currentTimeMillis() - lastCommunicationAt;
            long elapsedSec = TimeUnit.SECONDS.convert(elapsedMillis, TimeUnit.MILLISECONDS);
            return elapsedSec > transactionTtlSec;
        }

        private void extend() {
            lastCommunicationAt = System.currentTimeMillis();
        }
    }

    @Override
    public void setRootGroup(ProcessGroup rootGroup) {
        this.rootGroup = rootGroup;
    }

    public void setupServerProtocol(HttpFlowFileServerProtocol serverProtocol) {
        serverProtocol.setRootProcessGroup(rootGroup);
    }

    @Override
    public void start() throws IOException {
        transactionMaintenanceTask = taskExecutor.scheduleWithFixedDelay(() -> {

            int originalSize = transactions.size();
            logger.trace("Transaction maintenance task started.");
            try {
                Set<String> transactionIds = transactions.keySet().stream().collect(Collectors.toSet());
                transactionIds.stream().filter(tid -> !isTransactionActive(tid))
                        .forEach(tid -> cancelTransaction(tid));
            } catch (Exception e) {
                // Swallow exception so that this thread can keep working.
                logger.error("An exception occurred while maintaining transactions", e);
            }
            logger.debug("Transaction maintenance task finished. originalSize={}, currentSize={}", originalSize, transactions.size());

        }, 0, transactionTtlSec / 2, TimeUnit.SECONDS);
    }

    public void cancelTransaction(String transactionId) {
        TransactionWrapper wrapper = transactions.remove(transactionId);
        if (wrapper == null) {
            logger.debug("The transaction was not found. transactionId={}", transactionId);
        } else {
            logger.debug("Cancel a transaction. transactionId={}", transactionId);
            FlowFileTransaction t = wrapper.transaction;
            if (t != null && t.getSession() != null) {
                logger.info("Cancel a transaction, rollback its session. transactionId={}", transactionId);
                try {
                    t.getSession().rollback();
                } catch (Exception e) {
                    // Swallow exception so that it can keep expiring other transactions.
                    logger.error("Failed to rollback. transactionId={}", transactionId, e);
                }
            }
        }
    }

    @Override
    public void stop() {
        if(taskExecutor != null) {
            logger.debug("Stopping Http Site-to-Site Transaction Maintenance task...");
            taskExecutor.shutdown();
        }
        if (transactionMaintenanceTask != null) {
            logger.debug("Stopping transactionMaintenanceTask...");
            transactionMaintenanceTask.cancel(true);
        }
    }

    public String createTransaction() {
        final String transactionId = UUID.randomUUID().toString();
        transactions.put(transactionId, new TransactionWrapper(null, null));
        logger.debug("Created a new transaction: {}", transactionId);
        return transactionId;
    }

    public boolean isTransactionActive(final String transactionId) {
        TransactionWrapper transaction = transactions.get(transactionId);
        return isTransactionActive(transaction);
    }

    private boolean isTransactionActive(TransactionWrapper transaction) {
        if (transaction == null) {
            return false;
        }
        if (transaction.isExpired()) {
            return false;
        }
        return true;
    }

    /**
     * @param transactionId transactionId to check
     * @return Returns a HandshakeProperties instance which is created when this
     * transaction is started, only if the transaction is active, and it holds a
     * HandshakeProperties, otherwise return null
     */
    public HandshakeProperties getHandshakenProperties(final String transactionId) {
        TransactionWrapper transaction = transactions.get(transactionId);
        if (isTransactionActive(transaction)) {
            return transaction.handshakeProperties;
        }
        return null;
    }

    public void holdTransaction(final String transactionId, final FlowFileTransaction transaction,
            final HandshakeProperties handshakenProperties) throws IllegalStateException {
        // We don't check expiration of the transaction here, to support large file transport or slow network.
        // The availability of current transaction is already checked when the HTTP request was received at SiteToSiteResource.
        TransactionWrapper currentTransaction = transactions.remove(transactionId);
        if (currentTransaction == null) {
            logger.debug("The transaction was not found, it looks it took longer than transaction TTL.");
        } else if (currentTransaction.transaction != null) {
            throw new IllegalStateException("Transaction has already been processed. It can only be finalized. transactionId=" + transactionId);
        }
        if (transaction.getSession() == null) {
            throw new IllegalStateException("Passed transaction is not associated any session yet, can not hold. transactionId=" + transactionId);
        }
        logger.debug("Holding a transaction: {}", transactionId);
        // Server has received or sent all data, and transaction TTL count down starts here.
        // However, if the client doesn't consume data fast enough, server might expire and rollback the transaction.
        transactions.put(transactionId, new TransactionWrapper(transaction, handshakenProperties));
    }

    public FlowFileTransaction finalizeTransaction(final String transactionId) throws IllegalStateException {
        if (!isTransactionActive(transactionId)) {
            throw new IllegalStateException("Transaction was not found or not active anymore. transactionId=" + transactionId);
        }
        TransactionWrapper transaction = transactions.remove(transactionId);
        if (transaction == null) {
            throw new IllegalStateException("Transaction was not found anymore. It's already finalized or expired. transactionId=" + transactionId);
        }
        if (transaction.transaction == null) {
            throw new IllegalStateException("Transaction has not started yet.");
        }
        logger.debug("Finalized a transaction: {}", transactionId);
        return transaction.transaction;
    }

    public void extendTransaction(final String transactionId) throws IllegalStateException {
        if (!isTransactionActive(transactionId)) {
            throw new IllegalStateException("Transaction was not found or not active anymore. transactionId=" + transactionId);
        }
        TransactionWrapper transaction = transactions.get(transactionId);
        if (transaction != null) {
            logger.debug("Extending transaction TTL, transactionId={}", transactionId);
            transaction.extend();
        }
    }

    public int getTransactionTtlSec() {
        return transactionTtlSec;
    }

}
