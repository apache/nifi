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
package org.apache.nifi.admin.service.impl;

import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.KeyService;
import org.apache.nifi.admin.service.action.DeleteKeysAction;
import org.apache.nifi.admin.service.action.GetKeyByIdAction;
import org.apache.nifi.admin.service.action.GetOrCreateKeyAction;
import org.apache.nifi.admin.service.transaction.Transaction;
import org.apache.nifi.admin.service.transaction.TransactionBuilder;
import org.apache.nifi.admin.service.transaction.TransactionException;
import org.apache.nifi.key.Key;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public class StandardKeyService implements KeyService {

    private static final Logger logger = LoggerFactory.getLogger(StandardKeyService.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    private TransactionBuilder transactionBuilder;
    private NiFiProperties properties;

    @Override
    public Key getKey(int id) {
        Transaction transaction = null;
        Key key = null;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // get the key
            GetKeyByIdAction addActions = new GetKeyByIdAction(id);
            key = transaction.execute(addActions);

            // commit the transaction
            transaction.commit();
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            readLock.unlock();
        }

        return key;
    }

    @Override
    public Key getOrCreateKey(String identity) {
        Transaction transaction = null;
        Key key = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // get or create a key
            GetOrCreateKeyAction addActions = new GetOrCreateKeyAction(identity);
            key = transaction.execute(addActions);

            // commit the transaction
            transaction.commit();
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }

        return key;
    }

    @Override
    public void deleteKey(String identity) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // delete the keys
            DeleteKeysAction deleteKeys = new DeleteKeysAction(identity);
            transaction.execute(deleteKeys);

            // commit the transaction
            transaction.commit();
        } catch (TransactionException | DataAccessException te) {
            rollback(transaction);
            throw new AdministrationException(te);
        } catch (Throwable t) {
            rollback(transaction);
            throw t;
        } finally {
            closeQuietly(transaction);
            writeLock.unlock();
        }
    }

    private void rollback(final Transaction transaction) {
        if (transaction != null) {
            transaction.rollback();
        }
    }

    private void closeQuietly(final Transaction transaction) {
        if (transaction != null) {
            try {
                transaction.close();
            } catch (final IOException ioe) {
            }
        }
    }

    public void setTransactionBuilder(TransactionBuilder transactionBuilder) {
        this.transactionBuilder = transactionBuilder;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
