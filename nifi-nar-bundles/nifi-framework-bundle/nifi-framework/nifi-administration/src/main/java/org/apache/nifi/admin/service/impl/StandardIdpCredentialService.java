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
import org.apache.nifi.admin.service.IdpCredentialService;
import org.apache.nifi.admin.service.action.CreateIdpCredentialAction;
import org.apache.nifi.admin.service.action.DeleteIdpCredentialByIdAction;
import org.apache.nifi.admin.service.action.DeleteIdpCredentialByIdentityAction;
import org.apache.nifi.admin.service.action.GetIdpCredentialByIdentity;
import org.apache.nifi.admin.service.transaction.Transaction;
import org.apache.nifi.admin.service.transaction.TransactionBuilder;
import org.apache.nifi.admin.service.transaction.TransactionException;
import org.apache.nifi.idp.IdpCredential;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Database implementation of IdpCredentialService.
 */
public class StandardIdpCredentialService implements IdpCredentialService {

    private static Logger LOGGER = LoggerFactory.getLogger(StandardIdpCredentialService.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    private TransactionBuilder transactionBuilder;

    @Override
    public IdpCredential createCredential(final IdpCredential credential) {
        Transaction transaction = null;
        IdpCredential createdCredential;

        writeLock.lock();
        try {
            // ensure the created date is set
            if (credential.getCreated() == null) {
                credential.setCreated(new Date());
            }

            // start the transaction
            transaction = transactionBuilder.start();

            // create the credential
            final CreateIdpCredentialAction action = new CreateIdpCredentialAction(credential);
            createdCredential = transaction.execute(action);

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

        return createdCredential;
    }

    @Override
    public IdpCredential getCredential(final String identity) {
        Transaction transaction = null;
        IdpCredential credential;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // get the credential
            final GetIdpCredentialByIdentity action = new GetIdpCredentialByIdentity(identity);
            credential = transaction.execute(action);

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

        return credential;
    }

    @Override
    public void deleteCredential(final int id) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // delete the credential
            final DeleteIdpCredentialByIdAction action = new DeleteIdpCredentialByIdAction(id);
            Integer rowsDeleted = transaction.execute(action);
            if (rowsDeleted == 0) {
                LOGGER.warn("No IDP credential was found to delete for id " + id);
            }

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

    @Override
    public IdpCredential replaceCredential(final IdpCredential credential) {
        final String identity = credential.getIdentity();
        if (StringUtils.isBlank(identity)) {
            throw new IllegalArgumentException("Identity is required");
        }

        Transaction transaction = null;
        IdpCredential createdCredential;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // delete the credential
            final DeleteIdpCredentialByIdentityAction deleteAction = new DeleteIdpCredentialByIdentityAction(identity);
            Integer rowsDeleted = transaction.execute(deleteAction);
            if (rowsDeleted == 0) {
                LOGGER.debug("No IDP credential was found to delete for id " + identity);
            }

            // ensure the created date is set for the new credential
            if (credential.getCreated() == null) {
                credential.setCreated(new Date());
            }

            // create the new credential
            final CreateIdpCredentialAction createAction = new CreateIdpCredentialAction(credential);
            createdCredential = transaction.execute(createAction);

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

        return createdCredential;
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

}
