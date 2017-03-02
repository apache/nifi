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

import org.apache.nifi.action.Action;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.action.AddActionsAction;
import org.apache.nifi.admin.service.action.GetActionAction;
import org.apache.nifi.admin.service.action.GetActionsAction;
import org.apache.nifi.admin.service.action.GetPreviousValues;
import org.apache.nifi.admin.service.action.PurgeActionsAction;
import org.apache.nifi.admin.service.transaction.Transaction;
import org.apache.nifi.admin.service.transaction.TransactionBuilder;
import org.apache.nifi.admin.service.transaction.TransactionException;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public class StandardAuditService implements AuditService {

    private static final Logger logger = LoggerFactory.getLogger(StandardAuditService.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    private TransactionBuilder transactionBuilder;

    @Override
    public void addActions(Collection<Action> actions) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // seed the accounts
            AddActionsAction addActions = new AddActionsAction(actions);
            transaction.execute(addActions);

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
    public Map<String, List<PreviousValue>> getPreviousValues(String componentId) {
        Transaction transaction = null;
        Map<String, List<PreviousValue>> previousValues = null;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // seed the accounts
            GetPreviousValues getActions = new GetPreviousValues(componentId);
            previousValues = transaction.execute(getActions);

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

        return previousValues;
    }

    @Override
    public History getActions(HistoryQuery query) {
        Transaction transaction = null;
        History history = null;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // seed the accounts
            GetActionsAction getActions = new GetActionsAction(query);
            history = transaction.execute(getActions);

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

        return history;
    }

    @Override
    public History getActions(int firstActionId, int maxActions) {
        final HistoryQuery query = new HistoryQuery();
        query.setOffset(firstActionId);
        query.setCount(maxActions);
        query.setSortOrder("asc");
        query.setSortColumn("timestamp");

        return getActions(query);
    }

    @Override
    public Action getAction(Integer actionId) {
        Transaction transaction = null;
        Action action = null;

        readLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // seed the accounts
            GetActionAction getAction = new GetActionAction(actionId);
            action = transaction.execute(getAction);

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

        return action;
    }

    @Override
    public void purgeActions(Date end, Action purgeAction) {
        Transaction transaction = null;

        writeLock.lock();
        try {
            // start the transaction
            transaction = transactionBuilder.start();

            // purge the action database
            PurgeActionsAction purgeActions = new PurgeActionsAction(end, purgeAction);
            transaction.execute(purgeActions);

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

    private void rollback(Transaction transaction) {
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
