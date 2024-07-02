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
package org.apache.nifi.questdb.embedded;

import org.apache.nifi.questdb.Client;
import org.apache.nifi.questdb.DatabaseException;
import org.apache.nifi.questdb.InsertRowDataSource;
import org.apache.nifi.questdb.QueryResultProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

final class LockedClient implements Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(LockedClient.class);
    private final Lock lock;

    private final Duration lockAttemptDuration;

    private final Client client;

    LockedClient(final Lock lock, final Duration lockAttemptDuration, final Client client) {
        this.lock = lock;
        this.lockAttemptDuration = lockAttemptDuration;
        this.client = client;
    }

    @Override
    public void execute(final String query) throws DatabaseException {
        lockedOperation(() -> {
            client.execute(query);
            return null;
        });
    }

    @Override
    public void insert(final String tableName, final InsertRowDataSource rowSource) throws DatabaseException {
        lockedOperation(() -> {
            client.insert(tableName, rowSource);
            return null;
        });
    }

    @Override
    public <T> T query(final String query, final QueryResultProcessor<T> rowProcessor) throws DatabaseException {
        return lockedOperation(() -> client.query(query, rowProcessor));
    }

    @Override
    public void disconnect() throws DatabaseException {
        client.disconnect();
    }

    private <R> R lockedOperation(final Callable<R> operation) throws DatabaseException {
        LOGGER.debug("Start locking client {}", client);
        try {
            if (!lock.tryLock(lockAttemptDuration.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new LockUnsuccessfulException("Could not lock read lock on the database");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LockUnsuccessfulException(e);
        }

        try {
            LOGGER.debug("Successfully locked client {}", client);
            return operation.call();
        } catch (final DatabaseException e) {
            LOGGER.error("Locked operation was unsuccessful", e);
            throw e;
        } catch (final Exception e) {
            LOGGER.error("Locked operation was unsuccessful", e);
            throw new DatabaseException(e);
        } finally {
            lock.unlock();
            LOGGER.debug("Unlocked client {}", client);
        }
    }

    @Override
    public String toString() {
        return "LockedQuestDbClient{" +
                "lock=" + lock +
                ", lockAttemptTime=" + lockAttemptDuration +
                ", client=" + client +
                '}';
    }
}
