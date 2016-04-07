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
package org.apache.nifi.admin.service.transaction.impl;

import org.apache.nifi.admin.RepositoryUtils;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.impl.DAOFactoryImpl;
import org.apache.nifi.admin.service.action.AdministrationAction;
import org.apache.nifi.admin.service.transaction.Transaction;
import org.apache.nifi.admin.service.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Transaction implementation that uses the specified SQL Connection and
 * AuthorityProvider.
 */
public class StandardTransaction implements Transaction {

    private static final Logger logger = LoggerFactory.getLogger(StandardTransaction.class);

    private Connection connection;

    public StandardTransaction(Connection connection) {
        this.connection = connection;
    }

    @Override
    public <T> T execute(AdministrationAction<T> action) {
        // ensure the transaction has been started
        if (connection == null) {
            throw new IllegalStateException("This transaction is not active.");
        }

        // create a dao factory
        DAOFactory daoFactory = new DAOFactoryImpl(connection);

        // execute the specified action
        return action.execute(daoFactory);
    }

    @Override
    public void commit() throws TransactionException {
        // ensure there is an active transaction
        if (connection == null) {
            throw new IllegalStateException("No active transaction.");
        }

        try {
            // commit the transaction
            connection.commit();
        } catch (SQLException sqle) {
            throw new TransactionException(sqle.getMessage());
        }
    }

    @Override
    public void rollback() {
        // ensure there is an active transaction
        if (connection != null) {
            // rollback the transaction
            RepositoryUtils.rollback(connection, logger);
        }
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            RepositoryUtils.closeQuietly(connection);
            connection = null;
        }
    }
}
