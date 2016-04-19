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

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.nifi.admin.service.transaction.Transaction;
import org.apache.nifi.admin.service.transaction.TransactionBuilder;
import org.apache.nifi.admin.service.transaction.TransactionException;

/**
 *
 */
public class StandardTransactionBuilder implements TransactionBuilder {

    private DataSource dataSource;

    @Override
    public Transaction start() throws TransactionException {
        try {
            // get a new connection
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            // create a new transaction
            return new StandardTransaction(connection);
        } catch (SQLException sqle) {
            throw new TransactionException(sqle.getMessage());
        }
    }

    /* setters */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
