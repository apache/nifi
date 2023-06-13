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
package org.apache.nifi.registry.db;

import java.sql.SQLException;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import oracle.jdbc.datasource.OracleDataSource;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.delegate.DatabaseDelegate;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;

public abstract class OracleDbDataSourceFactory extends TestDataSourceFactory {

    protected abstract OracleContainer oracleContainer();

    @Override
    protected DataSource createDataSource() {
        final OracleContainer oracleContainer = oracleContainer();
        try {
            final OracleDataSource dataSource = new oracle.jdbc.pool.OracleDataSource();
            dataSource.setURL(oracleContainer.getJdbcUrl());
            dataSource.setUser(oracleContainer.getUsername());
            dataSource.setPassword(oracleContainer.getPassword());
            dataSource.setDatabaseName(oracleContainer.getDatabaseName());
            return dataSource;
        } catch (SQLException e) {
            throw new RuntimeException("Unable to create Oracle data source", e);
        }
    }

    @PostConstruct
    public void initDatabase() {
        final DatabaseDelegate databaseDelegate = new JdbcDatabaseDelegate(oracleContainer(), "");
        databaseDelegate.execute("DROP DATABASE test; CREATE DATABASE test;", "", 0, false, true);
    }
}
