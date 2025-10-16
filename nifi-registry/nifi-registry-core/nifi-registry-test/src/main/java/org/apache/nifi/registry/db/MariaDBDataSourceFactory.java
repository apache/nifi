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

import jakarta.annotation.PostConstruct;
import org.mariadb.jdbc.MariaDbDataSource;
import org.testcontainers.delegate.DatabaseDelegate;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;
import org.testcontainers.mariadb.MariaDBContainer;

import javax.sql.DataSource;

import java.sql.SQLException;

public abstract class MariaDBDataSourceFactory extends TestDataSourceFactory {

    protected abstract MariaDBContainer mariaDBContainer();

    @Override
    protected DataSource createDataSource() {
        try {
            final MariaDBContainer container = mariaDBContainer();
            final MariaDbDataSource dataSource = new MariaDbDataSource();
            dataSource.setUrl(container.getJdbcUrl());
            dataSource.setUser(container.getUsername());
            dataSource.setPassword(container.getPassword());
            return dataSource;
        } catch (SQLException e) {
            throw new RuntimeException("Unable to create MariaDB DataSource", e);
        }
    }

    @PostConstruct
    public void initDatabase() {
        DatabaseDelegate databaseDelegate = new JdbcDatabaseDelegate(mariaDBContainer(), "");
        databaseDelegate.execute("DROP DATABASE test; CREATE DATABASE test;", "", 0, false, true);
    }
}
