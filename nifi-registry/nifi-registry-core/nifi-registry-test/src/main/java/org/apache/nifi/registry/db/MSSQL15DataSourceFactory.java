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

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.delegate.DatabaseDelegate;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;

import javax.annotation.PostConstruct;
import javax.script.ScriptException;
import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
@Profile("mssql-15")
public class MSSQL15DataSourceFactory extends TestDataSourceFactory{

    private static final MSSQLServerContainer MSSQL_CONTAINER = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2019-latest").acceptLicense();

    static {
        MSSQL_CONTAINER.start();
    }

    @Override
    protected DataSource createDataSource() {
        final SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setURL(MSSQL_CONTAINER.getJdbcUrl());
        dataSource.setUser(MSSQL_CONTAINER.getUsername());
        dataSource.setPassword(MSSQL_CONTAINER.getPassword());
        return dataSource;
    }

    @PostConstruct
    public void initDatabase() throws SQLException, ScriptException {
        DatabaseDelegate databaseDelegate = new JdbcDatabaseDelegate(MSSQL_CONTAINER, "");
        databaseDelegate.execute("DROP DATABASE test; CREATE DATABASE test;", "", 0, false, true);
    }
}
