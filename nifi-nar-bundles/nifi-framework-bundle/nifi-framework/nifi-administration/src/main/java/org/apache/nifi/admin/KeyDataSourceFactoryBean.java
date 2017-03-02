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
package org.apache.nifi.admin;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.NiFiProperties;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class KeyDataSourceFactoryBean implements FactoryBean {

    private static final Logger logger = LoggerFactory.getLogger(KeyDataSourceFactoryBean.class);
    private static final String NF_USERNAME_PASSWORD = "nf";
    private static final int MAX_CONNECTIONS = 5;

    // database file name
    private static final String USER_KEYS_DATABASE_FILE_NAME = "nifi-user-keys";

    // ----------
    // keys table
    // ----------

    private static final String CREATE_KEY_TABLE = "CREATE TABLE KEY ("
            + "ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT, "
            + "IDENTITY VARCHAR2(4096) NOT NULL UNIQUE, "
            + "KEY VARCHAR2(100) NOT NULL"
            + ")";

    private JdbcConnectionPool connectionPool;

    private NiFiProperties properties;

    @Override
    public Object getObject() throws Exception {
        if (connectionPool == null) {

            // locate the repository directory
            String repositoryDirectoryPath = properties.getProperty(NiFiProperties.REPOSITORY_DATABASE_DIRECTORY);

            // ensure the repository directory is specified
            if (repositoryDirectoryPath == null) {
                throw new NullPointerException("Database directory must be specified.");
            }

            // create a handle to the repository directory
            File repositoryDirectory = new File(repositoryDirectoryPath);

            // create a handle to the database directory and file
            File databaseFile = new File(repositoryDirectory, USER_KEYS_DATABASE_FILE_NAME);
            String databaseUrl = getDatabaseUrl(databaseFile);

            // create the pool
            connectionPool = JdbcConnectionPool.create(databaseUrl, NF_USERNAME_PASSWORD, NF_USERNAME_PASSWORD);
            connectionPool.setMaxConnections(MAX_CONNECTIONS);

            Connection connection = null;
            ResultSet rs = null;
            Statement statement = null;
            try {
                // get a connection
                connection = connectionPool.getConnection();
                connection.setAutoCommit(false);

                // create a statement for creating/updating the database
                statement = connection.createStatement();

                // determine if the key table need to be created
                rs = connection.getMetaData().getTables(null, null, "KEY", null);
                if (!rs.next()) {
                    statement.execute(CREATE_KEY_TABLE);
                }

                // commit any changes
                connection.commit();
            } catch (SQLException sqle) {
                RepositoryUtils.rollback(connection, logger);
                throw sqle;
            } finally {
                RepositoryUtils.closeQuietly(rs);
                RepositoryUtils.closeQuietly(statement);
                RepositoryUtils.closeQuietly(connection);
            }
        }

        return connectionPool;
    }

    private String getDatabaseUrl(File databaseFile) {
        String databaseUrl = "jdbc:h2:" + databaseFile + ";AUTOCOMMIT=OFF;DB_CLOSE_ON_EXIT=FALSE;LOCK_MODE=3";
        String databaseUrlAppend = properties.getProperty(NiFiProperties.H2_URL_APPEND);
        if (StringUtils.isNotBlank(databaseUrlAppend)) {
            databaseUrl += databaseUrlAppend;
        }
        return databaseUrl;
    }

    @Override
    public Class getObjectType() {
        return JdbcConnectionPool.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void shutdown() {
        // shutdown the connection pool
        if (connectionPool != null) {
            try {
                connectionPool.dispose();
            } catch (Exception e) {
                logger.warn("Unable to dispose of connection pool: " + e.getMessage());
                if (logger.isDebugEnabled()) {
                    logger.warn(StringUtils.EMPTY, e);
                }
            }
        }
    }

}
