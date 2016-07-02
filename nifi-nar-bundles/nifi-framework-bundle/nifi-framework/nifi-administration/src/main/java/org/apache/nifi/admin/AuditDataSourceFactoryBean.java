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

/**
 *
 */
public class AuditDataSourceFactoryBean implements FactoryBean {

    private static final Logger logger = LoggerFactory.getLogger(AuditDataSourceFactoryBean.class);
    private static final String NF_USERNAME_PASSWORD = "nf";
    private static final int MAX_CONNECTIONS = 5;

    // database file name
    private static final String AUDIT_DATABASE_FILE_NAME = "nifi-flow-audit";

    // ------------
    // action table
    // ------------
    private static final String CREATE_ACTION_TABLE = "CREATE TABLE ACTION ("
            + "ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT, "
            + "IDENTITY VARCHAR2(4096) NOT NULL, "
            + "SOURCE_ID VARCHAR2(100) NOT NULL, "
            + "SOURCE_NAME VARCHAR2(1000) NOT NULL, "
            + "SOURCE_TYPE VARCHAR2(1000) NOT NULL, "
            + "OPERATION VARCHAR2(50) NOT NULL, "
            + "ACTION_TIMESTAMP TIMESTAMP NOT NULL "
            + ")";

    // -----------------
    // component details
    // -----------------
    private static final String CREATE_PROCESSOR_DETAILS_TABLE = "CREATE TABLE PROCESSOR_DETAILS ("
            + "ACTION_ID INT NOT NULL PRIMARY KEY, "
            + "TYPE VARCHAR2(1000) NOT NULL, "
            + "FOREIGN KEY (ACTION_ID) REFERENCES ACTION(ID)"
            + ")";

    private static final String CREATE_REMOTE_PROCESS_GROUP_DETAILS_TABLE = "CREATE TABLE REMOTE_PROCESS_GROUP_DETAILS ("
            + "ACTION_ID INT NOT NULL PRIMARY KEY, "
            + "URI VARCHAR2(2500) NOT NULL, "
            + "FOREIGN KEY (ACTION_ID) REFERENCES ACTION(ID)"
            + ")";

    // --------------
    // action details
    // --------------
    private static final String CREATE_MOVE_DETAILS_TABLE = "CREATE TABLE MOVE_DETAILS ("
            + "ACTION_ID INT NOT NULL PRIMARY KEY, "
            + "GROUP_ID VARCHAR2(100) NOT NULL, "
            + "GROUP_NAME VARCHAR2(1000) NOT NULL, "
            + "PREVIOUS_GROUP_ID VARCHAR2(100) NOT NULL, "
            + "PREVIOUS_GROUP_NAME VARCHAR2(1000) NOT NULL, "
            + "FOREIGN KEY (ACTION_ID) REFERENCES ACTION(ID)"
            + ")";

    private static final String CREATE_CONFIGURE_DETAILS_TABLE = "CREATE TABLE CONFIGURE_DETAILS ("
            + "ACTION_ID INT NOT NULL PRIMARY KEY, "
            + "NAME VARCHAR2(1000) NOT NULL, "
            + "VALUE VARCHAR2(5000), "
            + "PREVIOUS_VALUE VARCHAR2(5000), "
            + "FOREIGN KEY (ACTION_ID) REFERENCES ACTION(ID)"
            + ")";

    private static final String CREATE_CONNECT_DETAILS_TABLE = "CREATE TABLE CONNECT_DETAILS ("
            + "ACTION_ID INT NOT NULL PRIMARY KEY, "
            + "SOURCE_ID VARCHAR2(100) NOT NULL, "
            + "SOURCE_NAME VARCHAR2(1000), "
            + "SOURCE_TYPE VARCHAR2(1000) NOT NULL, "
            + "RELATIONSHIP VARCHAR2(1000), "
            + "DESTINATION_ID VARCHAR2(100) NOT NULL, "
            + "DESTINATION_NAME VARCHAR2(1000), "
            + "DESTINATION_TYPE VARCHAR2(1000) NOT NULL, "
            + "FOREIGN KEY (ACTION_ID) REFERENCES ACTION(ID)"
            + ")";

    private static final String CREATE_PURGE_DETAILS_TABLE = "CREATE TABLE PURGE_DETAILS ("
            + "ACTION_ID INT NOT NULL PRIMARY KEY, "
            + "END_DATE TIMESTAMP NOT NULL, "
            + "FOREIGN KEY (ACTION_ID) REFERENCES ACTION(ID)"
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

            // get a handle to the database file
            File databaseFile = new File(repositoryDirectory, AUDIT_DATABASE_FILE_NAME);

            // format the database url
            String databaseUrl = "jdbc:h2:" + databaseFile + ";AUTOCOMMIT=OFF;DB_CLOSE_ON_EXIT=FALSE;LOCK_MODE=3";
            String databaseUrlAppend = properties.getProperty(NiFiProperties.H2_URL_APPEND);
            if (StringUtils.isNotBlank(databaseUrlAppend)) {
                databaseUrl += databaseUrlAppend;
            }

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

                // create a statement for initializing the database
                statement = connection.createStatement();

                // determine if the tables need to be created
                rs = connection.getMetaData().getTables(null, null, "ACTION", null);
                if (!rs.next()) {
                    logger.info("Database not built for repository: " + databaseUrl + ".  Building now...");
                    RepositoryUtils.closeQuietly(rs);

                    // action table
                    statement.execute(CREATE_ACTION_TABLE);

                    // component details
                    statement.execute(CREATE_PROCESSOR_DETAILS_TABLE);
                    statement.execute(CREATE_REMOTE_PROCESS_GROUP_DETAILS_TABLE);

                    // action details
                    statement.execute(CREATE_MOVE_DETAILS_TABLE);
                    statement.execute(CREATE_CONFIGURE_DETAILS_TABLE);
                    statement.execute(CREATE_CONNECT_DETAILS_TABLE);
                    statement.execute(CREATE_PURGE_DETAILS_TABLE);
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

    /**
     * Disposes resources.
     */
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
