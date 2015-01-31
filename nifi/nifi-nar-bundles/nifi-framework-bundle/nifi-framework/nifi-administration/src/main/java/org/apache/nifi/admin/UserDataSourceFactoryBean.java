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

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.h2.jdbcx.JdbcConnectionPool;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

/**
 *
 */
public class UserDataSourceFactoryBean implements FactoryBean {

    private static final Logger logger = LoggerFactory.getLogger(UserDataSourceFactoryBean.class);
    private static final String NF_USERNAME_PASSWORD = "nf";
    private static final int MAX_CONNECTIONS = 5;

    // database file name
    private static final String AUDIT_DATABASE_FILE_NAME = "nifi-users";

    private static final String CREATE_USER_TABLE = "CREATE TABLE USER ("
            + "ID VARCHAR2(100) NOT NULL PRIMARY KEY, "
            + "DN VARCHAR2(255) NOT NULL UNIQUE, "
            + "USER_NAME VARCHAR2(100) NOT NULL, "
            + "USER_GROUP VARCHAR2(100), "
            + "CREATION TIMESTAMP NOT NULL, "
            + "LAST_ACCESSED TIMESTAMP, "
            + "LAST_VERIFIED TIMESTAMP, "
            + "JUSTIFICATION VARCHAR2(500) NOT NULL, "
            + "STATUS VARCHAR2(10) NOT NULL"
            + ")";

    private static final String CREATE_AUTHORITY_TABLE = "CREATE TABLE AUTHORITY ("
            + "ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT, "
            + "USER_ID VARCHAR2(100) NOT NULL, "
            + "ROLE VARCHAR2(50) NOT NULL, "
            + "FOREIGN KEY (USER_ID) REFERENCES USER (ID), "
            + "CONSTRAINT USER_ROLE_UNIQUE_CONSTRAINT UNIQUE (USER_ID, ROLE)"
            + ")";

    private static final String INSERT_ANONYMOUS_USER = "INSERT INTO USER ("
            + "ID, DN, USER_NAME, CREATION, LAST_VERIFIED, JUSTIFICATION, STATUS"
            + ") VALUES ("
            + "'" + UUID.randomUUID().toString() + "', "
            + "'" + NiFiUser.ANONYMOUS_USER_DN + "', "
            + "'" + NiFiUser.ANONYMOUS_USER_DN + "', "
            + "NOW(), "
            + "NOW(), "
            + "'Anonymous user needs no justification', "
            + "'ACTIVE'"
            + ")";

    private static final String INSERT_ANONYMOUS_MONITOR_AUTHORITY = "INSERT INTO AUTHORITY ("
            + "USER_ID, ROLE"
            + ") VALUES ("
            + "(SELECT ID FROM USER WHERE DN = '" + NiFiUser.ANONYMOUS_USER_DN + "'), "
            + "'ROLE_MONITOR'"
            + ")";

    private static final String INSERT_ANONYMOUS_DFM_AUTHORITY = "INSERT INTO AUTHORITY ("
            + "USER_ID, ROLE"
            + ") VALUES ("
            + "(SELECT ID FROM USER WHERE DN = '" + NiFiUser.ANONYMOUS_USER_DN + "'), "
            + "'ROLE_DFM'"
            + ")";

    private static final String INSERT_ANONYMOUS_ADMIN_AUTHORITY = "INSERT INTO AUTHORITY ("
            + "USER_ID, ROLE"
            + ") VALUES ("
            + "(SELECT ID FROM USER WHERE DN = '" + NiFiUser.ANONYMOUS_USER_DN + "'), "
            + "'ROLE_ADMIN'"
            + ")";

    private static final String INSERT_ANONYMOUS_NIFI_AUTHORITY = "INSERT INTO AUTHORITY ("
            + "USER_ID, ROLE"
            + ") VALUES ("
            + "(SELECT ID FROM USER WHERE DN = '" + NiFiUser.ANONYMOUS_USER_DN + "'), "
            + "'ROLE_NIFI'"
            + ")";

    private static final String INSERT_ANONYMOUS_PROVENANCE_AUTHORITY = "INSERT INTO AUTHORITY ("
            + "USER_ID, ROLE"
            + ") VALUES ("
            + "(SELECT ID FROM USER WHERE DN = '" + NiFiUser.ANONYMOUS_USER_DN + "'), "
            + "'ROLE_PROVENANCE'"
            + ")";

    private static final String SELECT_ANONYMOUS_PROVENANCE_AUTHORITY = "SELECT * FROM AUTHORITY "
            + "WHERE "
            + "USER_ID = (SELECT ID FROM USER WHERE DN = '" + NiFiUser.ANONYMOUS_USER_DN + "') "
            + "AND "
            + "ROLE = 'ROLE_PROVENANCE'";

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
            File databaseFile = new File(repositoryDirectory, AUDIT_DATABASE_FILE_NAME);
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

                // determine if the tables need to be created
                rs = connection.getMetaData().getTables(null, null, "USER", null);
                if (!rs.next()) {
                    logger.info("Database not built for repository: " + databaseUrl + ".  Building now...");

                    // create the tables
                    statement.execute(CREATE_USER_TABLE);
                    statement.execute(CREATE_AUTHORITY_TABLE);

                    // seed the anonymous user
                    statement.execute(INSERT_ANONYMOUS_USER);
                    statement.execute(INSERT_ANONYMOUS_MONITOR_AUTHORITY);
                    statement.execute(INSERT_ANONYMOUS_DFM_AUTHORITY);
                    statement.execute(INSERT_ANONYMOUS_ADMIN_AUTHORITY);
                    statement.execute(INSERT_ANONYMOUS_NIFI_AUTHORITY);
                } else {
                    logger.info("Existing database found and connected to at: " + databaseUrl);
                }

                // close the previous result set
                RepositoryUtils.closeQuietly(rs);

                // merge in the provenance role to handle existing databases
                rs = statement.executeQuery(SELECT_ANONYMOUS_PROVENANCE_AUTHORITY);
                if (!rs.next()) {
                    statement.execute(INSERT_ANONYMOUS_PROVENANCE_AUTHORITY);
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

    /**
     * Get the database url for the specified database file.
     *
     * @param databaseFile
     * @return
     */
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
