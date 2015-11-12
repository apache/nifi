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
package org.apache.nifi.admin.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.nifi.admin.RepositoryUtils;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.KeyDAO;

/**
 *
 */
public class StandardKeyDAO implements KeyDAO {

    private static final String SELECT_KEY_FOR_USER = "SELECT KEY "
            + "FROM KEY "
            + "WHERE IDENTITY = ?";

    private static final String INSERT_KEY = "INSERT INTO KEY ("
            + "IDENTITY, KEY"
            + ") VALUES ("
            + "?, ?"
            + ")";

    private final Connection connection;

    public StandardKeyDAO(Connection connection) {
        this.connection = connection;
    }

    @Override
    public String getKey(String identity) {
        if (identity == null) {
            throw new IllegalArgumentException("Specified identity cannot be null.");
        }

        String key = null;

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // add each authority for the specified user
            statement = connection.prepareStatement(SELECT_KEY_FOR_USER);
            statement.setString(1, identity);

            // execute the query
            rs = statement.executeQuery();

            // if the key was found, add it
            if (rs.next()) {
                key = rs.getString("KEY");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return key;
    }

    @Override
    public String createKey(final String identity) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            final String key = UUID.randomUUID().toString();

            // add each authority for the specified user
            statement = connection.prepareStatement(INSERT_KEY);
            statement.setString(1, identity);
            statement.setString(2, key);

            // insert the key
            int updateCount = statement.executeUpdate();
            if (updateCount == 1) {
                return key;
            } else {
                throw new DataAccessException("Unable to add key for user.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }
}
