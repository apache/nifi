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
import java.sql.Statement;
import java.util.UUID;
import org.apache.nifi.admin.RepositoryUtils;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.KeyDAO;
import org.apache.nifi.key.Key;

/**
 *
 */
public class StandardKeyDAO implements KeyDAO {

    private static final String SELECT_KEY_FOR_USER_BY_ID = "SELECT ID, IDENTITY, KEY "
            + "FROM KEY "
            + "WHERE ID = ?";

    private static final String SELECT_KEY_FOR_USER_BY_IDENTITY = "SELECT ID, IDENTITY, KEY "
            + "FROM KEY "
            + "WHERE IDENTITY = ?";

    private static final String INSERT_KEY = "INSERT INTO KEY ("
            + "IDENTITY, KEY"
            + ") VALUES ("
            + "?, ?"
            + ")";

    private static final String DELETE_KEYS = "DELETE FROM KEY "
            + "WHERE IDENTITY = ?";

    private final Connection connection;

    public StandardKeyDAO(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Key findKeyById(int id) {
        Key key = null;

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // add each authority for the specified user
            statement = connection.prepareStatement(SELECT_KEY_FOR_USER_BY_ID);
            statement.setInt(1, id);

            // execute the query
            rs = statement.executeQuery();

            // if the key was found, add it
            if (rs.next()) {
                key = new Key();
                key.setId(rs.getInt("ID"));
                key.setIdentity(rs.getString("IDENTITY"));
                key.setKey(rs.getString("KEY"));
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
    public Key findLatestKeyByIdentity(String identity) {
        if (identity == null) {
            throw new IllegalArgumentException("Specified identity cannot be null.");
        }

        Key key = null;

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // add each authority for the specified user
            statement = connection.prepareStatement(SELECT_KEY_FOR_USER_BY_IDENTITY);
            statement.setString(1, identity);

            // execute the query
            rs = statement.executeQuery();

            // if the key was found, add it
            if (rs.next()) {
                key = new Key();
                key.setId(rs.getInt("ID"));
                key.setIdentity(rs.getString("IDENTITY"));
                key.setKey(rs.getString("KEY"));
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
    public Key createKey(final String identity) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            final String keyValue = UUID.randomUUID().toString();

            // add each authority for the specified user
            statement = connection.prepareStatement(INSERT_KEY, Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, identity);
            statement.setString(2, keyValue);

            // insert the key
            int updateCount = statement.executeUpdate();
            rs = statement.getGeneratedKeys();

            // verify the results
            if (updateCount == 1 && rs.next()) {
                final Key key = new Key();
                key.setId(rs.getInt(1));
                key.setIdentity(identity);
                key.setKey(keyValue);
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

    @Override
    public void deleteKeys(String identity) {
        PreparedStatement statement = null;
        try {
            // add each authority for the specified user
            statement = connection.prepareStatement(DELETE_KEYS);
            statement.executeUpdate();
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } catch (DataAccessException dae) {
            throw dae;
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

}
