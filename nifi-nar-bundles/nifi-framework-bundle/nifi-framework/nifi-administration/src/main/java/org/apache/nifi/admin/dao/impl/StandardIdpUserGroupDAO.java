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

import org.apache.nifi.admin.RepositoryUtils;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.IdpUserGroupDAO;
import org.apache.nifi.idp.IdpType;
import org.apache.nifi.idp.IdpUserGroup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class StandardIdpUserGroupDAO implements IdpUserGroupDAO {

    private static final String INSERT_USER_GROUP = "INSERT INTO IDENTITY_PROVIDER_USER_GROUP " +
            "(IDENTITY, IDP_TYPE, GROUP_NAME, CREATED) VALUES (?, ?, ?, ?)";

    private static final String SELECT_USER_GROUP_BY_ID = "SELECT ID, IDENTITY, IDP_TYPE, GROUP_NAME, CREATED " +
            "FROM IDENTITY_PROVIDER_USER_GROUP " +
            "WHERE ID =?";

    private static final String SELECT_USER_GROUP_BY_IDENTITY = "SELECT ID, IDENTITY, IDP_TYPE, GROUP_NAME, CREATED " +
            "FROM IDENTITY_PROVIDER_USER_GROUP " +
            "WHERE IDENTITY =?";

    private static final String DELETE_USER_GROUPS_BY_ID = "DELETE FROM IDENTITY_PROVIDER_USER_GROUP " +
            "WHERE ID = ?";

    private static final String DELETE_USER_GROUPS_BY_IDENTITY = "DELETE FROM IDENTITY_PROVIDER_USER_GROUP " +
            "WHERE IDENTITY = ?";

    private final Connection connection;

    public StandardIdpUserGroupDAO(final Connection connection) {
        this.connection = connection;
    }

    @Override
    public IdpUserGroup createUserGroup(final IdpUserGroup userGroup) throws DataAccessException {
        if (userGroup == null) {
            throw new IllegalArgumentException("UserGroup cannot be null");
        }

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // populate the parameters
            statement = connection.prepareStatement(INSERT_USER_GROUP, Statement.RETURN_GENERATED_KEYS);
            populateStatement(statement, userGroup);

            // execute the insert
            int updateCount = statement.executeUpdate();
            rs = statement.getGeneratedKeys();

            // verify the results
            if (updateCount == 1 && rs.next()) {
                userGroup.setId(rs.getInt(1));
                return userGroup;
            } else {
                throw new DataAccessException("Unable to save IDP User Group.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public List<IdpUserGroup> createUserGroups(final List<IdpUserGroup> userGroups) throws DataAccessException {
        if (userGroups == null) {
            throw new IllegalArgumentException("UserGroups cannot be null");
        }

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // populate the parameters
            statement = connection.prepareStatement(INSERT_USER_GROUP, Statement.RETURN_GENERATED_KEYS);

            for (final IdpUserGroup userGroup : userGroups) {
                populateStatement(statement, userGroup);
                statement.addBatch();
            }

            int[] updateCounts = statement.executeBatch();
            if (updateCounts.length != userGroups.size()) {
                throw new DataAccessException("Unable to save IDP User Groups");
            }

            for (int i=0; i < updateCounts.length; i++) {
                if (updateCounts[i] == 0) {
                    throw new DataAccessException("Unable to save IDP User Groups");
                }
            }

            rs = statement.getGeneratedKeys();

            int count = 0;
            while (rs.next()) {
                final int id = rs.getInt(1);
                final IdpUserGroup userGroup = userGroups.get(count);
                userGroup.setId(id);
                count++;
            }

        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return userGroups;
    }

    private void populateStatement(PreparedStatement statement, IdpUserGroup userGroup) throws SQLException {
        statement.setString(1, userGroup.getIdentity());
        statement.setString(2, userGroup.getType().name());
        statement.setString(3, userGroup.getGroupName());
        statement.setTimestamp(4, new java.sql.Timestamp(userGroup.getCreated().getTime()));
    }

    @Override
    public IdpUserGroup findUserGroupById(final int id) throws DataAccessException {
        IdpUserGroup userGroup = null;

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // set parameters
            statement = connection.prepareStatement(SELECT_USER_GROUP_BY_ID);
            statement.setInt(1, id);

            // execute the query
            rs = statement.executeQuery();

            // if the group was found, add it
            if (rs.next()) {
                userGroup = new IdpUserGroup();
                populateUserGroup(rs, userGroup);
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return userGroup;
    }

    @Override
    public List<IdpUserGroup> findUserGroupsByIdentity(final String identity) throws DataAccessException {
        final List<IdpUserGroup> userGroups = new ArrayList<>();

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // set parameters
            statement = connection.prepareStatement(SELECT_USER_GROUP_BY_IDENTITY);
            statement.setString(1, identity);

            // execute the query
            rs = statement.executeQuery();

            // add any found groups to the result list
            while (rs.next()) {
                final IdpUserGroup userGroup = new IdpUserGroup();
                populateUserGroup(rs, userGroup);
                userGroups.add(userGroup);
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return userGroups;
    }

    @Override
    public int deleteUserGroupById(int id) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(DELETE_USER_GROUPS_BY_ID);
            statement.setInt(1, id);
            return statement.executeUpdate();
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } catch (DataAccessException dae) {
            throw dae;
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public int deleteUserGroupsByIdentity(final String identity) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(DELETE_USER_GROUPS_BY_IDENTITY);
            statement.setString(1, identity);
            return statement.executeUpdate();
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } catch (DataAccessException dae) {
            throw dae;
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    private void populateUserGroup(final ResultSet rs, final IdpUserGroup userGroup) throws SQLException {
        userGroup.setId(rs.getInt("ID"));
        userGroup.setIdentity(rs.getString("IDENTITY"));
        userGroup.setType(IdpType.valueOf(rs.getString("IDP_TYPE")));
        userGroup.setGroupName(rs.getString("GROUP_NAME"));
        userGroup.setCreated(new Date(rs.getTimestamp("CREATED").getTime()));
    }
}
