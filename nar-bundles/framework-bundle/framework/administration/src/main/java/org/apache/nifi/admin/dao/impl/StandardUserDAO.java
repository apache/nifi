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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import org.apache.nifi.admin.dao.UserDAO;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.nifi.admin.RepositoryUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;

/**
 * Responsible for loading and persisting NiFiUsers.
 */
public class StandardUserDAO implements UserDAO {

    private static final String SELECT_PENDING_ACCOUNTS_COUNT = "SELECT "
            + "COUNT(*) as PENDING_ACCOUNTS "
            + "FROM USER U "
            + "WHERE U.STATUS = 'PENDING'";

    private static final String SELECT_USER_BY_DN = "SELECT "
            + "U.ID, "
            + "U.DN, "
            + "U.USER_NAME, "
            + "U.USER_GROUP, "
            + "U.CREATION, "
            + "U.LAST_ACCESSED, "
            + "U.LAST_VERIFIED, "
            + "U.JUSTIFICATION, "
            + "U.STATUS, "
            + "A.ROLE "
            + "FROM USER U "
            + "LEFT JOIN AUTHORITY A " // ensures that users without authorities are still matched
            + "ON U.ID = A.USER_ID "
            + "WHERE U.DN = ?";

    private static final String SELECT_USER_BY_ID = "SELECT "
            + "U.ID, "
            + "U.DN, "
            + "U.USER_NAME, "
            + "U.USER_GROUP, "
            + "U.CREATION, "
            + "U.LAST_ACCESSED, "
            + "U.LAST_VERIFIED, "
            + "U.JUSTIFICATION, "
            + "U.STATUS, "
            + "A.ROLE "
            + "FROM USER U "
            + "LEFT JOIN AUTHORITY A " // ensures that users without authorities are still matched
            + "ON U.ID = A.USER_ID "
            + "WHERE U.ID = ?";

    private static final String SELECT_USERS = "SELECT "
            + "U.ID, "
            + "U.DN, "
            + "U.USER_NAME, "
            + "U.USER_GROUP, "
            + "U.CREATION, "
            + "U.LAST_ACCESSED, "
            + "U.LAST_VERIFIED, "
            + "U.JUSTIFICATION, "
            + "U.STATUS, "
            + "A.ROLE "
            + "FROM USER U "
            + "LEFT JOIN AUTHORITY A " // ensures that users without authorities are still matched
            + "ON U.ID = A.USER_ID "
            + "WHERE U.DN <> ?";

    private static final String SELECT_USER_GROUPS = "SELECT DISTINCT "
            + "U.USER_GROUP "
            + "FROM USER U";

    private static final String SELECT_USER_GROUP = "SELECT "
            + "U.ID, "
            + "U.DN, "
            + "U.USER_NAME, "
            + "U.USER_GROUP, "
            + "U.CREATION, "
            + "U.LAST_ACCESSED, "
            + "U.LAST_VERIFIED, "
            + "U.JUSTIFICATION, "
            + "U.STATUS, "
            + "A.ROLE "
            + "FROM USER U "
            + "LEFT JOIN AUTHORITY A " // ensures that users without authorities are still matched
            + "ON U.ID = A.USER_ID "
            + "WHERE U.DN <> ? AND U.USER_GROUP = ?";

    private static final String INSERT_USER = "INSERT INTO USER ("
            + "ID, DN, USER_NAME, USER_GROUP, CREATION, LAST_VERIFIED, JUSTIFICATION, STATUS"
            + ") VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "NOW(), "
            + "?, "
            + "?, "
            + "?"
            + ")";

    private static final String UPDATE_USER = "UPDATE USER SET "
            + "DN = ?, "
            + "USER_NAME = ?, "
            + "USER_GROUP = ?, "
            + "LAST_ACCESSED = ?, "
            + "LAST_VERIFIED = ?, "
            + "JUSTIFICATION = ?, "
            + "STATUS = ? "
            + "WHERE ID = ?";

    private static final String UPDATE_USER_GROUP_STATUS = "UPDATE USER SET "
            + "STATUS = ?,"
            + "USER_GROUP = NULL "
            + "WHERE USER_GROUP = ?";

    private static final String UPDATE_USER_GROUP_VERIFICATION = "UPDATE USER SET "
            + "LAST_VERIFIED = ? "
            + "WHERE USER_GROUP = ?";

    private static final String UNGROUP_GROUP = "UPDATE USER SET "
            + "USER_GROUP = NULL "
            + "WHERE USER_GROUP = ?";

    private static final String DELETE_USER = "DELETE FROM USER "
            + "WHERE ID = ?";

    private final Connection connection;

    public StandardUserDAO(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Boolean hasPendingUserAccounts() throws DataAccessException {
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the connection and obtain a statement
            statement = connection.prepareStatement(SELECT_PENDING_ACCOUNTS_COUNT);

            // execute the query
            rs = statement.executeQuery();

            // get the first row which will contain the number of pending accounts
            if (rs.next()) {
                int pendingAccounts = rs.getInt("PENDING_ACCOUNTS");
                return pendingAccounts > 0;
            }

            // query returned no results?
            return false;
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public Set<NiFiUser> findUsers() throws DataAccessException {
        Set<NiFiUser> users = new HashSet<>();

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the connection and obtain a statement
            statement = connection.prepareStatement(SELECT_USERS);
            statement.setString(1, NiFiUser.ANONYMOUS_USER_DN);

            // execute the query
            rs = statement.executeQuery();

            // create the user
            NiFiUser user = null;

            // go through the user and its roles
            while (rs.next()) {
                // get the user id for the current record
                String userId = rs.getString("ID");

                // create the user during the first iteration
                if (user == null || !userId.equals(user.getId())) {
                    user = new NiFiUser();
                    user.setId(userId);
                    user.setDn(rs.getString("DN"));
                    user.setUserName(rs.getString("USER_NAME"));
                    user.setUserGroup(rs.getString("USER_GROUP"));
                    user.setJustification(rs.getString("JUSTIFICATION"));
                    user.setStatus(AccountStatus.valueOfStatus(rs.getString("STATUS")));

                    // set the creation date
                    user.setCreation(new Date(rs.getTimestamp("CREATION").getTime()));

                    // get the last accessed date
                    if (rs.getTimestamp("LAST_ACCESSED") != null) {
                        user.setLastAccessed(new Date(rs.getTimestamp("LAST_ACCESSED").getTime()));
                    }

                    // get the last verified date
                    if (rs.getTimestamp("LAST_VERIFIED") != null) {
                        user.setLastVerified(new Date(rs.getTimestamp("LAST_VERIFIED").getTime()));
                    }

                    // add the user
                    users.add(user);
                }

                // the select statement performs a left join since the desired
                // user may not have any authorities
                String authority = rs.getString("ROLE");
                if (StringUtils.isNotBlank(authority)) {
                    user.getAuthorities().add(Authority.valueOfAuthority(authority));
                }
            }

            return users;
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public Set<String> findUserGroups() throws DataAccessException {
        Set<String> userGroups = new HashSet<>();

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the connection and obtain a statement
            statement = connection.prepareStatement(SELECT_USER_GROUPS);

            // execute the query
            rs = statement.executeQuery();

            // get each user group
            while (rs.next()) {
                userGroups.add(rs.getString("USER_GROUP"));
            }

            return userGroups;
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public Set<NiFiUser> findUsersForGroup(String group) throws DataAccessException {
        Set<NiFiUser> users = new HashSet<>();

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the connection and obtain a statement
            statement = connection.prepareStatement(SELECT_USER_GROUP);
            statement.setString(1, NiFiUser.ANONYMOUS_USER_DN);
            statement.setString(2, group);

            // execute the query
            rs = statement.executeQuery();

            // create the user
            NiFiUser user = null;

            // go through the user and its roles
            while (rs.next()) {
                // get the user id for the current record
                String userId = rs.getString("ID");

                // create the user during the first iteration
                if (user == null || !userId.equals(user.getId())) {
                    user = new NiFiUser();
                    user.setId(userId);
                    user.setDn(rs.getString("DN"));
                    user.setUserName(rs.getString("USER_NAME"));
                    user.setUserGroup(rs.getString("USER_GROUP"));
                    user.setJustification(rs.getString("JUSTIFICATION"));
                    user.setStatus(AccountStatus.valueOfStatus(rs.getString("STATUS")));

                    // set the creation date
                    user.setCreation(new Date(rs.getTimestamp("CREATION").getTime()));

                    // get the last accessed date
                    if (rs.getTimestamp("LAST_ACCESSED") != null) {
                        user.setLastAccessed(new Date(rs.getTimestamp("LAST_ACCESSED").getTime()));
                    }

                    // get the last verified date
                    if (rs.getTimestamp("LAST_VERIFIED") != null) {
                        user.setLastVerified(new Date(rs.getTimestamp("LAST_VERIFIED").getTime()));
                    }

                    // add the user
                    users.add(user);
                }

                // the select statement performs a left join since the desired
                // user may not have any authorities
                String authority = rs.getString("ROLE");
                if (StringUtils.isNotBlank(authority)) {
                    user.getAuthorities().add(Authority.valueOfAuthority(authority));
                }
            }

            return users;
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public NiFiUser findUserById(String id) throws DataAccessException {
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the connection and obtain a statement
            statement = connection.prepareStatement(SELECT_USER_BY_ID);
            statement.setString(1, id);

            // execute the query
            rs = statement.executeQuery();

            // create the user
            NiFiUser user = null;

            // go through the user and its roles
            while (rs.next()) {
                // create the user during the first iteration
                if (user == null) {
                    user = new NiFiUser();
                    user.setId(rs.getString("ID"));
                    user.setDn(rs.getString("DN"));
                    user.setUserName(rs.getString("USER_NAME"));
                    user.setUserGroup(rs.getString("USER_GROUP"));
                    user.setJustification(rs.getString("JUSTIFICATION"));
                    user.setStatus(AccountStatus.valueOfStatus(rs.getString("STATUS")));

                    // set the creation date
                    user.setCreation(new Date(rs.getTimestamp("CREATION").getTime()));

                    // get the last accessed date
                    if (rs.getTimestamp("LAST_ACCESSED") != null) {
                        user.setLastAccessed(new Date(rs.getTimestamp("LAST_ACCESSED").getTime()));
                    }

                    // get the last verified date
                    if (rs.getTimestamp("LAST_VERIFIED") != null) {
                        user.setLastVerified(new Date(rs.getTimestamp("LAST_VERIFIED").getTime()));
                    }
                }

                // the select statement performs a left join since the desired
                // user may not have any authorities
                String authority = rs.getString("ROLE");
                if (StringUtils.isNotBlank(authority)) {
                    user.getAuthorities().add(Authority.valueOfAuthority(authority));
                }
            }

            return user;
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public NiFiUser findUserByDn(String dn) throws DataAccessException {
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the connection and obtain a statement
            statement = connection.prepareStatement(SELECT_USER_BY_DN);
            statement.setString(1, dn);

            // execute the query
            rs = statement.executeQuery();

            // create the user
            NiFiUser user = null;

            // go through the user and its roles
            while (rs.next()) {
                // create the user during the first iteration
                if (user == null) {
                    user = new NiFiUser();
                    user.setId(rs.getString("ID"));
                    user.setDn(rs.getString("DN"));
                    user.setUserName(rs.getString("USER_NAME"));
                    user.setUserGroup(rs.getString("USER_GROUP"));
                    user.setJustification(rs.getString("JUSTIFICATION"));
                    user.setStatus(AccountStatus.valueOfStatus(rs.getString("STATUS")));

                    // set the creation date
                    user.setCreation(new Date(rs.getTimestamp("CREATION").getTime()));

                    // get the last accessed date
                    if (rs.getTimestamp("LAST_ACCESSED") != null) {
                        user.setLastAccessed(new Date(rs.getTimestamp("LAST_ACCESSED").getTime()));
                    }

                    // get the last verified date
                    if (rs.getTimestamp("LAST_VERIFIED") != null) {
                        user.setLastVerified(new Date(rs.getTimestamp("LAST_VERIFIED").getTime()));
                    }
                }

                // the select statement performs a left join since the desired
                // user may not have any authorities
                String authority = rs.getString("ROLE");
                if (StringUtils.isNotBlank(authority)) {
                    user.getAuthorities().add(Authority.valueOfAuthority(authority));
                }
            }

            return user;
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public void createUser(NiFiUser user) throws DataAccessException {
        if (user.getDn() == null) {
            throw new IllegalArgumentException("User dn must be specified.");
        }

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            final String id = UUID.nameUUIDFromBytes(user.getDn().getBytes(StandardCharsets.UTF_8)).toString();

            // create a statement
            statement = connection.prepareStatement(INSERT_USER, Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, id);
            statement.setString(2, StringUtils.left(user.getDn(), 255));
            statement.setString(3, StringUtils.left(user.getUserName(), 100));
            statement.setString(4, StringUtils.left(user.getUserGroup(), 100));
            if (user.getLastVerified() != null) {
                statement.setTimestamp(5, new java.sql.Timestamp(user.getLastVerified().getTime()));
            } else {
                statement.setTimestamp(5, null);
            }
            statement.setString(6, StringUtils.left(user.getJustification(), 500));
            statement.setString(7, user.getStatus().toString());

            // insert the user
            int updateCount = statement.executeUpdate();
            if (updateCount == 1) {
                user.setId(id);
            } else {
                throw new DataAccessException("Unable to insert user.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } catch (DataAccessException dae) {
            throw dae;
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public void deleteUser(String id) throws DataAccessException {
        // ensure there are some authorities to create
        PreparedStatement statement = null;
        try {
            // add each authority for the specified user
            statement = connection.prepareStatement(DELETE_USER);
            statement.setString(1, id);

            // insert the authorities
            statement.executeUpdate();
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } catch (DataAccessException dae) {
            throw dae;
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public void updateUser(NiFiUser user) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // create a statement
            statement = connection.prepareStatement(UPDATE_USER);
            statement.setString(1, StringUtils.left(user.getDn(), 255));
            statement.setString(2, StringUtils.left(user.getUserName(), 100));
            statement.setString(3, StringUtils.left(user.getUserGroup(), 100));
            statement.setString(6, StringUtils.left(user.getJustification(), 500));
            statement.setString(7, user.getStatus().toString());
            statement.setString(8, user.getId());

            // set the last accessed time accordingly
            if (user.getLastAccessed() == null) {
                statement.setNull(4, Types.TIMESTAMP);
            } else {
                statement.setTimestamp(4, new java.sql.Timestamp(user.getLastAccessed().getTime()));
            }

            // set the last verified time accordingly
            if (user.getLastVerified() == null) {
                statement.setNull(5, Types.TIMESTAMP);
            } else {
                statement.setTimestamp(5, new java.sql.Timestamp(user.getLastVerified().getTime()));
            }

            // perform the update
            int updateCount = statement.executeUpdate();
            if (updateCount != 1) {
                throw new DataAccessException("Unable to update user.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } catch (DataAccessException dae) {
            throw dae;
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public void updateGroupStatus(String group, AccountStatus status) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // create a statement
            statement = connection.prepareStatement(UPDATE_USER_GROUP_STATUS);
            statement.setString(1, status.toString());
            statement.setString(2, group);

            // perform the update
            statement.executeUpdate();
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } catch (DataAccessException dae) {
            throw dae;
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public void updateGroupVerification(String group, Date lastVerified) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // create a statement
            statement = connection.prepareStatement(UPDATE_USER_GROUP_VERIFICATION);

            // set the last verified time accordingly
            if (lastVerified == null) {
                statement.setNull(1, Types.TIMESTAMP);
            } else {
                statement.setTimestamp(1, new java.sql.Timestamp(lastVerified.getTime()));
            }

            // set the group
            statement.setString(2, group);

            // perform the update
            statement.executeUpdate();
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } catch (DataAccessException dae) {
            throw dae;
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public void ungroup(String group) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // create a statement
            statement = connection.prepareStatement(UNGROUP_GROUP);
            statement.setString(1, group);

            // perform the update
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
