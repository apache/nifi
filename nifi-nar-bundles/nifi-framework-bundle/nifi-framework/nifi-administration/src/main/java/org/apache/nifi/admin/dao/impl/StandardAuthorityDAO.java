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
import java.util.EnumSet;
import java.util.Set;
import org.apache.nifi.admin.RepositoryUtils;
import org.apache.nifi.admin.dao.AuthorityDAO;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.authorization.Authority;

/**
 *
 */
public class StandardAuthorityDAO implements AuthorityDAO {

    private static final String SELECT_AUTHORITIES_FOR_USER = "SELECT ID, ROLE "
            + "FROM AUTHORITY "
            + "WHERE USER_ID = ?";

    private static final String INSERT_AUTHORITY = "INSERT INTO AUTHORITY ("
            + "USER_ID, ROLE"
            + ") VALUES ("
            + "?, ?"
            + ")";

    private static final String DELETE_AUTHORITY = "DELETE FROM AUTHORITY "
            + "WHERE USER_ID = ? AND ROLE = ?";

    private static final String DELETE_AUTHORITIES_FOR_USER = "DELETE FROM AUTHORITY "
            + "WHERE USER_ID = ?";

    private final Connection connection;

    public StandardAuthorityDAO(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void createAuthorities(Set<Authority> authorities, String userId) throws DataAccessException {
        if (authorities == null) {
            throw new IllegalArgumentException("Specified authorities cannot be null.");
        }

        // ensure there are some authorities to create
        if (!authorities.isEmpty()) {
            PreparedStatement statement = null;
            try {
                // add each authority for the specified user
                statement = connection.prepareStatement(INSERT_AUTHORITY);
                statement.setString(1, userId);
                for (Authority authority : authorities) {
                    statement.setString(2, authority.toString());
                    statement.addBatch();
                }

                // insert the authorities
                int[] updateCounts = statement.executeBatch();
                for (int updateCount : updateCounts) {
                    if (updateCount != 1) {
                        throw new DataAccessException("Unable to insert user authorities.");
                    }
                }
            } catch (SQLException sqle) {
                throw new DataAccessException(sqle);
            } catch (DataAccessException dae) {
                throw dae;
            } finally {
                RepositoryUtils.closeQuietly(statement);
            }
        }
    }

    @Override
    public void deleteAuthorities(String userId) throws DataAccessException {
        // ensure there are some authorities to create
        PreparedStatement statement = null;
        try {
            // add each authority for the specified user
            statement = connection.prepareStatement(DELETE_AUTHORITIES_FOR_USER);
            statement.setString(1, userId);

            // insert the authorities
            statement.executeUpdate();
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public void deleteAuthorities(Set<Authority> authorities, String userId) throws DataAccessException {
        if (authorities == null) {
            throw new IllegalArgumentException("Specified authorities cannot be null.");
        }

        // ensure there are some authorities to create
        if (!authorities.isEmpty()) {
            PreparedStatement statement = null;
            try {
                // add each authority for the specified user
                statement = connection.prepareStatement(DELETE_AUTHORITY);
                statement.setString(1, userId);
                for (Authority authority : authorities) {
                    statement.setString(2, authority.toString());
                    statement.addBatch();
                }

                // insert the authorities
                int[] updateCounts = statement.executeBatch();
                for (int updateCount : updateCounts) {
                    if (updateCount != 1) {
                        throw new DataAccessException("Unable to remove user authorities.");
                    }
                }
            } catch (SQLException sqle) {
                throw new DataAccessException(sqle);
            } catch (DataAccessException dae) {
                throw dae;
            } finally {
                RepositoryUtils.closeQuietly(statement);
            }
        }
    }

    @Override
    public Set<Authority> findAuthoritiesByUserId(String userId) throws DataAccessException {
        Set<Authority> authorities = EnumSet.noneOf(Authority.class);
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // add each authority for the specified user
            statement = connection.prepareStatement(SELECT_AUTHORITIES_FOR_USER);
            statement.setString(1, userId);

            // execute the query
            rs = statement.executeQuery();

            // create each corresponding authority
            while (rs.next()) {
                authorities.add(Authority.valueOfAuthority(rs.getString("ROLE")));
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return authorities;
    }

}
