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
import org.apache.nifi.admin.dao.IdpCredentialDAO;
import org.apache.nifi.idp.IdpCredential;
import org.apache.nifi.idp.IdpType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class StandardIdpCredentialDAO implements IdpCredentialDAO {

    private static final String INSERT_CREDENTIAL = "INSERT INTO IDENTITY_PROVIDER_CREDENTIAL " +
            "(IDENTITY, IDP_TYPE, CREDENTIAL, CREATED) VALUES (?, ?, ?, ?)";

    private static final String SELECT_CREDENTIAL_BY_ID = "SELECT ID, IDENTITY, IDP_TYPE, CREDENTIAL, CREATED " +
            "FROM IDENTITY_PROVIDER_CREDENTIAL " +
            "WHERE ID = ?";

    private static final String SELECT_CREDENTIAL_BY_IDENTITY = "SELECT ID, IDENTITY, IDP_TYPE, CREDENTIAL, CREATED " +
            "FROM IDENTITY_PROVIDER_CREDENTIAL " +
            "WHERE IDENTITY = ?";

    private static final String DELETE_CREDENTIAL_BY_ID = "DELETE FROM IDENTITY_PROVIDER_CREDENTIAL " +
            "WHERE ID = ?";

    private static final String DELETE_CREDENTIAL_BY_IDENTITY = "DELETE FROM IDENTITY_PROVIDER_CREDENTIAL " +
            "WHERE IDENTITY = ?";

    private final Connection connection;

    public StandardIdpCredentialDAO(final Connection connection) {
        this.connection = connection;
    }

    @Override
    public IdpCredential createCredential(final IdpCredential credential) throws DataAccessException {
        if (credential == null) {
            throw new IllegalArgumentException("Credential cannot be null");
        }

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // populate the parameters
            statement = connection.prepareStatement(INSERT_CREDENTIAL, Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, credential.getIdentity());
            statement.setString(2, credential.getType().name());
            statement.setBytes(3, credential.getCredential());
            statement.setTimestamp(4, new java.sql.Timestamp(credential.getCreated().getTime()));

            // execute the insert
            int updateCount = statement.executeUpdate();
            rs = statement.getGeneratedKeys();

            // verify the results
            if (updateCount == 1 && rs.next()) {
                credential.setId(rs.getInt(1));
                return credential;
            } else {
                throw new DataAccessException("Unable to save IDP credential.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public IdpCredential findCredentialById(final int id) throws DataAccessException {
        IdpCredential credential = null;

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // set parameters
            statement = connection.prepareStatement(SELECT_CREDENTIAL_BY_ID);
            statement.setInt(1, id);

            // execute the query
            rs = statement.executeQuery();

            // if the credential was found, add it
            if (rs.next()) {
                credential = new IdpCredential();
                populateCredential(rs, credential);
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return credential;
    }

    @Override
    public IdpCredential findCredentialByIdentity(final String identity) throws DataAccessException {
        IdpCredential credential = null;

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // set parameters
            statement = connection.prepareStatement(SELECT_CREDENTIAL_BY_IDENTITY);
            statement.setString(1, identity);

            // execute the query
            rs = statement.executeQuery();

            // if the credential was found, add it
            if (rs.next()) {
                credential = new IdpCredential();
                populateCredential(rs, credential);
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return credential;
    }

    @Override
    public int deleteCredentialById(int id) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(DELETE_CREDENTIAL_BY_ID);
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
    public int deleteCredentialByIdentity(String identity) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(DELETE_CREDENTIAL_BY_IDENTITY);
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

    private void populateCredential(final ResultSet rs, final IdpCredential credential) throws SQLException {
        credential.setId(rs.getInt("ID"));
        credential.setIdentity(rs.getString("IDENTITY"));
        credential.setType(IdpType.valueOf(rs.getString("IDP_TYPE")));
        credential.setCredential(rs.getBytes("CREDENTIAL"));
        credential.setCreated(new Date(rs.getTimestamp("CREATED").getTime()));
    }
}
