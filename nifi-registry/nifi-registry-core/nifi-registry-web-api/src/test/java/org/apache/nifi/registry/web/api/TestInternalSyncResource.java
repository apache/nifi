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
package org.apache.nifi.registry.web.api;

import jakarta.ws.rs.core.Response;
import org.apache.nifi.registry.cluster.LeaderElectionManager;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Pure Mockito unit tests for {@link InternalSyncResource}.
 *
 * <p>No Spring context is loaded. DataSource → Connection → MetaData / Statement
 * chains are stubbed to control the behaviour of
 * {@link InternalSyncResource#export(String)}.
 */
@ExtendWith(MockitoExtension.class)
public class TestInternalSyncResource {

    private static final String VALID_TOKEN = "shared-secret-token";

    @Mock
    private NiFiRegistryProperties properties;

    @Mock
    private LeaderElectionManager leaderElectionManager;

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private DatabaseMetaData databaseMetaData;

    @Mock
    private Statement statement;

    @Mock
    private ResultSet resultSet;

    private InternalSyncResource resource;

    @BeforeEach
    public void setup() {
        resource = new InternalSyncResource(properties, leaderElectionManager, dataSource);
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * Null auth token should return 403 Forbidden without touching the database.
     */
    @Test
    public void testMissingTokenReturnsForbidden() {
        when(properties.getClusterNodeInternalAuthToken()).thenReturn(VALID_TOKEN);

        final Response response = resource.export(null);

        assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    }

    /**
     * A wrong auth token should return 403 Forbidden.
     */
    @Test
    public void testWrongTokenReturnsForbidden() {
        when(properties.getClusterNodeInternalAuthToken()).thenReturn(VALID_TOKEN);

        final Response response = resource.export("wrong-token");

        assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    }

    /**
     * A correct token but this node is not the leader should return 503.
     */
    @Test
    public void testNotLeaderReturnsServiceUnavailable() {
        when(properties.getClusterNodeInternalAuthToken()).thenReturn(VALID_TOKEN);
        when(leaderElectionManager.isLeader()).thenReturn(false);

        final Response response = resource.export(VALID_TOKEN);

        assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    }

    /**
     * When the underlying database is not H2, the export should return a
     * server error (500) containing an explanatory message.
     */
    @Test
    public void testNonH2DatabaseThrowsUnsupported() throws Exception {
        when(properties.getClusterNodeInternalAuthToken()).thenReturn(VALID_TOKEN);
        when(leaderElectionManager.isLeader()).thenReturn(true);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getDatabaseProductName()).thenReturn("PostgreSQL");

        final Response response = resource.export(VALID_TOKEN);

        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity().toString().contains("PostgreSQL"),
                "Error message should mention the unsupported DB type");
    }

    /**
     * Happy path: valid token, this node is the leader, H2 database.
     * The response should be 200 with the two SQL script lines concatenated.
     */
    @Test
    public void testLeaderReturnsScript() throws Exception {
        when(properties.getClusterNodeInternalAuthToken()).thenReturn(VALID_TOKEN);
        when(leaderElectionManager.isLeader()).thenReturn(true);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getDatabaseProductName()).thenReturn("H2");

        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SCRIPT DROP NOPASSWORDS NOSETTINGS")).thenReturn(resultSet);

        // ResultSet returns two rows then signals end-of-results.
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString(1))
                .thenReturn("CREATE TABLE FOO (ID INT);")
                .thenReturn("INSERT INTO FOO VALUES (1);");

        final Response response = resource.export(VALID_TOKEN);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        final String body = (String) response.getEntity();
        assertNotNull(body);
        assertTrue(body.contains("CREATE TABLE FOO"), "Response should contain CREATE TABLE statement");
        assertTrue(body.contains("INSERT INTO FOO"), "Response should contain INSERT statement");
    }
}
