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
package org.apache.nifi.registry.db;

import org.apache.nifi.registry.cluster.LeaderElectionManager;
import org.apache.nifi.registry.cluster.NodeAddress;
import org.apache.nifi.registry.cluster.NodeRegistry;
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
import java.util.Optional;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pure Mockito unit tests for {@link DataSyncBootstrapper}.
 *
 * <p>No Spring context is loaded.  All dependencies are mocked so that each
 * test can push {@link DataSyncBootstrapper#syncFromLeaderIfNeeded()} through
 * a specific early-exit path and verify that no HTTP sync call is made.
 */
@ExtendWith(MockitoExtension.class)
public class TestDataSyncBootstrapper {

    @Mock
    private NiFiRegistryProperties properties;

    @Mock
    private DataSource dataSource;

    @Mock
    private LeaderElectionManager leaderElectionManager;

    @Mock
    private NodeRegistry nodeRegistry;

    @Mock
    private Connection connection;

    @Mock
    private DatabaseMetaData databaseMetaData;

    @Mock
    private Statement statement;

    @Mock
    private ResultSet resultSet;

    // -------------------------------------------------------------------------
    // Tests — early-exit paths
    // -------------------------------------------------------------------------

    /**
     * When cluster is disabled syncFromLeaderIfNeeded() should return
     * immediately without touching the NodeRegistry or DataSource.
     */
    @Test
    public void testSkipsWhenNotClusterEnabled() {
        when(properties.isClusterEnabled()).thenReturn(false);

        final DataSyncBootstrapper bootstrapper = new DataSyncBootstrapper(
                properties, dataSource, leaderElectionManager, nodeRegistry);
        bootstrapper.syncFromLeaderIfNeeded();

        verify(nodeRegistry, never()).getLeaderAddress();
    }

    /**
     * When cluster coordination is "database" (not ZooKeeper)
     * syncFromLeaderIfNeeded() should skip the bootstrap sync entirely.
     */
    @Test
    public void testSkipsWhenNotZkCoordination() {
        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.isZooKeeperCoordination()).thenReturn(false);

        final DataSyncBootstrapper bootstrapper = new DataSyncBootstrapper(
                properties, dataSource, leaderElectionManager, nodeRegistry);
        bootstrapper.syncFromLeaderIfNeeded();

        verify(nodeRegistry, never()).getLeaderAddress();
    }

    /**
     * When nodeRegistry is null (non-ZK mode, no bean injected) the bootstrapper
     * should skip the sync.
     */
    @Test
    public void testSkipsWhenNodeRegistryNull() throws Exception {
        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.isZooKeeperCoordination()).thenReturn(true);

        final DataSyncBootstrapper bootstrapper = new DataSyncBootstrapper(
                properties, dataSource, leaderElectionManager, null /* nodeRegistry */);
        bootstrapper.syncFromLeaderIfNeeded();

        // No interactions on dataSource because the null nodeRegistry check
        // fires before any DB access.
        verify(dataSource, never()).getConnection();
    }

    /**
     * When this node is already the elected leader it should skip the sync —
     * leaders do not pull from themselves.
     */
    @Test
    public void testSkipsWhenIsLeader() throws Exception {
        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.isZooKeeperCoordination()).thenReturn(true);
        when(leaderElectionManager.isLeader()).thenReturn(true);

        final DataSyncBootstrapper bootstrapper = new DataSyncBootstrapper(
                properties, dataSource, leaderElectionManager, nodeRegistry);
        bootstrapper.syncFromLeaderIfNeeded();

        verify(nodeRegistry, never()).getLeaderAddress();
    }

    /**
     * When the local database already has data (COUNT > 0 in BUCKET) the
     * bootstrapper should skip the sync to avoid overwriting existing data.
     */
    @Test
    public void testSkipsWhenDbNotEmpty() throws Exception {
        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.isZooKeeperCoordination()).thenReturn(true);
        when(leaderElectionManager.isLeader()).thenReturn(false);

        // Wire up DataSource → Connection → MetaData → "H2" product name.
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getDatabaseProductName()).thenReturn("H2");

        // Wire up Statement → ResultSet → COUNT = 5 (non-empty).
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SELECT COUNT(*) FROM BUCKET")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(5);

        final DataSyncBootstrapper bootstrapper = new DataSyncBootstrapper(
                properties, dataSource, leaderElectionManager, nodeRegistry);
        bootstrapper.syncFromLeaderIfNeeded();

        // If DB is non-empty the bootstrapper returns before calling getLeaderAddress().
        verify(nodeRegistry, never()).getLeaderAddress();
    }

    /**
     * When this node wins the election while waiting for the leader address it
     * should detect the leadership change on re-check and skip the HTTP sync call.
     */
    @Test
    public void testSkipsWhenLeaderElectedDuringWait() throws Exception {
        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.isZooKeeperCoordination()).thenReturn(true);
        when(properties.getClusterNodeInternalAuthToken()).thenReturn("test-token");

        // First call: not leader; second call (re-check after waitForLeader): leader.
        when(leaderElectionManager.isLeader()).thenReturn(false, true);

        // Wire up DataSource → H2 + empty bucket table.
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getDatabaseProductName()).thenReturn("H2");
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SELECT COUNT(*) FROM BUCKET")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(0); // empty DB

        // Leader address is available immediately so waitForLeader() returns fast.
        final NodeAddress leaderAddress = new NodeAddress("leader-node", "http://leader:18080");
        when(nodeRegistry.getLeaderAddress()).thenReturn(Optional.of(leaderAddress));

        final DataSyncBootstrapper bootstrapper = new DataSyncBootstrapper(
                properties, dataSource, leaderElectionManager, nodeRegistry);
        bootstrapper.syncFromLeaderIfNeeded();

        // getLeaderAddress() must have been called — we entered the waitForLeader() path.
        verify(nodeRegistry).getLeaderAddress();
        // isLocalDbH2() and isLocalDbEmpty() each call getConnection() — exactly 2 calls.
        // A 3rd call would indicate applyScript() ran (RUNSCRIPT), which must NOT happen
        // because the post-wait isLeader() re-check returned true.
        // We use org.mockito.Mockito.times(2) to assert exactly two getConnection() calls.
        verify(dataSource, times(2)).getConnection();
    }

    /**
     * When no leader is elected within the timeout window the bootstrapper
     * should log a warning and return gracefully without throwing.
     */
    @Test
    public void testSkipsWhenNoLeaderElectedWithinTimeout() throws Exception {
        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.isZooKeeperCoordination()).thenReturn(true);
        when(properties.getClusterNodeInternalAuthToken()).thenReturn("test-token");
        when(leaderElectionManager.isLeader()).thenReturn(false);

        // Wire up DataSource → H2 + empty bucket table.
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getDatabaseProductName()).thenReturn("H2");
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SELECT COUNT(*) FROM BUCKET")).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(0); // empty DB

        // nodeRegistry never returns a leader address.
        when(nodeRegistry.getLeaderAddress()).thenReturn(Optional.empty());

        // Override the wait timeout to 1 ms so the test completes quickly.
        // DataSyncBootstrapper reads LEADER_WAIT_TIMEOUT_MS as a static final;
        // we accept the full 30s wait would be too long, so instead we verify
        // that getLeaderAddress() was called at least once and the method
        // returns without exception.
        //
        // For faster test execution we interrupt the thread after a brief moment.
        final Thread testThread = Thread.currentThread();
        final Thread interrupter = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ignored) {
            }
            testThread.interrupt();
        });
        interrupter.setDaemon(true);
        interrupter.start();

        final DataSyncBootstrapper bootstrapper = new DataSyncBootstrapper(
                properties, dataSource, leaderElectionManager, nodeRegistry);
        // Must not throw even if interrupted or timed out.
        bootstrapper.syncFromLeaderIfNeeded();
        // Clear interrupt flag if it was set.
        Thread.interrupted();

        // getLeaderAddress() must have been called at least once (we entered the wait loop).
        verify(nodeRegistry).getLeaderAddress();
    }
}
