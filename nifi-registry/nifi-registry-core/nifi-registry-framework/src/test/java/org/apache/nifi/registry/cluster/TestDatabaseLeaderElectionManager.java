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
package org.apache.nifi.registry.cluster;

import org.apache.nifi.registry.db.DatabaseBaseTest;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Integration tests for {@link DatabaseLeaderElectionManager} using the
 * in-memory H2 database wired up by {@link DatabaseBaseTest}.
 *
 * <p>All tests call {@link DatabaseLeaderElectionManager#tryAcquireOrRenew()}
 * directly (package-private, same package) to avoid spawning the background
 * scheduler and to keep tests fast and deterministic.
 */
public class TestDatabaseLeaderElectionManager extends DatabaseBaseTest {

    private static final String TEST_NODE_ID = "test-node";
    private static final String OTHER_NODE_ID = "other-node";
    private static final String LOCK_KEY = "LEADER";

    @Autowired
    private DataSource dataSource;

    @Autowired
    private NiFiRegistryProperties properties;

    private JdbcTemplate jdbcTemplate;
    private DatabaseLeaderElectionManager manager;

    @BeforeEach
    public void setup() {
        jdbcTemplate = new JdbcTemplate(dataSource);

        // Ensure the CLUSTER_LEADER table is empty before each test.
        jdbcTemplate.update("DELETE FROM CLUSTER_LEADER");

        when(properties.isClusterEnabled()).thenReturn(true);
        when(properties.getClusterNodeIdentifier()).thenReturn(TEST_NODE_ID);

        manager = new DatabaseLeaderElectionManager(dataSource, properties);
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * When the CLUSTER_LEADER table is empty the first call to
     * tryAcquireOrRenew() should INSERT a row and return true.
     */
    @Test
    public void testFirstNodeBecomesLeader() {
        final boolean result = manager.tryAcquireOrRenew();

        // tryAcquireOrRenew() returns true when the node acquires the lease.
        // isLeader() reflects the AtomicBoolean updated by heartbeat(), not by
        // tryAcquireOrRenew() directly, so we verify only the return value here.
        assertTrue(result, "First node should become leader when no row exists");
    }

    /**
     * A node that already holds the lease should successfully renew it on
     * subsequent calls to tryAcquireOrRenew().
     */
    @Test
    public void testRenewOwnLease() {
        // Acquire the lease first.
        assertTrue(manager.tryAcquireOrRenew(), "Initial acquisition should succeed");

        // Renew: the UPDATE ... WHERE NODE_ID = ? branch should match.
        final boolean renewed = manager.tryAcquireOrRenew();

        assertTrue(renewed, "Node should be able to renew its own active lease");
    }

    /**
     * When the CLUSTER_LEADER row exists but its EXPIRES_AT is in the past
     * our node should claim the expired lease and become the leader.
     */
    @Test
    public void testClaimExpiredLease() {
        // Insert an expired row owned by a different node.
        final Timestamp expired = Timestamp.from(Instant.now().minusSeconds(60));
        jdbcTemplate.update(
                "INSERT INTO CLUSTER_LEADER (LOCK_KEY, NODE_ID, EXPIRES_AT) VALUES (?, ?, ?)",
                LOCK_KEY, OTHER_NODE_ID, expired);

        final boolean result = manager.tryAcquireOrRenew();

        assertTrue(result, "Node should claim an expired lease from another node");
    }

    /**
     * When the CLUSTER_LEADER row exists and its EXPIRES_AT is in the future
     * our node should NOT be able to claim it — the active leader still holds it.
     */
    @Test
    public void testDoNotClaimActiveLease() {
        // Insert an active row owned by a different node.
        final Timestamp active = Timestamp.from(Instant.now().plusSeconds(60));
        jdbcTemplate.update(
                "INSERT INTO CLUSTER_LEADER (LOCK_KEY, NODE_ID, EXPIRES_AT) VALUES (?, ?, ?)",
                LOCK_KEY, OTHER_NODE_ID, active);

        final boolean result = manager.tryAcquireOrRenew();

        assertFalse(result, "Node must not claim an active lease held by another node");
    }

    /**
     * If this node was the leader but another node claims the row (simulating a
     * lease expiry + takeover), the next heartbeat should detect it is no longer
     * the leader.
     *
     * <p>We simulate the takeover by directly updating the row's NODE_ID to a
     * different node and extending its EXPIRES_AT, then calling tryAcquireOrRenew()
     * again.
     */
    @Test
    public void testBecomeFollowerAfterLeadershipLost() {
        // Acquire the lease.
        assertTrue(manager.tryAcquireOrRenew());

        // Simulate another node stealing the lease (e.g. our node was paused,
        // lease expired, other node claimed it, and then extended it further).
        final Timestamp newExpiry = Timestamp.from(Instant.now().plusSeconds(120));
        jdbcTemplate.update(
                "UPDATE CLUSTER_LEADER SET NODE_ID = ?, EXPIRES_AT = ? WHERE LOCK_KEY = ?",
                OTHER_NODE_ID, newExpiry, LOCK_KEY);

        // Our next heartbeat should fail to renew (row no longer belongs to us)
        // and fail to claim (row is not expired) => follower.
        final boolean stillLeader = manager.tryAcquireOrRenew();

        assertFalse(stillLeader, "Node should become a follower after another node claims the lease");
    }
}
