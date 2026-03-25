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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.net.InetAddress;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;

/**
 * Database-backed implementation of {@link LeaderElectionManager}.
 *
 * <p>Uses a single row in the {@code CLUSTER_LEADER} table as a TTL-based
 * distributed lock. Each cluster node tries every {@value #HEARTBEAT_INTERVAL_SECONDS}
 * seconds to either renew its own lease or claim an expired one. A node is
 * considered the leader when it holds a lease whose {@code EXPIRES_AT} is in
 * the future.
 *
 * <p>Lease duration: {@value #LEASE_DURATION_SECONDS} seconds.
 * Heartbeat interval: {@value #HEARTBEAT_INTERVAL_SECONDS} seconds.
 *
 * <p>This bean is always created by Spring but only activates the background
 * heartbeat thread when {@code nifi.registry.cluster.enabled=true}. In
 * standalone mode {@link #isLeader()} always returns {@code false}.
 */
public class DatabaseLeaderElectionManager implements LeaderElectionManager, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseLeaderElectionManager.class);

    static final int LEASE_DURATION_SECONDS = 30;
    static final int HEARTBEAT_INTERVAL_SECONDS = 10;
    private static final String LOCK_KEY = "LEADER";

    private final JdbcTemplate jdbcTemplate;
    private final NiFiRegistryProperties properties;
    private final String nodeId;
    private final AtomicBoolean currentLeader = new AtomicBoolean(false);

    private ScheduledExecutorService scheduler;

    public DatabaseLeaderElectionManager(final DataSource dataSource,
            final NiFiRegistryProperties properties) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.properties = properties;
        this.nodeId = resolveNodeId(properties);
    }

    public void start() {
        if (!properties.isClusterEnabled()) {
            LOGGER.info("Cluster mode is disabled; DatabaseLeaderElectionManager will not start.");
            return;
        }

        LOGGER.info("Starting DatabaseLeaderElectionManager for node '{}' with lease {}s / heartbeat {}s.",
                nodeId, LEASE_DURATION_SECONDS, HEARTBEAT_INTERVAL_SECONDS);

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "LeaderElectionHeartbeat");
            t.setDaemon(true);
            return t;
        });

        // Run first heartbeat immediately, then every HEARTBEAT_INTERVAL_SECONDS.
        scheduler.scheduleWithFixedDelay(
                this::heartbeat,
                0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void destroy() {
        if (scheduler != null) {
            LOGGER.info("Shutting down DatabaseLeaderElectionManager.");
            scheduler.shutdownNow();
        }
    }

    @Override
    public boolean isLeader() {
        return currentLeader.get();
    }

    // -------------------------------------------------------------------------
    // Internal heartbeat
    // -------------------------------------------------------------------------

    private void heartbeat() {
        try {
            final boolean leader = tryAcquireOrRenew();
            final boolean wasLeader = currentLeader.getAndSet(leader);
            if (leader && !wasLeader) {
                LOGGER.info("Node '{}' became the cluster leader.", nodeId);
            } else if (!leader && wasLeader) {
                LOGGER.warn("Node '{}' lost the cluster leader lease.", nodeId);
            }
        } catch (final Exception e) {
            LOGGER.error("Unexpected error during leader heartbeat", e);
            currentLeader.set(false);
        }
    }

    /**
     * Tries to acquire or renew the leader lease.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Try to renew our own lease (UPDATE ... WHERE NODE_ID = self).</li>
     *   <li>If that fails, try to claim an expired lease (UPDATE ... WHERE EXPIRES_AT &lt; now).</li>
     *   <li>If that fails, try to INSERT the first-ever row.</li>
     *   <li>Otherwise remain a follower.</li>
     * </ol>
     */
    boolean tryAcquireOrRenew() {
        final Timestamp expires = Timestamp.from(Instant.now().plusSeconds(LEASE_DURATION_SECONDS));
        final Timestamp now = Timestamp.from(Instant.now());

        // 1. Renew existing lease if we're already the leader.
        final int renewed = jdbcTemplate.update(
                "UPDATE CLUSTER_LEADER SET EXPIRES_AT = ? WHERE LOCK_KEY = ? AND NODE_ID = ?",
                expires, LOCK_KEY, nodeId);
        if (renewed > 0) {
            return true;
        }

        // 2. Claim the lease if it has expired (another node died or first run).
        final int claimed = jdbcTemplate.update(
                "UPDATE CLUSTER_LEADER SET NODE_ID = ?, EXPIRES_AT = ? WHERE LOCK_KEY = ? AND EXPIRES_AT < ?",
                nodeId, expires, LOCK_KEY, now);
        if (claimed > 0) {
            return true;
        }

        // 3. No row exists yet — first node to start. INSERT to create it.
        try {
            jdbcTemplate.update(
                    "INSERT INTO CLUSTER_LEADER (LOCK_KEY, NODE_ID, EXPIRES_AT) VALUES (?, ?, ?)",
                    LOCK_KEY, nodeId, expires);
            return true;
        } catch (final Exception e) {
            // Another node raced and inserted first, or the active leader holds the lease.
            LOGGER.debug("No row exists yet — first node to start. INSERT to create it. Could not acquire leader lease for node '{}': {}", nodeId, e.getMessage());
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String resolveNodeId(final NiFiRegistryProperties props) {
        final String configured = props.getClusterNodeIdentifier();
        if (!StringUtils.isBlank(configured)) {
            return configured;
        }

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final Exception e) {
            LOGGER.warn("Unable to resolve hostname for node identifier; using 'unknown'", e);
            return "unknown";
        }
    }
}
