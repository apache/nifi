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

import jakarta.annotation.PostConstruct;
import org.apache.nifi.registry.cluster.LeaderElectionManager;
import org.apache.nifi.registry.cluster.NodeAddress;
import org.apache.nifi.registry.cluster.NodeRegistry;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;

/**
 * Synchronises the local H2 database from the cluster leader when a node
 * starts up with an empty database.
 *
 * <p>This bean is active only when ZooKeeper cluster coordination is enabled
 * ({@code nifi.registry.cluster.coordination=zookeeper}). On every startup
 * it checks whether the local database is empty (zero rows in the
 * {@code BUCKET} table). If so, and if this node is not the elected leader,
 * it:
 * <ol>
 *   <li>Waits up to {@value #LEADER_WAIT_TIMEOUT_MS} ms for a leader to be
 *       elected and registered in ZooKeeper.</li>
 *   <li>Calls {@code GET /nifi-registry-api/internal/sync/export} on the
 *       leader with the shared internal auth token.</li>
 *   <li>Applies the returned H2 SQL script via {@code RUNSCRIPT FROM}.</li>
 * </ol>
 *
 * <p>If the sync fails (leader unreachable, timeout, etc.) the node continues
 * with its empty database. Data will populate organically as write operations
 * are replicated to this node by {@link
 * org.apache.nifi.registry.web.security.replication.WriteReplicationFilter}.
 *
 * <p>This is intentionally H2-only. Operators using PostgreSQL or MySQL have
 * a shared database and do not need this sync.
 */
@Component
public class DataSyncBootstrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSyncBootstrapper.class);

    static final long LEADER_WAIT_TIMEOUT_MS = 30_000L;
    static final long LEADER_POLL_INTERVAL_MS = 1_000L;
    private static final Duration HTTP_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration HTTP_REQUEST_TIMEOUT = Duration.ofSeconds(120);

    private final NiFiRegistryProperties properties;
    private final DataSource dataSource;
    private final LeaderElectionManager leaderElectionManager;
    private final NodeRegistry nodeRegistry; // null in non-ZK modes

    @Autowired
    public DataSyncBootstrapper(final NiFiRegistryProperties properties,
            final DataSource dataSource,
            final LeaderElectionManager leaderElectionManager,
            @Autowired(required = false) final NodeRegistry nodeRegistry) {
        this.properties = properties;
        this.dataSource = dataSource;
        this.leaderElectionManager = leaderElectionManager;
        this.nodeRegistry = nodeRegistry;
    }

    @PostConstruct
    public void syncFromLeaderIfNeeded() {
        if (!properties.isClusterEnabled() || !properties.isZooKeeperCoordination()) {
            return;
        }
        if (nodeRegistry == null) {
            return;
        }
        if (leaderElectionManager.isLeader()) {
            LOGGER.info("DataSyncBootstrapper: this node is the leader; skipping bootstrap sync.");
            return;
        }
        if (!isLocalDbH2()) {
            LOGGER.debug("DataSyncBootstrapper: database is not H2; skipping bootstrap sync.");
            return;
        }
        if (!isLocalDbEmpty()) {
            LOGGER.info("DataSyncBootstrapper: local database has existing data; skipping bootstrap sync.");
            return;
        }

        LOGGER.info("DataSyncBootstrapper: local H2 database is empty and this node is a follower. "
                + "Initiating bootstrap sync from leader.");

        final String authToken = properties.getClusterNodeInternalAuthToken();
        if (authToken == null || authToken.isBlank()) {
            LOGGER.warn("DataSyncBootstrapper: '{}' is not configured. "
                    + "Skipping bootstrap sync to avoid repeated 403 errors. "
                    + "This node will start with an empty database and populate via write replication.",
                    "nifi.registry.cluster.node.internal.auth.token");
            return;
        }

        final NodeAddress leader = waitForLeader();
        if (leader == null) {
            LOGGER.warn("DataSyncBootstrapper: no leader elected within {}ms. "
                    + "This node will start with an empty database and populate via write replication.",
                    LEADER_WAIT_TIMEOUT_MS);
            return;
        }

        // Re-check: this node may have won the election while waiting.
        if (leaderElectionManager.isLeader()) {
            LOGGER.info("DataSyncBootstrapper: this node became leader during the wait; skipping bootstrap sync.");
            return;
        }

        LOGGER.info("DataSyncBootstrapper: syncing database snapshot from leader '{}'.", leader.getNodeId());
        try {
            final String script = fetchExportScript(leader);
            applyScript(script);
            LOGGER.info("DataSyncBootstrapper: bootstrap sync from leader '{}' completed successfully.",
                    leader.getNodeId());
        } catch (final Exception e) {
            LOGGER.error("DataSyncBootstrapper: bootstrap sync from leader '{}' failed: {}. "
                    + "This node will start with an empty database.",
                    leader.getNodeId(), e.getMessage(), e);
        }
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private boolean isLocalDbH2() {
        try (Connection conn = dataSource.getConnection()) {
            return conn.getMetaData().getDatabaseProductName().contains("H2");
        } catch (final Exception e) {
            LOGGER.debug("DataSyncBootstrapper: could not determine database type.", e);
            return false;
        }
    }

    private boolean isLocalDbEmpty() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM BUCKET")) {
            if (rs.next()) {
                return rs.getInt(1) == 0;
            }
            return true;
        } catch (final Exception e) {
            LOGGER.debug("DataSyncBootstrapper: could not count BUCKET rows; assuming non-empty.", e);
            return false;
        }
    }

    private NodeAddress waitForLeader() {
        final long deadline = System.currentTimeMillis() + LEADER_WAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            final Optional<NodeAddress> leader = nodeRegistry.getLeaderAddress();
            if (leader.isPresent()) {
                return leader.get();
            }
            try {
                Thread.sleep(LEADER_POLL_INTERVAL_MS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    private String fetchExportScript(final NodeAddress leader) throws IOException, InterruptedException {
        final String url = leader.getBaseUrl() + "/nifi-registry-api/internal/sync/export";
        LOGGER.debug("DataSyncBootstrapper: requesting export from {}.", url);

        final HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(HTTP_CONNECT_TIMEOUT)
                .build();

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("X-Registry-Internal-Auth", properties.getClusterNodeInternalAuthToken())
                .GET()
                .timeout(HTTP_REQUEST_TIMEOUT)
                .build();

        final HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        if (response.statusCode() != 200) {
            throw new IOException("Leader returned HTTP " + response.statusCode()
                    + " for export request (body: " + response.body() + ")");
        }
        return response.body();
    }

    private void applyScript(final String script) throws Exception {
        // Write to a temp file because H2's RUNSCRIPT command takes a file path.
        final Path tempFile = Files.createTempFile("registry-bootstrap-", ".sql");
        try {
            Files.writeString(tempFile, script, StandardCharsets.UTF_8);

            // Escape backslashes on Windows paths.
            final String safePath = tempFile.toAbsolutePath().toString().replace("\\", "/");
            LOGGER.debug("DataSyncBootstrapper: applying script from temp file: {}", safePath);

            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute("RUNSCRIPT FROM '" + safePath + "'");
            }
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
}
