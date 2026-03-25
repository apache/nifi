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

import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.cluster.LeaderElectionManager;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Internal JAX-RS resource providing cluster synchronisation endpoints.
 *
 * <p>All paths under {@code /internal/} require the shared-secret
 * {@code X-Registry-Internal-Auth} header set to the value configured in
 * {@code nifi.registry.cluster.node.internal.auth.token}. Requests without a
 * valid token receive {@code 403 Forbidden}.
 *
 * <p>This resource is only useful when
 * {@code nifi.registry.cluster.coordination=zookeeper} and the database is H2.
 * It is registered unconditionally so the endpoint is always present; callers
 * receive an appropriate error response when the preconditions are not met.
 */
@Component
@Path("/internal/sync")
public class InternalSyncResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(InternalSyncResource.class);

    private final NiFiRegistryProperties properties;
    private final LeaderElectionManager leaderElectionManager;
    private final DataSource dataSource;

    @Autowired
    public InternalSyncResource(final NiFiRegistryProperties properties,
            final LeaderElectionManager leaderElectionManager,
            final DataSource dataSource) {
        this.properties = properties;
        this.leaderElectionManager = leaderElectionManager;
        this.dataSource = dataSource;
    }

    /**
     * Returns a complete SQL export (DDL + DML) of the local H2 database.
     *
     * <p>Only the current cluster leader should be targeted by this endpoint.
     * If this node is not the leader, {@code 503 Service Unavailable} is
     * returned so the caller can retry against a different node.
     *
     * <p>The response body is a plain-text SQL script compatible with H2's
     * {@code RUNSCRIPT FROM} command.  It is generated via H2's built-in
     * {@code SCRIPT DROP NOPASSWORDS NOSETTINGS} statement.
     *
     * @param authToken Value of the {@code X-Registry-Internal-Auth} header.
     */
    @GET
    @Path("/export")
    @Produces(MediaType.TEXT_PLAIN)
    public Response export(
            @HeaderParam("X-Registry-Internal-Auth") final String authToken) {

        if (!isValidToken(authToken)) {
            LOGGER.warn("Rejected /internal/sync/export request: invalid or missing auth token.");
            return Response.status(Response.Status.FORBIDDEN)
                    .entity("Invalid or missing X-Registry-Internal-Auth token.")
                    .build();
        }

        if (!leaderElectionManager.isLeader()) {
            LOGGER.info("Rejecting /internal/sync/export: this node is not the current leader.");
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("This node is not the cluster leader. Retry against the leader.")
                    .build();
        }

        LOGGER.info("Generating database export script for bootstrap sync.");
        try {
            final String script = generateScript();
            LOGGER.info("Database export script generated ({} bytes).", script.length());
            return Response.ok(script, MediaType.TEXT_PLAIN).build();
        } catch (final Exception e) {
            LOGGER.error("Failed to generate database export script.", e);
            return Response.serverError()
                    .entity("Failed to generate export: " + e.getMessage())
                    .build();
        }
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private boolean isValidToken(final String token) {
        final String expected = properties.getClusterNodeInternalAuthToken();
        if (StringUtils.isBlank(expected) || token == null) {
            return false;
        }
        // Constant-time comparison to prevent timing side-channel attacks.
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                token.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Executes H2's {@code SCRIPT DROP NOPASSWORDS NOSETTINGS} and returns
     * the result as a single newline-delimited string.
     *
     * <p>This is H2-specific; the endpoint returns {@code 501 Not Implemented}
     * if the underlying database is not H2.
     */
    private String generateScript() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            final String productName = conn.getMetaData().getDatabaseProductName();
            if (!productName.contains("H2")) {
                throw new UnsupportedOperationException(
                        "DB export is only supported for H2 databases, not: " + productName);
            }

            final StringBuilder sb = new StringBuilder(1024 * 64);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SCRIPT DROP NOPASSWORDS NOSETTINGS")) {
                while (rs.next()) {
                    sb.append(rs.getString(1)).append('\n');
                }
            }
            return sb.toString();
        }
    }
}
