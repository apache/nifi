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

package org.apache.nifi.service.scylladb;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.service.cql.it.CqlConnectionInfo;
import org.apache.nifi.service.cql.it.CqlDdl;
import org.testcontainers.scylladb.ScyllaDBContainer;

/**
 * The single ScyllaDB container shared by every IT in this package that needs only a plain,
 * default-configured server (CRUD, record field types), sparing an integration run one container start
 * apiece. The ScyllaDB counterpart of {@link org.apache.nifi.service.cassandra.SharedCassandraCluster},
 * simpler for running against one fixed image rather than a version matrix. {@code ScyllaSecureVerificationIT}
 * cannot share: its authenticator and TLS settings are baked into {@code scylla.yaml} at startup.
 *
 * <p>Sharing makes keyspace separation load-bearing: each suite creates its own keyspace via
 * {@link #createKeyspace} (idempotently, since run order is not fixed) and must not touch another's; any
 * suite asserting on an exact row or counter value must be that table's only writer.
 *
 * <p>The container is deliberately never stopped - Testcontainers' Ryuk sidecar removes it on JVM exit,
 * which is the documented way to share one; an explicit {@code stop()} would pull it out from under suites
 * that have not run yet. The shared session uses {@link ScyllaDdlTimeouts#longSchemaTimeoutConfigLoader()}
 * because schema statements settle through ScyllaDB's Raft-based schema management, for which the driver's
 * 2 second defaults are not reliable.
 */
final class SharedScyllaCluster {

    private static final String IMAGE = "scylladb/scylla:2026.2";

    private static final String DATACENTER = "datacenter1";

    private static final int CQL_PORT = 9042;

    private static SharedScyllaCluster instance;

    private final String contactPoint;

    private final CqlSession session;

    private SharedScyllaCluster(final String contactPoint, final CqlSession session) {
        this.contactPoint = contactPoint;
        this.session = session;
    }

    /** The shared cluster, started on first request and reused thereafter. */
    static synchronized SharedScyllaCluster getInstance() {
        if (instance == null) {
            instance = start();
        }
        return instance;
    }

    private static SharedScyllaCluster start() {
        final ScyllaDBContainer container = ScyllaContainerLimits.apply(new ScyllaDBContainer(IMAGE));
        container.withExposedPorts(CQL_PORT);
        container.start();

        final CqlSession session = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(DATACENTER)
                .withConfigLoader(ScyllaDdlTimeouts.longSchemaTimeoutConfigLoader())
                .build();

        return new SharedScyllaCluster(
                container.getContainerIpAddress() + ":" + container.getMappedPort(CQL_PORT), session);
    }

    /**
     * Creates {@code keyspace} if absent. Idempotent because the container outlives whichever suite got
     * there first.
     */
    void createKeyspace(final String keyspace) {
        CqlDdl.executeWithRetry(session, "create keyspace if not exists " + keyspace
                + " with replication = { 'class': 'NetworkTopologyStrategy', '" + DATACENTER + "': 1};");
    }

    CqlConnectionInfo connectionInfo(final String keyspace) {
        return new CqlConnectionInfo(contactPoint, DATACENTER, keyspace, session);
    }

    CqlSession session() {
        return session;
    }
}
