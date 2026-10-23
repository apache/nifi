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

package org.apache.nifi.service.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.service.cql.it.CqlConnectionInfo;
import org.apache.nifi.service.cql.it.CqlDdl;
import org.testcontainers.cassandra.CassandraContainer;

import java.util.HashMap;
import java.util.Map;

/**
 * One Cassandra container per major version, shared by every IT in this package that needs only a plain,
 * default-configured server (CRUD, record field types), sparing the integration run a container start
 * apiece for suites that otherwise boot an identical server.
 *
 * <p>Keyed by version rather than a plain singleton because {@code CassandraCrudIT} is
 * {@code @ParameterizedClass} over {@link CassandraTestVersions} while {@code CassandraRecordFieldTypeIT}
 * is pinned to one version: the pinned suite asks for {@value #PINNED_VERSION} and lands on the same
 * instance the parameterized suite uses for that version. So it is one container per version rather than
 * one per suite per version, which matters under {@code -DTEST_CASSANDRA_OLDER_VERSIONS=true}.
 *
 * <p>Sharing makes keyspace separation load-bearing: {@code testspace} (from {@code init.cql} at container
 * start) belongs to the CRUD suite, {@code type_coverage} to {@code CassandraRecordFieldTypeIT} via
 * {@link #createKeyspace}. A suite must not touch another's keyspace, and any suite asserting on an exact
 * row or counter value must be that table's only writer.
 *
 * <p>The container is deliberately never stopped - Testcontainers' Ryuk sidecar removes it on JVM exit,
 * which is the documented way to share one; an explicit {@code stop()} would pull it out from under suites
 * that have not run yet. The trade-off for a multi-version run: every version's container stays up once
 * touched, so peak memory is their sum.
 */
final class SharedCassandraCluster {

    /**
     * The version suites that do not vary by release pin to. Taken from {@link CassandraTestVersions} so
     * bumping the current major cannot leave them starting a container nothing else asks for.
     */
    static final String PINNED_VERSION = CassandraTestVersions.CURRENT_VERSION;

    private static final String DATACENTER = "datacenter1";

    private static final int CQL_PORT = 9042;

    private static final Map<String, SharedCassandraCluster> BY_VERSION = new HashMap<>();

    private final String contactPoint;

    private final CqlSession session;

    private SharedCassandraCluster(final String contactPoint, final CqlSession session) {
        this.contactPoint = contactPoint;
        this.session = session;
    }

    /**
     * The cluster for {@code version}, started on first request and reused thereafter. Synchronized because
     * the map operation spans a container start.
     */
    static synchronized SharedCassandraCluster forVersion(final String version) {
        return BY_VERSION.computeIfAbsent(version, SharedCassandraCluster::start);
    }

    private static SharedCassandraCluster start(final String version) {
        // init.cql creates the "testspace" keyspace and its tables, which the CRUD and connection
        // verification suites both expect to exist before their first test.
        final CassandraContainer container = CassandraContainerLimits.apply(new CassandraContainer("cassandra:" + version))
                .withTmpFs(Map.of("/var/lib/cassandra", "rw,size=1g"))
                .withInitScript("init.cql");
        container.withExposedPorts(CQL_PORT);
        container.start();

        final CqlSession session = CqlSession.builder()
                .addContactPoint(container.getContactPoint())
                .withLocalDatacenter(DATACENTER)
                .build();

        return new SharedCassandraCluster(
                container.getContainerIpAddress() + ":" + container.getMappedPort(CQL_PORT), session);
    }

    /**
     * Creates {@code keyspace} if absent, for a suite that owns one rather than the {@code init.cql}
     * keyspace. Idempotent because the container outlives whichever suite got there first.
     */
    void createKeyspace(final String keyspace) {
        CqlDdl.executeWithRetry(session, "create keyspace if not exists " + keyspace
                + " with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    }

    CqlConnectionInfo connectionInfo(final String keyspace) {
        return new CqlConnectionInfo(contactPoint, DATACENTER, keyspace, session);
    }

    CqlSession session() {
        return session;
    }
}
