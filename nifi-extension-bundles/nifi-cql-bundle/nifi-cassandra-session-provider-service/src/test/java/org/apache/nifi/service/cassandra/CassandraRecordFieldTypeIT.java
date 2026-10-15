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

import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.it.AbstractCqlRecordFieldTypeIT;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Type-coverage tests from {@link AbstractCqlRecordFieldTypeIT} run against a real Cassandra container.
 * Single fixed version rather than a parameterized matrix, since the behavior under test (record field type
 * &lt;-&gt; CQL type conversion) doesn't vary across Cassandra major versions the way connection/auth
 * handling can.
 *
 * <p>The container itself belongs to {@link SharedCassandraCluster}, not to this class: pinning to
 * {@link SharedCassandraCluster#PINNED_VERSION} means this suite lands on the same instance the
 * version-parameterized suites use for that version rather than starting one of its own. It owns the
 * {@code type_coverage} keyspace, in which it creates a table per type under test.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CassandraRecordFieldTypeIT extends AbstractCqlRecordFieldTypeIT {

    private static final String KEYSPACE = "type_coverage";

    @BeforeAll
    void attachToCluster() throws Exception {
        final SharedCassandraCluster cluster = SharedCassandraCluster.forVersion(SharedCassandraCluster.PINNED_VERSION);
        cluster.createKeyspace(KEYSPACE);

        initializeSessionProvider(cluster.connectionInfo(KEYSPACE));
    }

    @Override
    protected CQLExecutionService newSessionProvider() {
        return new CassandraCQLExecutionService();
    }
}
