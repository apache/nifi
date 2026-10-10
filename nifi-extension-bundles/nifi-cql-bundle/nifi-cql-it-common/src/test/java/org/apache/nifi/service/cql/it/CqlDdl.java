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
package org.apache.nifi.service.cql.it;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;

/**
 * Executes schema-modifying statements against a real container, retrying the two failures that say nothing
 * about whether the statement actually ran.
 *
 * <p>A {@link DriverTimeoutException} means the client stopped waiting for schema agreement, not that the
 * server rejected the statement - ScyllaDB's Raft-based schema management (and, less often, Cassandra's own
 * agreement protocol) can take longer than the configured request timeout on an otherwise healthy cluster. A
 * {@link HeartbeatException} is the same story one layer down: the connection carrying the statement died
 * mid-flight (observed on ScyllaDB under the load of a full-reactor IT run). Either way the statement may
 * still land server-side moments later.
 *
 * <p><strong>Every statement passed here must be idempotent</strong> - an "if not exists" form - since a
 * retry after one of those false negatives must not fail with "already exists".
 *
 * <p>This lives in the shared IT module rather than beside any one suite because all three former copies of
 * this loop wanted the same behaviour, and had drifted: two caught only the timeout, so they would flake on
 * exactly the heartbeat failure the third already survived.
 */
public final class CqlDdl {

    private static final int MAX_ATTEMPTS = 3;

    private CqlDdl() {
    }

    public static void executeWithRetry(final CqlSession session, final String cql) {
        RuntimeException lastFailure = null;

        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                session.execute(cql);
                return;
            } catch (final DriverTimeoutException | HeartbeatException e) {
                lastFailure = e;
            }
        }

        throw lastFailure;
    }
}
