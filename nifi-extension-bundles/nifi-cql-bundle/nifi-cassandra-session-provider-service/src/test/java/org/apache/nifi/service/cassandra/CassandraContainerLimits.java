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

import org.apache.nifi.service.cql.it.DockerUtils;
import org.testcontainers.cassandra.CassandraContainer;

/**
 * The resource ceiling every Cassandra container in this package runs under, so no container in an
 * integration run can size itself against the whole host. This is the Cassandra counterpart to ScyllaDB's
 * {@code ScyllaContainerLimits}, and simpler than it: Cassandra needs no equivalent of ScyllaDB's Seastar
 * memory and commitlog flags, since the JVM reads the cgroup limit itself.
 *
 * <p>The CPU allowance is what this module's integration time is sensitive to, not the memory ceiling.
 * Capping containers at 2 cores cost the module roughly 30 seconds a run, most of it in the SSL/auth
 * verification suite, which pays Cassandra's CPU-bound startup for a specially-configured container. Measured
 * on that suite back when it booted a container per scenario: 57.1s at 2 cores, 36.7s at {@value #CPUS},
 * against 35.7s with no cap at all - so this allowance gives back essentially all of it. Memory turned out
 * not to matter over the range tried: at {@value #CPUS} cores the same suite ran 36.8s with this
 * {@value #MEMORY_GB} GB ceiling and 36.7s with 3 GB, so the tighter ceiling is kept.
 *
 * <p>{@value #CPUS} is an allowance rather than a reservation - it bounds a container that would otherwise
 * see every core on the host, and containers here start one at a time.
 */
final class CassandraContainerLimits {

    private static final long CPUS = 3;

    private static final long MEMORY_GB = 2;

    private CassandraContainerLimits() {
    }

    /**
     * Caps the container's host resources.
     *
     * @param container the container to constrain, before it is started
     * @return the same container, for chaining
     */
    static CassandraContainer apply(final CassandraContainer container) {
        return container.withCreateContainerCmdModifier(DockerUtils.createMemoryLimits(CPUS, MEMORY_GB));
    }
}
