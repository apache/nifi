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

import org.apache.nifi.service.cql.it.DockerUtils;
import org.testcontainers.scylladb.ScyllaDBContainer;

import java.util.Map;

/**
 * The resource ceiling and Seastar tuning every ScyllaDB container in this package runs under, so no
 * container in an integration run can size itself against the whole host. The ScyllaDB counterpart of
 * {@code CassandraContainerLimits}, with more to say because ScyllaDB's Seastar runtime does not read
 * cgroup limits on its own: {@code --smp}, {@code --memory}, {@code --reserve-memory}, and the commitlog
 * sizes all have to be passed explicitly, and {@code --developer-mode 1} switches off the startup checks
 * that would otherwise fail on a tmpfs data directory and an unprivileged container.
 *
 * <p><strong>Host requirement:</strong> ScyllaDB's Seastar reactor needs a large pool of asynchronous I/O
 * contexts, and it checks the kernel's {@code fs.aio-max-nr} at startup. If the limit is too low the
 * container never finishes booting - it restarts in a loop logging
 * {@code Could not initialize seastar: ... Your system does not satisfy minimum AIO requirements}, and the
 * Testcontainers wait strategy eventually times out with
 * {@code Timed out waiting for log output matching '.*initialization completed..*'}. This is a host tuning
 * knob, not something the test can set. {@code fs.aio-max-nr} is a system-wide ceiling and
 * {@code fs.aio-nr} counts what is already reserved, so any other ScyllaDB process on the machine (a
 * long-lived local container, another integration run) eats into the same budget; size the limit for the
 * peak number of concurrent instances, not one. As a rule of thumb allow ~1.05M per instance:
 * {@code sudo sysctl -w fs.aio-max-nr=3145728} covers this suite plus a couple of other Scylla containers,
 * and a matching line in {@code /etc/sysctl.d/} makes it survive a reboot. Cassandra is unaffected - it
 * uses epoll, not AIO.
 */
final class ScyllaContainerLimits {

    private static final long CPUS = 2;

    private static final long MEMORY_GB = 2;

    private static final String SCYLLA_MEMORY = "750M";

    private static final String SCYLLA_RESERVE_MEMORY = "256M";

    private static final String COMMITLOG_TOTAL_SPACE_MB = "64";

    private static final String SCHEMA_COMMITLOG_SEGMENT_SIZE_MB = "16";

    private static final String DATA_DIRECTORY = "/var/lib/scylla";

    private static final String DATA_TMPFS_OPTIONS = "rw,size=512m,mode=1777";

    private ScyllaContainerLimits() {
    }

    static ScyllaDBContainer apply(final ScyllaDBContainer container) {
        return container
                .withTmpFs(Map.of(DATA_DIRECTORY, DATA_TMPFS_OPTIONS))
                .withCommand("--smp", "1", "--memory", SCYLLA_MEMORY, "--reserve-memory", SCYLLA_RESERVE_MEMORY,
                        "--commitlog-total-space-in-mb", COMMITLOG_TOTAL_SPACE_MB,
                        "--schema-commitlog-segment-size-in-mb", SCHEMA_COMMITLOG_SEGMENT_SIZE_MB,
                        "--developer-mode", "1", "--overprovisioned", "1")
                .withCreateContainerCmdModifier(DockerUtils.createMemoryLimits(CPUS, MEMORY_GB));
    }
}
