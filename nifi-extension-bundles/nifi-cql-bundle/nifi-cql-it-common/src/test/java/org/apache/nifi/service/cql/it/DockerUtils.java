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

import com.github.dockerjava.api.command.CreateContainerCmd;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Docker-level container tuning that Testcontainers does not expose directly.
 */
public final class DockerUtils {

    private DockerUtils() {
    }

    /**
     * Caps what a test container may take from the host, so a database container cannot size itself against
     * all of the machine's memory and cores.
     *
     * @param cpus     CPU cores the container may use
     * @param memoryGb memory ceiling in gibibytes
     * @return a modifier to hand to {@code withCreateContainerCmdModifier}
     */
    public static Consumer<CreateContainerCmd> createMemoryLimits(final long cpus, final long memoryGb) {
        final long memoryLimitAsBytes = memoryGb * (1024L * 1024L * 1024L);
        final long nanoCpus = cpus * 1_000_000_000L;
        return cmd ->
                Objects.requireNonNull(cmd.getHostConfig(), "HostConfig unexpectedly null")
                        .withMemory(memoryLimitAsBytes)
                        .withNanoCPUs(nanoCpus);
    }
}
