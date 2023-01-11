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
package org.apache.nifi.kubernetes.leader.election.command;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElector;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Runnable command for starting the Leader Election process
 */
class LeaderElectionCommand implements Runnable {
    /** Leader Lease Duration based on kube-scheduler default setting */
    private static final Duration LEASE_DURATION = Duration.ofSeconds(15);

    /** Leader Lease Renew Deadline less than Lease Duration based on kube-scheduler default setting */
    private static final Duration RENEW_DEADLINE = Duration.ofSeconds(10);

    /** Lease Retry Period based on kube-scheduler default setting */
    private static final Duration RETRY_PERIOD = Duration.ofSeconds(2);

    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionCommand.class);

    private final KubernetesClient kubernetesClient;

    private final LeaderCallbacks leaderCallbacks;

    private final String name;

    private final Lock lock;

    LeaderElectionCommand(
            final KubernetesClient kubernetesClient,
            final String namespace,
            final String name,
            final String identity,
            final Runnable onStartLeading,
            final Runnable onStopLeading,
            final Consumer<String> onNewLeader
    ) {
        this.kubernetesClient = kubernetesClient;
        this.name = Objects.requireNonNull(name, "Name required");
        this.lock = new LeaseLock(namespace, name, identity);
        this.leaderCallbacks = new LeaderCallbacks(onStartLeading, onStopLeading, onNewLeader);
    }

    @Override
    public void run() {
        logger.info("Election Name [{}] ID [{}] Participation STARTED", name, lock.identity());

        while (!Thread.currentThread().isInterrupted()) {
            runLeaderElector();
        }

        logger.info("Election Name [{}] ID [{}] Participation STOPPED", name, lock.identity());
    }

    private void runLeaderElector() {
        logger.info("Election Name [{}] ID [{}] Command STARTED", name, lock.identity());
        try {
            final LeaderElectionConfig leaderElectionConfig = getLeaderElectionConfig();
            final LeaderElector leaderElector = kubernetesClient.leaderElector().withConfig(leaderElectionConfig).build();
            leaderElector.run();
            logger.info("Election Name [{}] ID [{}] Command STOPPED", name, lock.identity());
        } catch (final RuntimeException e) {
            logger.error("Election Name [{}] ID [{}] Command FAILED", name, lock.identity(), e);
        }
    }

    private LeaderElectionConfig getLeaderElectionConfig() {
        return new LeaderElectionConfigBuilder()
                .withName(name)
                .withLeaderCallbacks(leaderCallbacks)
                .withLock(lock)
                .withLeaseDuration(LEASE_DURATION)
                .withRenewDeadline(RENEW_DEADLINE)
                .withRetryPeriod(RETRY_PERIOD)
                .build();
    }
}
