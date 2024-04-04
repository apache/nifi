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

import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import io.fabric8.kubernetes.api.model.coordination.v1.LeaseSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.apache.nifi.kubernetes.client.KubernetesClientProvider;

import java.net.HttpURLConnection;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Standard implementation of Leader Election Command Provider with configurable namespace property
 */
public class StandardLeaderElectionCommandProvider implements LeaderElectionCommandProvider {
    private final KubernetesClient kubernetesClient;

    private final String namespace;

    public StandardLeaderElectionCommandProvider(final KubernetesClientProvider kubernetesClientProvider, final String namespace) {
        this.kubernetesClient = Objects.requireNonNull(kubernetesClientProvider).getKubernetesClient();
        this.namespace = Objects.requireNonNull(namespace);
    }

    /**
     * Get Leader Election Command with configured namespace and client provider
     *
     * @param name Election Name
     * @param identity Election Participant Identity
     * @param onStartLeading Callback run when elected as leader
     * @param onStopLeading Callback run when no longer elected as leader
     * @param onNewLeader Callback run with identification of new leader
     * @return Leader Election Command
     */
    @Override
    public Runnable getCommand(
            final String name,
            final String identity,
            final Runnable onStartLeading,
            final Runnable onStopLeading,
            final Consumer<String> onNewLeader
    ) {
        return new LeaderElectionCommand(
                kubernetesClient,
                namespace,
                name,
                identity,
                onStartLeading,
                onStopLeading,
                onNewLeader
        );
    }

    /**
     * Find Leader Identifier for specified Election Name
     *
     * @param name Election Name
     * @return Leader Identifier or empty when not found or lease expired
     */
    @Override
    public Optional<String> findLeader(final String name) {
        try {
            final Lease lease = kubernetesClient.leases().inNamespace(namespace).withName(name).get();
            final String currentHolderIdentity = getCurrentHolderIdentity(lease);
            return Optional.ofNullable(currentHolderIdentity);
        } catch (final KubernetesClientException e) {
            if (isNotFound(e)) {
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

    /**
     * Close Kubernetes Client
     */
    @Override
    public void close() {
        kubernetesClient.close();
    }

    private boolean isNotFound(final KubernetesClientException e) {
        return HttpURLConnection.HTTP_NOT_FOUND == e.getCode();
    }

    private String getCurrentHolderIdentity(final Lease lease) {
        final String holderIdentity;

        if (lease == null) {
            holderIdentity = null;
        } else {
            final LeaseSpec spec = lease.getSpec();
            final ZonedDateTime expiration = getExpiration(spec);
            final ZonedDateTime now = ZonedDateTime.now();
            if (now.isAfter(expiration)) {
                holderIdentity = null;
            } else {
                holderIdentity = spec.getHolderIdentity();
            }
        }

        return holderIdentity;
    }

    private ZonedDateTime getExpiration(final LeaseSpec leaseSpec) {
        final ZonedDateTime renewTime = leaseSpec.getRenewTime();
        final Integer leaseDuration = leaseSpec.getLeaseDurationSeconds();
        return renewTime.plusSeconds(leaseDuration);
    }
}
