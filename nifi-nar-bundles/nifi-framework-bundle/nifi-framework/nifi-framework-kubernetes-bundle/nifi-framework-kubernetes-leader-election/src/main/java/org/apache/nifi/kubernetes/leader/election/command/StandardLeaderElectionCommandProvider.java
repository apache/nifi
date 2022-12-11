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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.apache.nifi.kubernetes.client.KubernetesClientProvider;

import java.net.HttpURLConnection;
import java.util.Objects;
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
     * @return Leader Identifier or null when not found
     */
    @Override
    public String findLeader(final String name) {
        try {
            final Lease lease = kubernetesClient.leases().inNamespace(namespace).withName(name).get();
            return lease == null ? null : lease.getSpec().getHolderIdentity();
        } catch (final KubernetesClientException e) {
            if (isNotFound(e)) {
                return null;
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
}
