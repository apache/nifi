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
package org.apache.nifi.processors.azure.eventhub.utils;

import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComponentStateCheckpointStore implements CheckpointStore {
    private final ProcessSessionFactory processSessionFactory;

    public ComponentStateCheckpointStore(ProcessSessionFactory processSessionFactory) {
        this.processSessionFactory = processSessionFactory;
    }

    @Override
    public Flux<PartitionOwnership> listOwnership(String fullyQualifiedNamespace, String eventHubName, String consumerGroup) {
        return listCheckpoints(fullyQualifiedNamespace, eventHubName, consumerGroup).map(
                checkpoint -> new PartitionOwnership()
                        .setFullyQualifiedNamespace(fullyQualifiedNamespace)
                        .setEventHubName(eventHubName)
                        .setConsumerGroup(consumerGroup)
        );
    }

    @Override
    public Flux<PartitionOwnership> claimOwnership(List<PartitionOwnership> requestedPartitionOwnerships) {
        return Flux.fromIterable(requestedPartitionOwnerships);
    }

    @Override
    public Flux<Checkpoint> listCheckpoints(String fullyQualifiedNamespace, String eventHubName, String consumerGroup) {
        final ProcessSession session = processSessionFactory.createSession();
        try {
            StateMap stateMap = session.getState(Scope.CLUSTER);
            return Flux.fromIterable(stateMap.toMap().entrySet()).mapNotNull(
                    entry -> {
                        String[] parts = entry.getKey().split("/", 4);
                        if (parts.length < 4) {
                            return null;
                        }
                        String entryFullyQualifiedNamespace = parts[0];
                        String entryEventHubName = parts[1];
                        String entryConsumerGroup = parts[2];
                        String partitionId = parts[3];

                        if (entryFullyQualifiedNamespace.equals(fullyQualifiedNamespace) &&
                                entryEventHubName.equals(eventHubName) &&
                                entryConsumerGroup.equals(consumerGroup)) {
                            return null;
                        }

                        return new Checkpoint()
                                .setFullyQualifiedNamespace(fullyQualifiedNamespace)
                                .setEventHubName(eventHubName)
                                .setConsumerGroup(consumerGroup)
                                .setPartitionId(partitionId)
                                .setOffset(Long.parseLong(entry.getValue()));
                    }
            );
        } catch (IOException e) {
            return Flux.error(e);
        }
    }

    @Override
    public Mono<Void> updateCheckpoint(Checkpoint checkpoint) {
        final ProcessSession session = processSessionFactory.createSession();
        try {
            StateMap stateMap = session.getState(Scope.CLUSTER);
            Map<String, String> map = new HashMap<>(stateMap.toMap());
            Long offset = checkpoint.getOffset();
            String key = String.format(
                    "%s/%s/%s/%s",
                    checkpoint.getFullyQualifiedNamespace(),
                    checkpoint.getEventHubName(),
                    checkpoint.getConsumerGroup(),
                    checkpoint.getPartitionId()
            );
            if (offset == null) {
                map.remove(key);
            } else {
                map.put(key, offset.toString());
            }
            session.setState(map, Scope.CLUSTER);
            session.commitAsync();
        } catch (IOException e) {
            return Mono.error(e);
        }
        return Mono.empty();
    }
}
