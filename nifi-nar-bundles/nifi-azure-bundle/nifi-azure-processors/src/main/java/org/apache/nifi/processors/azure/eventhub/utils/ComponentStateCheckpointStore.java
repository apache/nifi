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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComponentStateCheckpointStore implements CheckpointStore {
    public interface State {
        Map<String, String> getState() throws IOException;
        void setState(Map<String, String> map) throws IOException;
    }
    private final State state;

    public ComponentStateCheckpointStore(State state) {
        this.state = state;
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
        try {
            Map<String, String> stateMap = state.getState();
            return Flux.fromIterable(stateMap.entrySet()).mapNotNull(
                    entry -> {
                        String[] parts = entry.getKey().split("/", 4);
                        if (parts.length < 4) {
                            return null;
                        }
                        String entryFullyQualifiedNamespace = parts[0];
                        String entryEventHubName = parts[1];
                        String entryConsumerGroup = parts[2];
                        String partitionId = parts[3];

                        if (entryFullyQualifiedNamespace.equals(fullyQualifiedNamespace)
                                && entryEventHubName.equals(eventHubName)
                                && entryConsumerGroup.equals(consumerGroup)) {
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
        try {
            Map<String, String> map = new HashMap<>(state.getState());
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

            // Note that we do not commit here because there is an implicit
            // agreement that the process session factory will provide a session
            // that is automatically committed (this is done by the processor).
            state.setState(map);
        } catch (IOException e) {
            return Mono.error(e);
        }
        return Mono.empty();
    }
}
