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
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.exception.ProcessException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class ComponentStateCheckpointStore implements CheckpointStore {
    public interface State {
        StateMap getState() throws IOException;
        void setState(Map<String, String> value) throws IOException;
        boolean replaceState(StateMap oldValue, Map<String, String> newValue) throws IOException;
    }

    private final String identifier;
    private final State state;

    private static final String KEY_CHECKPOINT = "checkpoint";
    private static final String KEY_OWNERSHIP = "ownership";

    public ComponentStateCheckpointStore(String identifier, State state) {
        this.identifier = identifier;
        this.state = state;
    }

    public Flux<PartitionOwnership> listOwnership(String fullyQualifiedNamespace, String eventHubName, String consumerGroup) {
        return Flux.defer(
                () -> {
                    Map<String, String> map;
                    try {
                        map = state.getState().toMap();
                    } catch (IOException e) {
                        return Flux.error(e);
                    }
                    List<PartitionOwnership> ownerships = getEntries(map, KEY_OWNERSHIP, this::convertOwnership);
                    return Flux.fromIterable(ownerships).filter(
                            ownership ->
                                    ownership.getFullyQualifiedNamespace().equals(fullyQualifiedNamespace)
                                    && ownership.getEventHubName().equals(eventHubName)
                                    && ownership.getConsumerGroup().equals(consumerGroup)
                    );
                }
        );
    }

    public Flux<PartitionOwnership> claimOwnership(List<PartitionOwnership> requestedPartitionOwnerships) {
        return Flux.fromIterable(requestedPartitionOwnerships).flatMap(
                partitionOwnership -> {
                    final StateMap oldState;
                    try {
                        oldState = state.getState();
                    } catch (IOException e) {
                        return Mono.error(e);
                    }

                    String key = makeKey(
                            KEY_OWNERSHIP,
                            partitionOwnership.getFullyQualifiedNamespace(),
                            partitionOwnership.getEventHubName(),
                            partitionOwnership.getConsumerGroup(),
                            partitionOwnership.getPartitionId()
                    );

                    final Map<String, String> newState = new HashMap<>(oldState.toMap());
                    long timestamp = System.currentTimeMillis();
                    String eTag = identifier + "/" + timestamp;
                    newState.put(key, eTag);
                    final boolean success;
                    try {
                        success = state.replaceState(oldState, newState);
                    } catch (IOException e) {
                        return Mono.error(e);
                    }

                    if (success) {
                        return Mono.just(partitionOwnership.setETag(eTag));
                    }

                    return Mono.empty();
                }
        );
    }

    public Flux<Checkpoint> listCheckpoints(String fullyQualifiedNamespace, String eventHubName, String consumerGroup) {
        return Flux.defer(() -> {
            Map<String, String> stateMap;
            try {
                stateMap = state.getState().toMap();
            } catch (IOException e) {
                return Flux.error(e);
            }

            List<Checkpoint> checkpoints = getEntries(stateMap, KEY_CHECKPOINT, this::convertCheckpoint);
            return Flux.fromIterable(checkpoints).filter(
                    checkpoint ->
                            checkpoint.getFullyQualifiedNamespace().equals(fullyQualifiedNamespace)
                                    && checkpoint.getEventHubName().equals(eventHubName)
                                    && checkpoint.getConsumerGroup().equals(consumerGroup)
            );
        });
    }

    public Mono<Void> updateCheckpoint(Checkpoint checkpoint) {
        Map<String, String> stateMap;
        try {
            stateMap = state.getState().toMap();
        } catch (IOException e) {
            return Mono.error(e);
        }

        Map<String, String> map = new HashMap<>(stateMap);
        Long offset = checkpoint.getOffset();
        String key = makeKey(
                KEY_CHECKPOINT,
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

        try {
            state.setState(map);
        } catch (IOException e) {
            return Mono.error(e);
        }

        // Note that we do not commit here because there is an implicit
        // agreement that the process session factory will provide a session
        // that is automatically committed (this is done by the processor).
        return Mono.empty();
    }

    private Checkpoint convertCheckpoint(PartitionContext context, String value) {
        return new Checkpoint()
                .setFullyQualifiedNamespace(context.getFullyQualifiedNamespace())
                .setEventHubName(context.getEventHubName())
                .setConsumerGroup(context.getConsumerGroup())
                .setPartitionId(context.getPartitionId())
                .setOffset(Long.parseLong(value));
    }

    private PartitionOwnership convertOwnership(PartitionContext context, String value) {
        final String[] parts = value.split("/", 2);
        if (parts.length != 2) {
            throw new ProcessException(String.format("Invalid ownership value: %s", value));
        }
        return new PartitionOwnership()
                .setFullyQualifiedNamespace(context.getFullyQualifiedNamespace())
                .setEventHubName(context.getEventHubName())
                .setConsumerGroup(context.getConsumerGroup())
                .setPartitionId(context.getPartitionId())
                .setETag(value)
                .setOwnerId(parts[0])
                .setLastModifiedTime(Long.parseLong(parts[1])
                );
    }

    private <T> List<T> getEntries(final Map<String, String> map, String kind, BiFunction<PartitionContext, String, T> extract) throws ProcessException {
        final List<T> result = new ArrayList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            final String key = entry.getKey();
            final String[] parts = key.split("/", 5);
            if (parts.length != 5) {
                throw new ProcessException(
                        String.format("Invalid %s key: %s", kind, entry.getKey())
                );
            }
            if (!parts[0].equals(kind)) {
                continue;
            }
            final String fullyQualifiedNamespace = parts[1];
            final String eventHubName = parts[2];
            final String consumerGroup = parts[3];
            final String partitionId = parts[4];
            PartitionContext partitionContext = new PartitionContext(
                    fullyQualifiedNamespace,
                    eventHubName,
                    consumerGroup,
                    partitionId
            );
            final T item = extract.apply(partitionContext, entry.getValue());
            result.add(item);
        }
        return result;
    }

    private String makeKey(String kind, String fullyQualifiedNamespace, String eventHubName, String consumerGroup, String partitionId) {
        return String.format(
                "%s/%s/%s/%s/%s",
                kind,
                fullyQualifiedNamespace,
                eventHubName,
                consumerGroup,
                partitionId
        );
    }
}
