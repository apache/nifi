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
package org.apache.nifi.atlas.hook;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.nifi.atlas.AtlasUtils;
import org.apache.nifi.atlas.NiFiAtlasClient;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.apache.atlas.notification.hook.HookNotification.HookNotificationType.ENTITY_CREATE;
import static org.apache.atlas.notification.hook.HookNotification.HookNotificationType.ENTITY_PARTIAL_UPDATE;
import static org.apache.nifi.atlas.AtlasUtils.toTypedQualifiedName;
import static org.apache.nifi.atlas.hook.NiFiAtlasHook.NIFI_USER;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_GUID;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_TYPENAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

/**
 * This class implements Atlas hook notification message deduplication mechanism.
 * Separated from {@link NiFiAtlasHook} for better testability.
 */
class NotificationSender {

    private static final Logger logger = LoggerFactory.getLogger(NotificationSender.class);

    private NiFiAtlasClient atlasClient;

    /**
     * An index to resolve a qualifiedName from a GUID.
     */
    private final Map<String, String> guidToQualifiedName;
    /**
     * An index to resolve a Referenceable from a typeName::qualifiedName.
     */
    private final Map<String, Referenceable> typedQualifiedNameToRef;

    private static <K, V> Map<K, V> createCache(final int maxSize) {
        return new LinkedHashMap<K, V>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > maxSize;
            }
        };
    }

    NotificationSender() {
        final int qualifiedNameCacheSize = 10_000;
        this.guidToQualifiedName = createCache(qualifiedNameCacheSize);

        final int dataSetRefCacheSize = 1_000;
        this.typedQualifiedNameToRef = createCache(dataSetRefCacheSize);
    }

    private class Metrics {
        final long startedAt = System.currentTimeMillis();

        /**
         * The total number of messages passed to commitMessages.
         */
        int totalMessages;
        /**
         * The number of CreateEntityRequest messages before de-duplication.
         */
        int createMessages;
        /**
         * The number of unique CreateEntityRequest messages for 'nifi_flow_path'.
         */
        int uniqueFlowPathCreates;
        /**
         * The number of unique CreateEntityRequest messages except 'nifi_flow_path'.
         */
        int uniqueOtherCreates;
        int partialNiFiFlowPathUpdates;
        int uniquePartialNiFiFlowPathUpdates;
        int otherMessages;
        int flowPathSearched;
        int dataSetSearched;
        int dataSetCacheHit;

        private String toLogString(String message) {
            return String.format("%s, %d ms passed, totalMessages=%d," +
                            " createMessages=%d, uniqueFlowPathCreates=%d, uniqueOtherCreates=%d," +
                            " partialNiFiFlowPathUpdates=%d, uniquePartialNiFiFlowPathUpdates=%d, otherMessage=%d," +
                            " flowPathSearched=%d, dataSetSearched=%d, dataSetCacheHit=%s," +
                            " guidToQualifiedName.size=%d, typedQualifiedNameToRef.size=%d",
                    message, System.currentTimeMillis() - startedAt, totalMessages,
                    createMessages, uniqueFlowPathCreates, uniqueOtherCreates,
                    partialNiFiFlowPathUpdates, uniquePartialNiFiFlowPathUpdates, otherMessages,
                    flowPathSearched, dataSetSearched, dataSetCacheHit,
                    guidToQualifiedName.size(), typedQualifiedNameToRef.size());
        }
    }

    void setAtlasClient(NiFiAtlasClient atlasClient) {
        this.atlasClient = atlasClient;
    }

    private Predicate<Referenceable> distinctReferenceable() {
        final Set<String> keys = new HashSet<>();
        return r -> {
            final String key = AtlasUtils.toTypedQualifiedName(r.getTypeName(), (String) r.get(ATTR_QUALIFIED_NAME));
            return keys.add(key);
        };
    }

    private <K, V> List<V> safeGet(Map<K, List<V>> map, K key) {
        return map.computeIfAbsent(key, k -> Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    private void mergeRefs(Referenceable r1, Referenceable r2) {
        r1.set(ATTR_INPUTS, mergeRefs((Collection<Referenceable>) r1.get(ATTR_INPUTS), (Collection<Referenceable>) r2.get(ATTR_INPUTS)));
        r1.set(ATTR_OUTPUTS, mergeRefs((Collection<Referenceable>) r1.get(ATTR_OUTPUTS), (Collection<Referenceable>) r2.get(ATTR_OUTPUTS)));
    }

    private Collection<Referenceable> mergeRefs(Collection<Referenceable> r1, Collection<Referenceable> r2) {
        final boolean isR1Empty = r1 == null || r1.isEmpty();
        final boolean isR2Empty = r2 == null || r2.isEmpty();

        if (isR1Empty) {
            // r2 may or may not have entities, don't have to merge r1.
            return r2;
        } else if (isR2Empty) {
            // r1 has some entities, don't have to merge r2.
            return r1;
        }

        // If both have entities, then need to be merged.
        return Stream.concat(r1.stream(), r2.stream()).filter(distinctReferenceable()).collect(Collectors.toList());
    }

    /**
     * <p>Send hook notification messages.
     * In order to notify relationships between 'nifi_flow_path' and its inputs/outputs, this method sends messages in following order:</p>
     * <ol>
     *     <li>As a a single {@link org.apache.atlas.notification.hook.HookNotification.EntityCreateRequest} message:
     *         <ul>
     *             <li>New entities except 'nifi_flow_path', including DataSets such as 'nifi_queue', 'kafka_topic' or 'hive_table' ... etc,
     *             so that 'nifi_flow_path' can refer</li>
     *             <li>New 'nifi_flow_path' entities, entities order is guaranteed in a single message</li>
     *         </ul>
     *     </li>
     *     <li>Update 'nifi_flow_path' messages, before notifying update messages, this method fetches existing 'nifi_flow_path' entity
     *     to merge new inputs/outputs element with existing ones, so that existing ones will not be removed.</li>
     *     <li>Other messages except</li>
     * </ol>
     * <p>Messages having the same type and qualified name will be de-duplicated before being sent.</p>
     * @param messages list of messages to be sent
     * @param notifier responsible for sending notification messages, its accept method can be called multiple times
     */
    void send(final List<HookNotification.HookNotificationMessage> messages, final Consumer<List<HookNotification.HookNotificationMessage>> notifier) {
        final Metrics metrics = new Metrics();
        try {
            metrics.totalMessages = messages.size();

            final Map<Boolean, List<HookNotification.HookNotificationMessage>> createAndOthers = messages.stream().collect(groupingBy(msg -> ENTITY_CREATE.equals(msg.getType())));

            final List<HookNotification.HookNotificationMessage> creates = safeGet(createAndOthers, true);
            metrics.createMessages = creates.size();

            final Map<Boolean, List<Referenceable>> newFlowPathsAndOtherEntities = creates.stream()
                    .flatMap(msg -> ((HookNotification.EntityCreateRequest) msg).getEntities().stream())
                    .collect(groupingBy(ref -> TYPE_NIFI_FLOW_PATH.equals(ref.typeName)));

            // Deduplicate same entity creation messages.
            final List<Referenceable> newEntitiesExceptFlowPaths = safeGet(newFlowPathsAndOtherEntities, false)
                    .stream().filter(distinctReferenceable()).collect(Collectors.toList());

            // Deduplicate same flow paths and also merge inputs and outputs
            final Collection<Referenceable> newFlowPaths = safeGet(newFlowPathsAndOtherEntities, true).stream()
                    .collect(toMap(ref -> ref.get(ATTR_QUALIFIED_NAME), ref -> ref, (r1, r2) -> {
                        // Merge inputs and outputs.
                        mergeRefs(r1, r2);
                        return r1;
                    })).values();
            metrics.uniqueFlowPathCreates = newFlowPaths.size();
            metrics.uniqueOtherCreates = newEntitiesExceptFlowPaths.size();


            // 1-1. Notify new entities except 'nifi_flow_path'
            // 1-2. Notify new 'nifi_flow_path'
            List<Referenceable> newEntities = new ArrayList<>();
            newEntities.addAll(newEntitiesExceptFlowPaths);
            newEntities.addAll(newFlowPaths);
            if (!newEntities.isEmpty()) {
                notifier.accept(Collections.singletonList(new HookNotification.EntityCreateRequest(NIFI_USER, newEntities)));
            }

            final Map<Boolean, List<HookNotification.HookNotificationMessage>> partialNiFiFlowPathUpdateAndOthers
                    = safeGet(createAndOthers, false).stream().collect(groupingBy(msg
                    -> ENTITY_PARTIAL_UPDATE.equals(msg.getType())
                    && TYPE_NIFI_FLOW_PATH.equals(((HookNotification.EntityPartialUpdateRequest)msg).getTypeName())
                    && ATTR_QUALIFIED_NAME.equals(((HookNotification.EntityPartialUpdateRequest)msg).getAttribute())
            ));


            // These updates are made against existing flow path entities.
            final List<HookNotification.HookNotificationMessage> partialNiFiFlowPathUpdates = safeGet(partialNiFiFlowPathUpdateAndOthers, true);
            final List<HookNotification.HookNotificationMessage> otherMessages = safeGet(partialNiFiFlowPathUpdateAndOthers, false);
            metrics.partialNiFiFlowPathUpdates = partialNiFiFlowPathUpdates.size();
            metrics.otherMessages = otherMessages.size();


            // 2. Notify de-duplicated 'nifi_flow_path' updates
            final List<HookNotification.HookNotificationMessage> deduplicatedMessages = partialNiFiFlowPathUpdates.stream().map(msg -> (HookNotification.EntityPartialUpdateRequest) msg)
                    // Group by nifi_flow_path qualifiedName value.
                    .collect(groupingBy(HookNotification.EntityPartialUpdateRequest::getAttributeValue)).entrySet().stream()
                    .map(entry -> {
                        final String flowPathQualifiedName = entry.getKey();
                        final Map<String, Referenceable> distinctInputs;
                        final Map<String, Referenceable> distinctOutputs;
                        final String flowPathGuid;
                        try {
                            // Fetch existing nifi_flow_path and its inputs/ouputs.
                            metrics.flowPathSearched++;
                            final AtlasEntity.AtlasEntityWithExtInfo flowPathExt
                                    = atlasClient.searchEntityDef(new AtlasObjectId(TYPE_NIFI_FLOW_PATH, ATTR_QUALIFIED_NAME, flowPathQualifiedName));
                            final AtlasEntity flowPathEntity = flowPathExt.getEntity();
                            flowPathGuid = flowPathEntity.getGuid();
                            distinctInputs = toReferenceables(flowPathEntity.getAttribute(ATTR_INPUTS), metrics);
                            distinctOutputs = toReferenceables(flowPathEntity.getAttribute(ATTR_OUTPUTS), metrics);

                        } catch (AtlasServiceException e) {
                            if (ClientResponse.Status.NOT_FOUND.equals(e.getStatus())) {
                                logger.debug("nifi_flow_path was not found for qualifiedName {}", flowPathQualifiedName);
                            } else {
                                logger.warn("Failed to retrieve nifi_flow_path with qualifiedName {} due to {}", flowPathQualifiedName, e, e);
                            }
                            return null;
                        }

                        // Merge all inputs and outputs for this nifi_flow_path.
                        for (HookNotification.EntityPartialUpdateRequest msg : entry.getValue()) {
                            fromReferenceable(msg.getEntity().get(ATTR_INPUTS), metrics)
                                    .entrySet().stream().filter(ref -> !distinctInputs.containsKey(ref.getKey()))
                                    .forEach(ref -> distinctInputs.put(ref.getKey(), ref.getValue()));

                            fromReferenceable(msg.getEntity().get(ATTR_OUTPUTS), metrics)
                                    .entrySet().stream().filter(ref -> !distinctOutputs.containsKey(ref.getKey()))
                                    .forEach(ref -> distinctOutputs.put(ref.getKey(), ref.getValue()));
                        }

                        // Consolidate messages into one.
                        final Referenceable flowPathRef = new Referenceable(flowPathGuid, TYPE_NIFI_FLOW_PATH, null);
                        // NOTE: distinctInputs.values() returns HashMap$Values, which causes following error. To avoid that, wrap with ArrayList:
                        // org.json4s.package$MappingException: Can't find ScalaSig for class org.apache.atlas.typesystem.Referenceable
                        flowPathRef.set(ATTR_INPUTS, new ArrayList<>(distinctInputs.values()));
                        flowPathRef.set(ATTR_OUTPUTS, new ArrayList<>(distinctOutputs.values()));
                        return new HookNotification.EntityPartialUpdateRequest(NIFI_USER, TYPE_NIFI_FLOW_PATH,
                                ATTR_QUALIFIED_NAME, flowPathQualifiedName, flowPathRef);
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            metrics.uniquePartialNiFiFlowPathUpdates = deduplicatedMessages.size();
            notifier.accept(deduplicatedMessages);

            // 3. Notify other messages
            notifier.accept(otherMessages);

        } finally {
            logger.info(metrics.toLogString("Finished"));
        }

    }

    /**
     * <p>Convert nifi_flow_path inputs or outputs to a map of Referenceable keyed by qualifiedName.</p>
     * <p>Atlas removes existing references those are not specified when a collection attribute is updated.
     * In order to preserve existing DataSet references, existing elements should be passed within a partial update message.</p>
     * <p>This method also populates entity cache for subsequent lookups.</p>
     * @param _refs Contains references from an existin nifi_flow_path entity inputs or outputs attribute.
     * @return A map of Referenceables keyed by qualifiedName.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Referenceable> toReferenceables(Object _refs, Metrics metrics) {
        if (_refs == null) {
            // NOTE: This empty map may be used to add new Referenceables. Can not be Collection.emptyMap which does not support addition.
            return new HashMap<>();
        }

        final Collection<Map<String, Object>> refs = (Collection<Map<String, Object>>) _refs;
        return refs.stream().map(ref -> {
            // Existing reference should has a GUID.
            final String typeName = (String) ref.get(ATTR_TYPENAME);
            final String guid = (String) ref.get(ATTR_GUID);

            if (guidToQualifiedName.containsKey(guid)) {
                metrics.dataSetCacheHit++;
            }

            final String refQualifiedName = guidToQualifiedName.computeIfAbsent(guid, k -> {
                try {
                    metrics.dataSetSearched++;
                    final AtlasEntity.AtlasEntityWithExtInfo refExt = atlasClient.searchEntityDef(new AtlasObjectId(guid, typeName));
                    final String qualifiedName = (String) refExt.getEntity().getAttribute(ATTR_QUALIFIED_NAME);
                    typedQualifiedNameToRef.put(toTypedQualifiedName(typeName, qualifiedName), new Referenceable(guid, typeName, Collections.EMPTY_MAP));
                    return qualifiedName;
                } catch (AtlasServiceException e) {
                    if (ClientResponse.Status.NOT_FOUND.equals(e.getStatus())) {
                        logger.warn("{} entity was not found for guid {}", typeName, guid);
                    } else {
                        logger.warn("Failed to retrieve {} with guid {} due to {}", typeName, guid, e);
                    }
                    return null;
                }
            });

            if (refQualifiedName == null) {
                return null;
            }
            return new Tuple<>(refQualifiedName, typedQualifiedNameToRef.get(toTypedQualifiedName(typeName, refQualifiedName)));
        }).filter(Objects::nonNull).filter(tuple -> tuple.getValue() != null)
                // If duplication happens, use new value.
                .collect(toMap(Tuple::getKey, Tuple::getValue, (oldValue, newValue) -> {
                    logger.debug("Duplicated qualified name was found, use the new one. oldValue={}, newValue={}", new Object[]{oldValue, newValue});
                    return newValue;
                }));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Referenceable> fromReferenceable(Object _refs, Metrics metrics) {
        if (_refs == null) {
            return Collections.emptyMap();
        }

        final Collection<Referenceable> refs = (Collection<Referenceable>) _refs;
        return refs.stream().map(ref -> {
            // This ref is created within this reporting cycle, and it may not have GUID assigned yet, if it is a brand new reference.
            // If cache has the Reference, then use it because instances in the cache are guaranteed to have GUID assigned.
            // Brand new Referenceables have to have all mandatory attributes.
            final String typeName = ref.getTypeName();
            final Id id = ref.getId();
            final String refQualifiedName = (String) ref.get(ATTR_QUALIFIED_NAME);
            final String typedRefQualifiedName = toTypedQualifiedName(typeName, refQualifiedName);

            final Referenceable refFromCacheIfAvailable = typedQualifiedNameToRef.computeIfAbsent(typedRefQualifiedName, k -> {
                if (id.isAssigned()) {
                    // If this referenceable has Guid assigned, then add this one to cache.
                    guidToQualifiedName.put(id._getId(), refQualifiedName);
                    typedQualifiedNameToRef.put(typedRefQualifiedName, ref);
                }
                return ref;
            });

            return new Tuple<>(refQualifiedName, refFromCacheIfAvailable);
        }).filter(tuple -> tuple.getValue() != null)
                .collect(toMap(Tuple::getKey, Tuple::getValue));
    }
}
