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
package org.apache.nifi.atlas;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.notification.hook.HookNotification.EntityPartialUpdateRequest;
import org.apache.atlas.notification.hook.HookNotification.HookNotificationMessage;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.nifi.atlas.provenance.lineage.LineageContext;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.atlas.notification.hook.HookNotification.HookNotificationType.ENTITY_PARTIAL_UPDATE;
import static org.apache.nifi.atlas.AtlasUtils.toTypedQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_GUID;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_TYPENAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

/**
 * This class is not thread-safe as it holds uncommitted notification messages within instance.
 * {@link #addMessage(HookNotificationMessage)} and {@link #commitMessages()} should be used serially from a single thread.
 */
public class NiFiAtlasHook extends AtlasHook implements LineageContext {

    public static final String NIFI_USER = "nifi";

    private static final Logger logger = LoggerFactory.getLogger(NiFiAtlasHook.class);
    private static final String CONF_PREFIX = "atlas.hook.nifi.";
    private static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";

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

    public NiFiAtlasHook() {
        final int qualifiedNameCacheSize = 10_000;
        this.guidToQualifiedName = createCache(qualifiedNameCacheSize);

        final int dataSetRefCacheSize = 1_000;
        this.typedQualifiedNameToRef = createCache(dataSetRefCacheSize);
    }

    public void setAtlasClient(NiFiAtlasClient atlasClient) {
        this.atlasClient = atlasClient;
    }

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }


    private final List<HookNotificationMessage> messages = new ArrayList<>();

    @Override
    public void addMessage(HookNotificationMessage message) {
        messages.add(message);
    }

    private class Metrics {
        final long startedAt = System.currentTimeMillis();
        int totalMessages;
        int partialNiFiFlowPathUpdates;
        int dedupedPartialNiFiFlowPathUpdates;
        int otherMessages;
        int flowPathSearched;
        int dataSetSearched;
        int dataSetCacheHit;
        private void log(String message) {
            logger.debug(String.format("%s, %d ms passed, totalMessages=%d," +
                    " partialNiFiFlowPathUpdates=%d, dedupedPartialNiFiFlowPathUpdates=%d, otherMessage=%d," +
                    " flowPathSearched=%d, dataSetSearched=%d, dataSetCacheHit=%s," +
                    " guidToQualifiedName.size=%d, typedQualifiedNameToRef.size=%d",
                    message, System.currentTimeMillis() - startedAt, totalMessages,
                    partialNiFiFlowPathUpdates, dedupedPartialNiFiFlowPathUpdates, otherMessages,
                    flowPathSearched, dataSetSearched, dataSetCacheHit,
                    guidToQualifiedName.size(), typedQualifiedNameToRef.size()));
        }
    }

    public void commitMessages() {
        final Map<Boolean, List<HookNotificationMessage>> partialNiFiFlowPathUpdateAndOthers
                = messages.stream().collect(Collectors.groupingBy(msg
                    -> ENTITY_PARTIAL_UPDATE.equals(msg.getType())
                        && TYPE_NIFI_FLOW_PATH.equals(((EntityPartialUpdateRequest)msg).getTypeName())
                        && ATTR_QUALIFIED_NAME.equals(((EntityPartialUpdateRequest)msg).getAttribute())
        ));


        final List<HookNotificationMessage> otherMessages = partialNiFiFlowPathUpdateAndOthers.computeIfAbsent(false, k -> Collections.emptyList());
        final List<HookNotificationMessage> partialNiFiFlowPathUpdates = partialNiFiFlowPathUpdateAndOthers.computeIfAbsent(true, k -> Collections.emptyList());
        logger.info("Commit messages: {} partialNiFiFlowPathUpdate and {} other messages.", partialNiFiFlowPathUpdates.size(), otherMessages.size());

        final Metrics metrics = new Metrics();
        metrics.totalMessages = messages.size();
        metrics.partialNiFiFlowPathUpdates = partialNiFiFlowPathUpdates.size();
        metrics.otherMessages = otherMessages.size();

        try {
            // Notify other messages first.
            notifyEntities(otherMessages);

            // De-duplicate messages.
            final List<HookNotificationMessage> deduplicatedMessages = partialNiFiFlowPathUpdates.stream().map(msg -> (EntityPartialUpdateRequest) msg)
                    // Group by nifi_flow_path qualifiedName value.
                    .collect(Collectors.groupingBy(EntityPartialUpdateRequest::getAttributeValue)).entrySet().stream()
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
                        for (EntityPartialUpdateRequest msg : entry.getValue()) {
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
                        return new EntityPartialUpdateRequest(NIFI_USER, TYPE_NIFI_FLOW_PATH,
                                ATTR_QUALIFIED_NAME, flowPathQualifiedName, flowPathRef);
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            metrics.dedupedPartialNiFiFlowPathUpdates = deduplicatedMessages.size();
            notifyEntities(deduplicatedMessages);
        } finally {
            metrics.log("Committed");
            messages.clear();
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

        final List<Map<String, Object>> refs = (List<Map<String, Object>>) _refs;
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
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue, (oldValue, newValue) -> {
                    logger.warn("Duplicated qualified name was found, use the new one. oldValue={}, newValue={}", new Object[]{oldValue, newValue});
                    return newValue;
                }));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Referenceable> fromReferenceable(Object _refs, Metrics metrics) {
        if (_refs == null) {
            return Collections.emptyMap();
        }

        final List<Referenceable> refs = (List<Referenceable>) _refs;
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
        }).filter(Objects::nonNull).filter(tuple -> tuple.getValue() != null)
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));
    }

    public void close() {
        if (notificationInterface != null) {
            notificationInterface.close();
        }
    }
}
