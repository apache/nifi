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

package org.apache.nifi.processor.util.watch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * TODO: doc
 */
@TriggerSerially
public abstract class AbstractWatchEntities extends AbstractProcessor {

    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("distributed-cache-service")
            .displayName("Distributed Cache Service")
            // TODO: doc
            .identifiesControllerService(DistributedMapCacheClient.class)
            .required(true)
            .build();

    private static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("min-age")
            .displayName("Minimum File Age")
            // TODO: doc, to avoid picking the same file more than once if it's being written
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .defaultValue("0 sec")
            .build();

    private static final PropertyDescriptor WATCH_TIME_WINDOW = new PropertyDescriptor.Builder()
            .name("watch-time-window")
            .displayName("Watch Time Window")
            // TODO: doc, at least two times longer than scheduled
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .defaultValue("10 mins")
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            // TODO: doc
            .build();

    // TODO: need a flag to manage state as local or cluster.
    // TODO: need some logic to clear alreadyListedEntities based on property changes. Similar to isListingResetNecessary.

    private ObjectMapper objectMapper = new ObjectMapper();
    private volatile Map<String, ListedEntity> alreadyListedEntities;
    private volatile boolean justElectedPrimaryNode = false;

    @Override
    protected final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        properties.add(MIN_AGE);
        properties.add(WATCH_TIME_WINDOW);
        properties.addAll(getSupportedProperties());
        return properties;
    }

    protected abstract List<PropertyDescriptor> getSupportedProperties();

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    private Serializer<String> stringSerializer = (v, o) -> o.write(v.getBytes(StandardCharsets.UTF_8));
    private Serializer<Map<String, ListedEntity>> listedEntitiesSerializer
            = (v, o) -> objectMapper.writeValue(new GZIPOutputStream(o), v);
    private Deserializer<Map<String, ListedEntity>> listedEntitiesDeserializer
            = v -> (v == null || v.length == 0) ? null
                : objectMapper.readValue(new GZIPInputStream(new ByteArrayInputStream(v)),
            new TypeReference<Map<String, ListedEntity>>() {});

    private String getCacheKey() {
        return String.format("%s::%s", getClass().getSimpleName(), getIdentifier());
    }

    private void persistListedEntities(DistributedMapCacheClient mapCacheClient, Map<String, ListedEntity> listedEntities) throws IOException {
        final String cacheKey = getCacheKey();
        getLogger().debug("Persisting listed entities: {}={}", new Object[]{cacheKey, listedEntities});
        mapCacheClient.put(cacheKey, listedEntities, stringSerializer, listedEntitiesSerializer);
    }

    private Map<String, ListedEntity> fetchListedEntities(DistributedMapCacheClient mapCacheClient) throws IOException {
        final String cacheKey = getCacheKey();
        final Map<String, ListedEntity> listedEntities = mapCacheClient.get(cacheKey, stringSerializer, listedEntitiesDeserializer);
        getLogger().debug("Fetched listed entities: {}={}", new Object[]{cacheKey, listedEntities});
        return listedEntities;
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange(final PrimaryNodeState newState) {
        justElectedPrimaryNode = (newState == PrimaryNodeState.ELECTED_PRIMARY_NODE);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final DistributedMapCacheClient mapCacheClient = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        if (alreadyListedEntities == null || justElectedPrimaryNode) {
            getLogger().info(justElectedPrimaryNode
                    ? "Just elected as Primary node, restoring already-listed entities."
                    : "At the first onTrigger, restoring already-listed entities.");
            try {
                alreadyListedEntities = fetchListedEntities(mapCacheClient);
                if (alreadyListedEntities == null) {
                    alreadyListedEntities = new HashMap<>();
                }
            } catch (IOException e) {
                throw new ProcessException("Failed to restore already-listed entities due to " + e, e);
            }
            justElectedPrimaryNode = false;
        }

        final long currentTimeMillis = System.currentTimeMillis();
        final long watchWindowMillis = context.getProperty(WATCH_TIME_WINDOW).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final long minTimestampToList = currentTimeMillis - watchWindowMillis;

        final Map<String, ListedEntity> listedEntities = performListing(context, minTimestampToList);

        if (listedEntities.size() == 0) {
            getLogger().debug("No entity is listed. Yielding.");
            context.yield();
            return;
        }

        final long minAgeMillis = context.getProperty(MIN_AGE).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final long maxTimestampToList = currentTimeMillis - minAgeMillis;

        final Map<String, ListedEntity> updatedEntities = listedEntities.entrySet().stream().filter(entry -> {
            final String identifier = entry.getKey();
            final ListedEntity entity = entry.getValue();

            if (entity.getLastModifiedTimestamp() > maxTimestampToList) {
                getLogger().trace("{} was skipped because its timestamp {} is not older than min age {} (ms).",
                        new Object[]{identifier, entity.getLastModifiedTimestamp(), minAgeMillis});
                return false;
            }

            final ListedEntity alreadyListedEntity = alreadyListedEntities.get(identifier);
            if (alreadyListedEntity == null) {
                getLogger().trace("{} was newly found.", new Object[]{identifier});
                return true;
            }

            if (entity.getLastModifiedTimestamp() > alreadyListedEntity.getLastModifiedTimestamp()) {
                getLogger().trace("{} has newer timestamp {} than {}.",
                        new Object[]{identifier, entity.getLastModifiedTimestamp(), alreadyListedEntity.getLastModifiedTimestamp()});
                return true;
            }

            if (entity.getSize() != alreadyListedEntity.getSize()) {
                getLogger().trace("{} has different size {} than {}.",
                        new Object[]{identifier, entity.getSize(), alreadyListedEntity.getSize()});
                return true;
            }

            return false;
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Remove old enough entries.
        final List<String> oldEntityIds = alreadyListedEntities.entrySet().stream()
                .filter(entry -> entry.getValue().getLastModifiedTimestamp() < minTimestampToList).map(Map.Entry::getKey)
                .collect(Collectors.toList());
        oldEntityIds.forEach(oldEntityId -> alreadyListedEntities.remove(oldEntityId));

        if (updatedEntities.isEmpty() && oldEntityIds.isEmpty()) {
            getLogger().debug("None of updated or old entity was found. Yielding.");
            context.yield();
            return;
        }

        // TODO: write a lock object into KVS so that other node will wait this cycle to finish, in case the primary node change.

        // Emit updated entities.
        updatedEntities.forEach((identifier, updatedEntity) -> {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, updatedEntity.createAttributes());
            session.transfer(flowFile, REL_SUCCESS);
            // In order to reduce object size, discard meta data captured at the sub-classes.
            alreadyListedEntities.put(identifier, new ListedEntity(updatedEntity.getLastModifiedTimestamp(), updatedEntity.getSize()));
        });

        // Commit ProcessSession before persisting listed entities.
        // In case persisting listed entities failure, same entities may be listed again, but better than not listing.
        session.commit();
        try {
            getLogger().trace("Removed old entities: {}, Updated entities: {}", new Object[]{oldEntityIds, updatedEntities});
            persistListedEntities(mapCacheClient, alreadyListedEntities);
        } catch (IOException e) {
            throw new ProcessException("Failed to persist already-listed entities due to " + e, e);
        }

    }

    protected abstract Map<String, ListedEntity> performListing(ProcessContext context, long minTimestampToList);
}