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
package org.apache.nifi.processor.util.list;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.lang.String.format;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.REL_SUCCESS;

public class ListedEntityTracker<T extends ListableEntity> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    protected volatile Map<String, ListedEntity> alreadyListedEntities;

    private static final String NOTE = "Used by 'Tracking Entities' strategy.";
    public static final PropertyDescriptor TRACKING_STATE_CACHE = new PropertyDescriptor.Builder()
            .name("et-state-cache")
            .displayName("Entity Tracking State Cache")
            .description(format("Listed entities are stored in the specified cache storage" +
                    " so that this processor can resume listing across NiFi restart or in case of primary node change." +
                    " 'Tracking Entities' strategy require tracking information of all listed entities within the last 'Tracking Time Window'." +
                    " To support large number of entities, the strategy uses DistributedMapCache instead of managed state." +
                    " Cache key format is 'ListedEntities::{processorId}(::{nodeId})'." +
                    " If it tracks per node listed entities, then the optional '::{nodeId}' part is added to manage state separately." +
                    " E.g. cluster wide cache key = 'ListedEntities::8dda2321-0164-1000-50fa-3042fe7d6a7b'," +
                    " per node cache key = 'ListedEntities::8dda2321-0164-1000-50fa-3042fe7d6a7b::nifi-node3'" +
                    " The stored cache content is Gzipped JSON string." +
                    " The cache key will be deleted when target listing configuration is changed." +
                    " %s", NOTE))
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor TRACKING_TIME_WINDOW = new PropertyDescriptor.Builder()
            .name("et-time-window")
            .displayName("Entity Tracking Time Window")
            .description(format("Specify how long this processor should track already-listed entities." +
                    " 'Tracking Entities' strategy can pick any entity whose timestamp is inside the specified time window." +
                    " For example, if set to '30 minutes', any entity having timestamp in recent 30 minutes will be the listing target when this processor runs." +
                    " A listed entity is considered 'new/updated' and a FlowFile is emitted if one of following condition meets:" +
                    " 1. does not exist in the already-listed entities," +
                    " 2. has newer timestamp than the cached entity," +
                    " 3. has different size than the cached entity." +
                    " If a cached entity's timestamp becomes older than specified time window, that entity will be removed from the cached already-listed entities." +
                    " %s", NOTE))
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("3 hours")
            .build();

    public static final AllowableValue INITIAL_LISTING_TARGET_ALL = new AllowableValue("all", "All Available",
            "Regardless of entities timestamp, all existing entities will be listed at the initial listing activity.");
    public static final AllowableValue INITIAL_LISTING_TARGET_WINDOW = new AllowableValue("window", "Tracking Time Window",
            "Ignore entities having timestamp older than the specified 'Tracking Time Window' at the initial listing activity.");

    public static final PropertyDescriptor INITIAL_LISTING_TARGET = new PropertyDescriptor.Builder()
            .name("et-initial-listing-target")
            .displayName("Entity Tracking Initial Listing Target")
            .description(format("Specify how initial listing should be handled." +
                    " %s", NOTE))
            .allowableValues(INITIAL_LISTING_TARGET_WINDOW, INITIAL_LISTING_TARGET_ALL)
            .defaultValue(INITIAL_LISTING_TARGET_ALL.getValue())
            .build();

    public static final PropertyDescriptor NODE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("et-node-identifier")
            .displayName("Entity Tracking Node Identifier")
            .description(format("The configured value will be appended to the cache key" +
                    " so that listing state can be tracked per NiFi node rather than cluster wide" +
                    " when tracking state is scoped to LOCAL. %s", NOTE))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("${hostname()}")
            .build();

    static final Supplier<Long> DEFAULT_CURRENT_TIMESTAMP_SUPPLIER = System::currentTimeMillis;
    private final Supplier<Long> currentTimestampSupplier;

    private final Serializer<String> stringSerializer = (v, o) -> o.write(v.getBytes(StandardCharsets.UTF_8));

    private final Serializer<Map<String, ListedEntity>> listedEntitiesSerializer = (v, o) -> {
        final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(o);
        objectMapper.writeValue(gzipOutputStream, v);
        // Finish writing gzip data without closing the underlying stream.
        gzipOutputStream.finish();
    };

    private final Deserializer<Map<String, ListedEntity>> listedEntitiesDeserializer = v -> {
        if (v == null || v.length == 0) {
            return null;
        }
        try (final GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(v))) {
            return objectMapper.readValue(in, new TypeReference<>() { });
        }
    };

    private final String componentId;
    private final ComponentLog logger;
    private final RecordSchema recordSchema;

    /*
     * The scope, nodeId and mapCacheClient being used at the previous trackEntities method execution is captured,
     * so that it can be used when resetListedEntities is called.
     */
    private Scope scope;
    private String nodeId;
    private DistributedMapCacheClient mapCacheClient;

    public ListedEntityTracker(final String componentId, final ComponentLog logger, final RecordSchema recordSchema) {
        this(componentId, logger, DEFAULT_CURRENT_TIMESTAMP_SUPPLIER, recordSchema);
    }

    /**
     * This constructor is used by unit test code so that it can produce the consistent result by controlling current timestamp.
     * @param currentTimestampSupplier a function to return current timestamp.
     */
    public ListedEntityTracker(final String componentId, final ComponentLog logger, final Supplier<Long> currentTimestampSupplier, final RecordSchema recordSchema) {
        this.componentId = componentId;
        this.logger = logger;
        this.currentTimestampSupplier = currentTimestampSupplier;
        this.recordSchema = recordSchema;
    }

    static void validateProperties(ValidationContext context, Collection<ValidationResult> results, Scope scope) {
        validateRequiredProperty(context, results, TRACKING_STATE_CACHE);
        validateRequiredProperty(context, results, TRACKING_TIME_WINDOW);

        if (Scope.LOCAL.equals(scope)
            && StringUtils.isEmpty(context.getProperty(NODE_IDENTIFIER).evaluateAttributeExpressions().getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(NODE_IDENTIFIER.getDisplayName())
                    .explanation(format("'%s' is required to use local scope with '%s' listing strategy",
                            NODE_IDENTIFIER.getDisplayName(), AbstractListProcessor.BY_ENTITIES.getDisplayName()))
                    .build());
        }
    }

    private static void validateRequiredProperty(ValidationContext context, Collection<ValidationResult> results, PropertyDescriptor property) {
        if (!context.getProperty(property).isSet()) {
            final String displayName = property.getDisplayName();
            results.add(new ValidationResult.Builder()
                    .subject(displayName)
                    .explanation(format("'%s' is required to use '%s' listing strategy", displayName, AbstractListProcessor.BY_ENTITIES.getDisplayName()))
                    .valid(false)
                    .build());
        }
    }

    private static final String CACHE_KEY_PREFIX = "ListedEntities";
    private String getCacheKey() {
        return switch (scope) {
            case LOCAL -> format("%s::%s::%s", CACHE_KEY_PREFIX, componentId, nodeId);
            case CLUSTER -> format("%s::%s", CACHE_KEY_PREFIX, componentId);
        };
    }

    private void persistListedEntities(Map<String, ListedEntity> listedEntities) throws IOException {
        final String cacheKey = getCacheKey();
        logger.debug("Persisting listed entities: {}={}", cacheKey, listedEntities);
        mapCacheClient.put(cacheKey, listedEntities, stringSerializer, listedEntitiesSerializer);
    }

    private Map<String, ListedEntity> fetchListedEntities() throws IOException {
        final String cacheKey = getCacheKey();
        final Map<String, ListedEntity> listedEntities = mapCacheClient.get(cacheKey, stringSerializer, listedEntitiesDeserializer);
        logger.debug("Fetched listed entities: {}={}", cacheKey, listedEntities);
        return listedEntities;
    }

    public void clearListedEntities() throws IOException {
        alreadyListedEntities = null;
        if (mapCacheClient != null) {
            final String cacheKey = getCacheKey();
            logger.debug("Removing listed entities from cache storage: {}", cacheKey);
            mapCacheClient.remove(cacheKey, stringSerializer);
        }
    }

    public void trackEntities(ProcessContext context, ProcessSession session,
                              boolean justElectedPrimaryNode,
                              Scope scope,
                              Function<Long, Collection<T>> listEntities,
                              Function<T, Map<String, String>> createAttributes) throws ProcessException {

        boolean initialListing = false;
        mapCacheClient = context.getProperty(TRACKING_STATE_CACHE).asControllerService(DistributedMapCacheClient.class);
        this.scope = scope;
        if (Scope.LOCAL.equals(scope)) {
            nodeId = context.getProperty(NODE_IDENTIFIER).evaluateAttributeExpressions().getValue();
        } else {
            nodeId = null;
        }

        if (alreadyListedEntities == null || justElectedPrimaryNode) {
            logger.info(justElectedPrimaryNode ? "Just elected as Primary node, restoring already-listed entities." : "At the first onTrigger, restoring already-listed entities.");

            try {
                final Map<String, ListedEntity> fetchedListedEntities = fetchListedEntities();
                if (fetchedListedEntities == null) {
                    this.alreadyListedEntities = new ConcurrentHashMap<>();
                    initialListing = true;
                } else {
                    this.alreadyListedEntities = new ConcurrentHashMap<>(fetchedListedEntities);
                }
            } catch (final IOException e) {
                throw new ProcessException("Failed to restore already-listed entities due to " + e, e);
            }
        }

        final long currentTimeMillis = currentTimestampSupplier.get();
        final long watchWindowMillis = context.getProperty(TRACKING_TIME_WINDOW).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        final String initialListingTarget = context.getProperty(INITIAL_LISTING_TARGET).getValue();
        final long minTimestampToList = (initialListing && INITIAL_LISTING_TARGET_ALL.getValue().equals(initialListingTarget))
                ? -1 : currentTimeMillis - watchWindowMillis;

        final Collection<T> listedEntities = listEntities.apply(minTimestampToList);

        if (listedEntities.isEmpty()) {
            logger.debug("No entity is listed. Yielding.");
            context.yield();
            return;
        }

        final List<T> updatedEntities = listedEntities.stream().filter(entity -> {
            final String identifier = entity.getIdentifier();

            if (entity.getTimestamp() < minTimestampToList) {
                logger.trace("Skipped {} having older timestamp {} than the minTimestampToList {}.", identifier, entity.getTimestamp(), minTimestampToList);
                return false;
            }

            final ListedEntity alreadyListedEntity = alreadyListedEntities.get(identifier);
            if (alreadyListedEntity == null) {
                logger.trace("Picked {} being newly found.", identifier);
                return true;
            }

            if (entity.getTimestamp() > alreadyListedEntity.getTimestamp()) {
                logger.trace("Picked {} having newer timestamp {} than {}.", identifier, entity.getTimestamp(), alreadyListedEntity.getTimestamp());
                return true;
            }

            if (entity.getSize() != alreadyListedEntity.getSize()) {
                logger.trace("Picked {} having different size {} than {}.", identifier, entity.getSize(), alreadyListedEntity.getSize());
                return true;
            }

            logger.trace("Skipped {}, not changed.", identifier);
            return false;
        }).collect(Collectors.toList());

        // Find old enough entries.
        final List<String> oldEntityIds = alreadyListedEntities.entrySet().stream()
                .filter(entry -> entry.getValue().getTimestamp() < minTimestampToList).map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (updatedEntities.isEmpty() && oldEntityIds.isEmpty()) {
            logger.debug("None of updated or old entity was found. Yielding.");
            context.yield();
            return;
        }

        // Remove old entries.
        oldEntityIds.forEach(oldEntityId -> alreadyListedEntities.remove(oldEntityId));

        // Emit updated entities.
        if (context.getProperty(AbstractListProcessor.RECORD_WRITER).isSet()) {
            try {
                createRecordsForEntities(context, session, updatedEntities);
            } catch (final IOException | SchemaNotFoundException e) {
                logger.error("Failed to create records for listed entities", e);
            }
        } else {
            createFlowFilesForEntities(context, session, updatedEntities, createAttributes);
        }

        // Commit ProcessSession before persisting listed entities.
        // In case persisting listed entities failure, same entities may be listed again, but better than not listing.
        session.commitAsync(() -> {
            try {
                logger.debug("Removed old entities count: {}, Updated entities count: {}", oldEntityIds.size(), updatedEntities.size());
                logger.trace("Removed old entities: {}, Updated entities: {}", oldEntityIds, updatedEntities);

                persistListedEntities(alreadyListedEntities);
            } catch (IOException e) {
                throw new ProcessException("Failed to persist already-listed entities due to " + e, e);
            }
        });
    }

    protected void createRecordsForEntities(final ProcessContext context, final ProcessSession session, final List<T> updatedEntities) throws IOException, SchemaNotFoundException {
        if (updatedEntities.isEmpty()) {
            logger.debug("No entities to write records for");
            return;
        }

        final RecordSetWriterFactory writerFactory = context.getProperty(AbstractListProcessor.RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        FlowFile flowFile = session.create();
        final WriteResult writeResult;
        try (final OutputStream out = session.write(flowFile);
             final RecordSetWriter recordSetWriter = writerFactory.createWriter(logger, recordSchema, out, Collections.emptyMap())) {

            recordSetWriter.beginRecordSet();
            for (T updatedEntity : updatedEntities) {
                recordSetWriter.write(updatedEntity.toRecord());

                // In order to reduce object size, discard meta data captured at the sub-classes.
                final ListedEntity listedEntity = new ListedEntity(updatedEntity.getTimestamp(), updatedEntity.getSize());
                alreadyListedEntities.put(updatedEntity.getIdentifier(), listedEntity);
            }

            writeResult = recordSetWriter.finishRecordSet();
        }

        final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.transfer(flowFile, REL_SUCCESS);
    }

    protected void createFlowFilesForEntities(ProcessContext context, final ProcessSession session, final List<T> updatedEntities, final Function<T, Map<String, String>> createAttributes) {
        for (T updatedEntity : updatedEntities) {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, createAttributes.apply(updatedEntity));
            session.transfer(flowFile, REL_SUCCESS);
            // In order to reduce object size, discard meta data captured at the sub-classes.
            final ListedEntity listedEntity = new ListedEntity(updatedEntity.getTimestamp(), updatedEntity.getSize());
            alreadyListedEntities.put(updatedEntity.getIdentifier(), listedEntity);
        }
    }

}
