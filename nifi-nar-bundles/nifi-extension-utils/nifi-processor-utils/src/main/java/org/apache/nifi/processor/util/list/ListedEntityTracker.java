package org.apache.nifi.processor.util.list;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.PropertyDescriptor;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.nifi.processor.util.list.AbstractListProcessor.DISTRIBUTED_CACHE_SERVICE;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.REL_SUCCESS;

public class ListedEntityTracker<T extends ListableEntity> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private volatile Map<String, ListedEntity> alreadyListedEntities;

    public static final PropertyDescriptor TRACKING_TIME_WINDOW = new PropertyDescriptor.Builder()
            .name("tracking-time-window")
            .displayName("Tracking Time Window")
            // TODO: doc, at least two times longer than scheduled
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .defaultValue("10 mins")
            .build();

    private Serializer<String> stringSerializer = (v, o) -> o.write(v.getBytes(StandardCharsets.UTF_8));
    private Serializer<Map<String, ListedEntity>> listedEntitiesSerializer
            = (v, o) -> objectMapper.writeValue(new GZIPOutputStream(o), v);
    private Deserializer<Map<String, ListedEntity>> listedEntitiesDeserializer
            = v -> (v == null || v.length == 0) ? null
            : objectMapper.readValue(new GZIPInputStream(new ByteArrayInputStream(v)),
            new TypeReference<Map<String, ListedEntity>>() {});

    private final String componentId;
    private final ComponentLog logger;

    ListedEntityTracker(String componentId, ComponentLog logger) {
        this.componentId = componentId;
        this.logger = logger;
    }

    private String getCacheKey() {
        return String.format("%s::%s", getClass().getSimpleName(), componentId);
    }

    private void persistListedEntities(DistributedMapCacheClient mapCacheClient, Map<String, ListedEntity> listedEntities) throws IOException {
        final String cacheKey = getCacheKey();
        logger.debug("Persisting listed entities: {}={}", new Object[]{cacheKey, listedEntities});
        mapCacheClient.put(cacheKey, listedEntities, stringSerializer, listedEntitiesSerializer);
    }

    private Map<String, ListedEntity> fetchListedEntities(DistributedMapCacheClient mapCacheClient) throws IOException {
        final String cacheKey = getCacheKey();
        final Map<String, ListedEntity> listedEntities = mapCacheClient.get(cacheKey, stringSerializer, listedEntitiesDeserializer);
        logger.debug("Fetched listed entities: {}={}", new Object[]{cacheKey, listedEntities});
        return listedEntities;
    }

    // TODO: better naming
    public void trackEntities(ProcessContext context, ProcessSession session,
                              boolean justElectedPrimaryNode,
                              Function<Long, Map<String, T>> listEntities,
                              Function<T, Map<String, String>> createAttributes) throws ProcessException {

        final DistributedMapCacheClient mapCacheClient = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        if (alreadyListedEntities == null || justElectedPrimaryNode) {
            logger.info(justElectedPrimaryNode
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
        }

        final long currentTimeMillis = System.currentTimeMillis();
        final long watchWindowMillis = context.getProperty(TRACKING_TIME_WINDOW).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final long minTimestampToList = currentTimeMillis - watchWindowMillis;

        final Map<String, T> listedEntities = listEntities.apply(minTimestampToList);

        if (listedEntities.size() == 0) {
            logger.debug("No entity is listed. Yielding.");
            context.yield();
            return;
        }

        final Map<String, T> updatedEntities = listedEntities.entrySet().stream().filter(entry -> {
            final String identifier = entry.getKey();
            final ListableEntity entity = entry.getValue();

            final ListedEntity alreadyListedEntity = alreadyListedEntities.get(identifier);
            if (alreadyListedEntity == null) {
                logger.trace("{} was newly found.", new Object[]{identifier});
                return true;
            }

            if (entity.getTimestamp() > alreadyListedEntity.getTimestamp()) {
                logger.trace("{} has newer timestamp {} than {}.",
                        new Object[]{identifier, entity.getTimestamp(), alreadyListedEntity.getTimestamp()});
                return true;
            }

            if (entity.getSize() != alreadyListedEntity.getSize()) {
                logger.trace("{} has different size {} than {}.",
                        new Object[]{identifier, entity.getSize(), alreadyListedEntity.getSize()});
                return true;
            }

            return false;
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Remove old enough entries.
        final List<String> oldEntityIds = alreadyListedEntities.entrySet().stream()
                .filter(entry -> entry.getValue().getTimestamp() < minTimestampToList).map(Map.Entry::getKey)
                .collect(Collectors.toList());
        oldEntityIds.forEach(oldEntityId -> alreadyListedEntities.remove(oldEntityId));

        if (updatedEntities.isEmpty() && oldEntityIds.isEmpty()) {
            logger.debug("None of updated or old entity was found. Yielding.");
            context.yield();
            return;
        }

        // TODO: write a lock object into KVS so that other node will wait this cycle to finish, in case the primary node change.

        // Emit updated entities.
        updatedEntities.forEach((identifier, updatedEntity) -> {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, createAttributes.apply(updatedEntity));
            session.transfer(flowFile, REL_SUCCESS);
            // In order to reduce object size, discard meta data captured at the sub-classes.
            alreadyListedEntities.put(identifier, new ListedEntity(updatedEntity.getTimestamp(), updatedEntity.getSize()));
        });

        // Commit ProcessSession before persisting listed entities.
        // In case persisting listed entities failure, same entities may be listed again, but better than not listing.
        session.commit();
        try {
            logger.trace("Removed old entities: {}, Updated entities: {}", new Object[]{oldEntityIds, updatedEntities});
            persistListedEntities(mapCacheClient, alreadyListedEntities);
        } catch (IOException e) {
            throw new ProcessException("Failed to persist already-listed entities due to " + e, e);
        }

    }


    private class ListedEntity {
        /**
         * Milliseconds.
         */
        private final long lastModifiedTimestamp;
        /**
         * Bytes.
         */
        private final long size;

        private ListedEntity(long lastModifiedTimestamp, long size) {
            this.lastModifiedTimestamp = lastModifiedTimestamp;
            this.size = size;
        }

        public long getTimestamp() {
            return lastModifiedTimestamp;
        }

        public long getSize() {
            return size;
        }
    }

}
