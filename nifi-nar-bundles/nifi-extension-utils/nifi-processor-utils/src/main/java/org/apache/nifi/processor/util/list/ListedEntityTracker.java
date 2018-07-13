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
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.nifi.processor.util.list.AbstractListProcessor.REL_SUCCESS;

public class ListedEntityTracker<T extends ListableEntity> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private volatile Map<String, ListedEntity> alreadyListedEntities;

    public static final PropertyDescriptor TRACKING_STATE_CACHE = new PropertyDescriptor.Builder()
            .name("tracking-state-cache")
            .displayName("Tracking State Cache")
            // TODO: doc
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor TRACKING_TIME_WINDOW = new PropertyDescriptor.Builder()
            .name("tracking-time-window")
            .displayName("Tracking Time Window")
            // TODO: doc, at least two times longer than scheduled
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("3 hours")
            .build();

    // TODO: doc
    private static final AllowableValue INITIAL_LISTING_TARGET_ALL = new AllowableValue("all", "All Available");
    private static final AllowableValue INITIAL_LISTING_TARGET_WINDOW = new AllowableValue("window", "Tracking Time Window");

    public static final PropertyDescriptor INITIAL_LISTING_TARGET = new PropertyDescriptor.Builder()
            .name("initial-listing-target")
            .displayName("Initial Listing Target")
            // TODO: doc
            .allowableValues(INITIAL_LISTING_TARGET_WINDOW, INITIAL_LISTING_TARGET_ALL)
            .defaultValue(INITIAL_LISTING_TARGET_WINDOW.getValue())
            .build();

    public static final PropertyDescriptor NODE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("node-identifier")
            .displayName("Node Identifier")
            // TODO: doc
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("${hostname()}")
            .build();

    private Serializer<String> stringSerializer = (v, o) -> o.write(v.getBytes(StandardCharsets.UTF_8));
    private Serializer<Map<String, ListedEntity>> listedEntitiesSerializer
            = (v, o) -> objectMapper.writeValue(new GZIPOutputStream(o), v);
    private Deserializer<Map<String, ListedEntity>> listedEntitiesDeserializer
            = v -> (v == null || v.length == 0) ? null
            : objectMapper.readValue(new GZIPInputStream(new ByteArrayInputStream(v)), new TypeReference<Map<String, ListedEntity>>() {});

    private final String componentId;
    private final ComponentLog logger;
    private final Scope scope;

    /*
     * The nodeId and mapCacheClient being used at the previous trackEntities method execution is captured,
     * so that it can be used when resetListedEntities is called.
     */
    private String nodeId;
    private DistributedMapCacheClient mapCacheClient;

    ListedEntityTracker(String componentId, ComponentLog logger, Scope scope) {
        this.componentId = componentId;
        this.logger = logger;
        this.scope = scope;
    }

    static void validateProperties(ValidationContext context, Collection<ValidationResult> results, Scope scope) {
        final Consumer<PropertyDescriptor> validateRequiredProperty = property -> {
            if (!context.getProperty(property).isSet()) {
                final String displayName = property.getDisplayName();
                results.add(new ValidationResult.Builder()
                        .subject(displayName)
                        .explanation(String.format("'%s' is required to use '%s' listing strategy", displayName, AbstractListProcessor.BY_ENTITIES.getDisplayName()))
                        .valid(false)
                        .build());
            }
        };
        validateRequiredProperty.accept(ListedEntityTracker.TRACKING_STATE_CACHE);
        validateRequiredProperty.accept(ListedEntityTracker.TRACKING_TIME_WINDOW);

        if (Scope.LOCAL.equals(scope)) {
            if (StringUtils.isEmpty(context.getProperty(NODE_IDENTIFIER).evaluateAttributeExpressions().getValue())) {
                results.add(new ValidationResult.Builder()
                        .subject(NODE_IDENTIFIER.getDisplayName())
                        .explanation(String.format("'%s' is required to use local scope with '%s' listing strategy",
                                NODE_IDENTIFIER.getDisplayName(), AbstractListProcessor.BY_ENTITIES.getDisplayName()))
                        .build());
            }
        }
    }

    private String getCacheKey() {
        switch (scope) {
            case LOCAL:
                return String.format("%s::%s::%s", getClass().getSimpleName(), componentId, nodeId);
            case CLUSTER:
                return String.format("%s::%s", getClass().getSimpleName(), componentId);
        }
        throw new IllegalArgumentException("Unknown scope: " + scope);
    }

    private void persistListedEntities(Map<String, ListedEntity> listedEntities) throws IOException {
        final String cacheKey = getCacheKey();
        logger.debug("Persisting listed entities: {}={}", new Object[]{cacheKey, listedEntities});
        mapCacheClient.put(cacheKey, listedEntities, stringSerializer, listedEntitiesSerializer);
    }

    private Map<String, ListedEntity> fetchListedEntities() throws IOException {
        final String cacheKey = getCacheKey();
        final Map<String, ListedEntity> listedEntities = mapCacheClient.get(cacheKey, stringSerializer, listedEntitiesDeserializer);
        logger.debug("Fetched listed entities: {}={}", new Object[]{cacheKey, listedEntities});
        return listedEntities;
    }

    void clearListedEntities() throws IOException {
        alreadyListedEntities = null;
        if (mapCacheClient != null) {
            final String cacheKey = getCacheKey();
            logger.debug("Removing listed entities from cache storage: {}", new Object[]{cacheKey});
            mapCacheClient.remove(cacheKey, stringSerializer);
        }
    }

    public void trackEntities(ProcessContext context, ProcessSession session,
                              boolean justElectedPrimaryNode,
                              Function<Long, Collection<T>> listEntities,
                              Function<T, Map<String, String>> createAttributes) throws ProcessException {

        boolean initialListing = false;
        mapCacheClient = context.getProperty(TRACKING_STATE_CACHE).asControllerService(DistributedMapCacheClient.class);
        nodeId = context.getProperty(ListedEntityTracker.NODE_IDENTIFIER).evaluateAttributeExpressions().getValue();

        if (alreadyListedEntities == null || justElectedPrimaryNode) {
            logger.info(justElectedPrimaryNode
                    ? "Just elected as Primary node, restoring already-listed entities."
                    : "At the first onTrigger, restoring already-listed entities.");
            try {
                alreadyListedEntities = fetchListedEntities();
                if (alreadyListedEntities == null) {
                    alreadyListedEntities = new HashMap<>();
                    initialListing = true;
                }
            } catch (IOException e) {
                throw new ProcessException("Failed to restore already-listed entities due to " + e, e);
            }
        }

        final long currentTimeMillis = System.currentTimeMillis();
        final long watchWindowMillis = context.getProperty(TRACKING_TIME_WINDOW).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        final String initialListingTarget = context.getProperty(INITIAL_LISTING_TARGET).getValue();
        final long minTimestampToList = (initialListing && INITIAL_LISTING_TARGET_ALL.getValue().equals(initialListingTarget))
                ? -1 : currentTimeMillis - watchWindowMillis;

        final Collection<T> listedEntities = listEntities.apply(minTimestampToList);

        if (listedEntities.size() == 0) {
            logger.debug("No entity is listed. Yielding.");
            context.yield();
            return;
        }

        final Map<String, T> updatedEntities = listedEntities.stream().filter(entity -> {
            final String identifier = entity.getIdentifier();

            if (entity.getTimestamp() < minTimestampToList) {
                logger.trace("Skipped {} having older timestamp than the minTimestampToList {}.", new Object[]{identifier, entity.getTimestamp(), minTimestampToList});
                return false;
            }

            final ListedEntity alreadyListedEntity = alreadyListedEntities.get(identifier);
            if (alreadyListedEntity == null) {
                logger.trace("Picked {} being newly found.", new Object[]{identifier});
                return true;
            }

            if (entity.getTimestamp() > alreadyListedEntity.getTimestamp()) {
                logger.trace("Picked {} having newer timestamp {} than {}.",
                        new Object[]{identifier, entity.getTimestamp(), alreadyListedEntity.getTimestamp()});
                return true;
            }

            if (entity.getSize() != alreadyListedEntity.getSize()) {
                logger.trace("Picked {} having different size {} than {}.",
                        new Object[]{identifier, entity.getSize(), alreadyListedEntity.getSize()});
                return true;
            }

            logger.trace("Skipped {}, not changed.", new Object[]{identifier, entity.getTimestamp(), minTimestampToList});
            return false;
        }).collect(Collectors.toMap(T::getIdentifier, Function.identity()));

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

        // Emit updated entities.
        updatedEntities.forEach((identifier, updatedEntity) -> {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, createAttributes.apply(updatedEntity));
            session.transfer(flowFile, REL_SUCCESS);
            // In order to reduce object size, discard meta data captured at the sub-classes.
            final ListedEntity listedEntity = new ListedEntity();
            listedEntity.setTimestamp(updatedEntity.getTimestamp());
            listedEntity.setSize(updatedEntity.getSize());
            alreadyListedEntities.put(identifier, listedEntity);
        });

        // Commit ProcessSession before persisting listed entities.
        // In case persisting listed entities failure, same entities may be listed again, but better than not listing.
        session.commit();
        try {
            logger.trace("Removed old entities: {}, Updated entities: {}", new Object[]{oldEntityIds, updatedEntities});
            persistListedEntities(alreadyListedEntities);
        } catch (IOException e) {
            throw new ProcessException("Failed to persist already-listed entities due to " + e, e);
        }

    }

}
