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

package org.apache.nifi.processors.standard;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.util.EntityListing;
import org.apache.nifi.processors.standard.util.ListableEntity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * <p>
 * An Abstract Processor that is intended to simplify the coding required in order to perform Listing operations of remote resources.
 * Those remote resources may be files, "objects", "messages", or any other sort of entity that may need to be listed in such a way that
 * we identity the entity only once. Each of these objects, messages, etc. is referred to as an "entity" for the scope of this Processor.
 * </p>
 *
 * <p>
 * This class is responsible for triggering the listing to occur, filtering the results returned such that only new (unlisted) entities
 * or entities that have been modified will be emitted from the Processor.
 * </p>
 *
 * <p>
 * In order to make use of this abstract class, the entities listed must meet the following criteria:
 * </p>
 *
 * <ul>
 *  <li>
 *      Entity must have a timestamp associated with it. This timestamp is used to determine if entities are "new" or not. Any entity that is
 *      returned by the listing will be considered "new" if the timestamp is later than the latest timestamp pulled.
 *  </li>
 *  <li>
 *      Entity must have a unique identifier. This is used in conjunction with the timestamp in order to determine whether or not the entity is
 *      new. If the timestamp of an entity is before the latest timestamp pulled, then the entity is not considered new. If the timestamp is later
 *      than the last timestamp pulled, then the entity is considered new. If the timestamp is equal to the latest timestamp pulled, then the entity's
 *      identifier is compared to all of the entity identifiers that have that same timestamp in order to determine whether or not the entity has been
 *      seen already.
 *  </li>
 *  <li>
 *      Entity must have a user-readable name that can be used for logging purposes.
 *  </li>
 * </ul>
 *
 * <p>
 * This class persists state across restarts so that even if NiFi is restarted, duplicates will not be pulled from the remote system. This is performed using
 * two different mechanisms. First, state is stored locally. This allows the system to be restarted and begin processing where it left off. The state that is
 * stored is the latest timestamp that has been pulled (as determined by the timestamps of the entities that are returned), as well as the unique identifier of
 * each entity that has that timestamp. See the section above for information about how these pieces of information are used in order to determine entity uniqueness.
 * </p>
 *
 * <p>
 * In addition to storing state locally, the Processor exposes an optional <code>Distributed Cache Service</code> property. In standalone deployment of NiFi, this is
 * not necessary. However, in a clustered environment, subclasses of this class are expected to be run only on primary node. While this means that the local state is
 * accurate as long as the primary node remains constant, the primary node in the cluster can be changed. As a result, if running in a clustered environment, it is
 * recommended that this property be set. This allows the same state that is described above to also be replicated across the cluster. If this property is set, then
 * on restart the Processor will not begin listing until it has retrieved an updated state from this service, as it does not know whether or not another node has
 * modified the state in the mean time.
 * </p>
 *
 * <p>
 * For each new entity that is listed, the Processor will send a FlowFile to the 'success' relationship. The FlowFile will have no content but will have some set
 * of attributes (defined by the concrete implementation) that can be used to fetch those remote resources or interact with them in whatever way makes sense for
 * the configured dataflow.
 * </p>
 *
 * <p>
 * Subclasses are responsible for the following:
 * </p>
 *
 * <ul>
 * <li>
 * Perform a listing of remote resources. The subclass will implement the {@link #performListing(ProcessContext, Long)} method, which creates a listing of all
 * entities on the remote system that have timestamps later than the provided timestamp. If the entities returned have a timestamp before the provided one, those
 * entities will be filtered out. It is therefore not necessary to perform the filtering of timestamps but is provided in order to give the implementation the ability
 * to filter those resources on the server side rather than pulling back all of the information, if it makes sense to do so in the concrete implementation.
 * </li>
 * <li>
 * Creating a Map of attributes that are applicable for an entity. The attributes that are assigned to each FlowFile are exactly those returned by the
 * {@link #createAttributes(ListableEntity, ProcessContext)}.
 * </li>
 * <li>
 * Returning the configured path. Many resources can be comprised of a "path" (or a "container" or "bucket", etc.) as well as name or identifier that is unique only
 * within that path. The {@link #getPath(ProcessContext)} method is responsible for returning the path that is currently being polled for entities. If this does concept
 * does not apply for the concrete implementation, it is recommended that the concrete implementation return "." or "/" for all invocations of this method.
 * </li>
 * <li>
 * Determining when the listing must be cleared. It is sometimes necessary to clear state about which entities have already been ingested, as the result of a user
 * changing a property value. The {@link #isListingResetNecessary(PropertyDescriptor)} method is responsible for determining when the listing needs to be reset by returning
 * a boolean indicating whether or not a change in the value of the provided property should trigger the timestamp and identifier information to be cleared.
 * </li>
 * </ul>
 */
@TriggerSerially
public abstract class AbstractListProcessor<T extends ListableEntity> extends AbstractProcessor {
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
        .name("Distributed Cache Service")
        .description("Specifies the Controller Service that should be used to maintain state about what has been pulled from the remote server so that if a new node "
            + "begins pulling data, it won't duplicate all of the work that has been done. If not specified, the information will not be shared across the cluster. "
            + "This property does not need to be set for standalone instances of NiFi but should be configured if NiFi is run within a cluster.")
        .required(false)
        .identifiesControllerService(DistributedMapCacheClient.class)
        .build();



    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are received are routed to success")
        .build();


    private volatile Set<String> latestIdentifiersListed = new HashSet<>();
    private volatile Long lastListingTime = null;
    private volatile boolean electedPrimaryNode = false;
    private volatile boolean resetListing = false;

    static final String TIMESTAMP = "timestamp";
    static final String IDENTIFIER_PREFIX = "id";

    protected File getPersistenceFile() {
        return new File("conf/state/" + getIdentifier());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        return properties;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (isListingResetNecessary(descriptor)) {
            lastListingTime = null; // clear lastListingTime so that we have to fetch new time
            latestIdentifiersListed = new HashSet<>();
            resetListing = true;
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange(final PrimaryNodeState newState) {
        if (newState == PrimaryNodeState.ELECTED_PRIMARY_NODE) {
            electedPrimaryNode = true;
        }
    }

    @OnScheduled
    public final void updateState(final ProcessContext context) throws IOException {
        final String path = getPath(context);
        final DistributedMapCacheClient client = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        // Check if state already exists for this path. If so, we have already migrated the state.
        final StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
        if (stateMap.getVersion() == -1L) {
            try {
                // Migrate state from the old way of managing state (distributed cache service and local file)
                // to the new mechanism (State Manager).
                migrateState(path, client, context.getStateManager());
            } catch (final IOException ioe) {
                throw new IOException("Failed to properly migrate state to State Manager", ioe);
            }
        }

        // delete the local file, since it is no longer needed
        final File localFile = new File(path);
        if (localFile.exists() && !!localFile.delete()) {
            getLogger().warn("Migrated state but failed to delete local persistence file");
        }

        // remove entry from Distributed cache server
        if (client != null) {
            try {
                client.remove(path, new StringSerDe());
            } catch (final IOException ioe) {
                getLogger().warn("Failed to remove entry from Distributed Cache Service. However, the state has already been migrated to use the new "
                    + "State Management service, so the Distributed Cache Service is no longer needed.");
            }
        }

        if (resetListing) {
            context.getStateManager().clear(Scope.CLUSTER);
            resetListing = false;
        }
    }

    /**
     * This processor used to use the DistributedMapCacheClient in order to store cluster-wide state, before the introduction of
     * the StateManager. This method will migrate state from that DistributedMapCacheClient, or from a local file, to the StateManager,
     * if any state already exists
     *
     * @param path the path to migrate state for
     * @param client the DistributedMapCacheClient that is capable of obtaining the current state
     * @param stateManager the StateManager to use in order to store the new state
     * @throws IOException if unable to retrieve or store the state
     */
    private void migrateState(final String path, final DistributedMapCacheClient client, final StateManager stateManager) throws IOException {
        Long minTimestamp = null;
        final Set<String> latestIdentifiersListed = new HashSet<>();

        // Retrieve state from Distributed Cache Client
        if (client != null) {
            final StringSerDe serde = new StringSerDe();
            final String serializedState = client.get(getKey(path), serde, serde);
            if (serializedState != null && !serializedState.isEmpty()) {
                final EntityListing listing = deserialize(serializedState);
                minTimestamp = listing.getLatestTimestamp().getTime();
                latestIdentifiersListed.addAll(listing.getMatchingIdentifiers());
            }
        }

        // Retrieve state from locally persisted file
        final File persistenceFile = getPersistenceFile();
        if (persistenceFile.exists()) {
            final Properties props = new Properties();

            try (final FileInputStream fis = new FileInputStream(persistenceFile)) {
                props.load(fis);
            }

            final String locallyPersistedValue = props.getProperty(path);
            if (locallyPersistedValue != null) {
                final EntityListing listing = deserialize(locallyPersistedValue);
                final long localTimestamp = listing.getLatestTimestamp().getTime();
                if (minTimestamp == null || localTimestamp > minTimestamp) {
                    minTimestamp = localTimestamp;
                    latestIdentifiersListed.clear();
                    latestIdentifiersListed.addAll(listing.getMatchingIdentifiers());
                }
            }
        }

        if (minTimestamp != null) {
            persist(minTimestamp, latestIdentifiersListed, stateManager);
        }
    }

    private void persist(final long timestamp, final Collection<String> identifiers, final StateManager stateManager) throws IOException {
        final Map<String, String> updatedState = new HashMap<>(identifiers.size() + 1);
        updatedState.put(TIMESTAMP, String.valueOf(timestamp));
        int counter = 0;
        for (final String identifier : identifiers) {
            final String index = String.valueOf(++counter);
            updatedState.put(IDENTIFIER_PREFIX + "." + index, identifier);
        }
        stateManager.setState(updatedState, Scope.CLUSTER);
    }

    protected String getKey(final String directory) {
        return getIdentifier() + ".lastListingTime." + directory;
    }

    private EntityListing deserialize(final String serializedState) throws JsonParseException, JsonMappingException, IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode jsonNode = mapper.readTree(serializedState);
        return mapper.readValue(jsonNode, EntityListing.class);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        Long minTimestamp = lastListingTime;
        try {
            // We need to fetch the state from the cluster if we don't yet know the last listing time,
            // or if we were just elected the primary node
            if (this.lastListingTime == null || electedPrimaryNode) {
                final StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
                final Map<String, String> stateValues = stateMap.toMap();
                final String timestamp = stateValues.get(TIMESTAMP);

                if (timestamp == null) {
                    minTimestamp = 0L;
                    latestIdentifiersListed.clear();
                } else {
                    minTimestamp = this.lastListingTime = Long.parseLong(timestamp);
                    latestIdentifiersListed.clear();
                    for (final Map.Entry<String, String> entry : stateValues.entrySet()) {
                        final String key = entry.getKey();
                        final String value = entry.getValue();
                        if (TIMESTAMP.equals(key)) {
                            continue;
                        }

                        latestIdentifiersListed.add(value);
                    }
                }
            }
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve timestamp of last listing from Distributed Cache Service. Will not perform listing until this is accomplished.");
            context.yield();
            return;
        }

        final List<T> entityList;
        try {
            entityList = performListing(context, minTimestamp);
        } catch (final IOException e) {
            getLogger().error("Failed to perform listing on remote host due to {}", e);
            context.yield();
            return;
        }

        if (entityList == null) {
            context.yield();
            return;
        }

        int listCount = 0;
        Long latestListingTimestamp = null;
        for (final T entity : entityList) {
            final boolean list = (minTimestamp == null || entity.getTimestamp() > minTimestamp
                || (entity.getTimestamp() == minTimestamp && !latestIdentifiersListed.contains(entity.getIdentifier())));

            // Create the FlowFile for this path.
            if (list) {
                final Map<String, String> attributes = createAttributes(entity, context);
                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_SUCCESS);
                listCount++;

                if (latestListingTimestamp == null || entity.getTimestamp() > latestListingTimestamp) {
                    latestListingTimestamp = entity.getTimestamp();
                }
            }
        }

        if (listCount > 0) {
            getLogger().info("Successfully created listing with {} new objects", new Object[] {listCount});
            session.commit();

            // We have performed a listing and pushed the FlowFiles out.
            // Now, we need to persist state about the Last Modified timestamp of the newest file
            // that we pulled in. We do this in order to avoid pulling in the same file twice.
            // However, we want to save the state both locally and remotely.
            // We store the state remotely so that if a new Primary Node is chosen, it can pick up where the
            // previously Primary Node left off.
            // We also store the state locally so that if the node is restarted, and the node cannot contact
            // the distributed state cache, the node can continue to run (if it is primary node).
            final Set<String> identifiers = new HashSet<>(entityList.size());
            try {
                for (final T entity : entityList) {
                    identifiers.add(entity.getIdentifier());
                }
                persist(latestListingTimestamp, identifiers, context.getStateManager());
            } catch (final IOException ioe) {
                getLogger().warn("Unable to save state due to {}. If NiFi restarted before state is saved, or "
                    + "if another node begins executing this Processor, data duplication may occur.", ioe);
            }

            lastListingTime = latestListingTimestamp;
            latestIdentifiersListed = identifiers;
        } else {
            getLogger().debug("There is no data to list. Yielding.");
            context.yield();

            // lastListingTime = 0 so that we don't continually poll the distributed cache / local file system
            if (lastListingTime == null) {
                lastListingTime = 0L;
            }

            return;
        }
    }


    /**
     * Creates a Map of attributes that should be applied to the FlowFile to represent this entity. This processor will emit a FlowFile for each "new" entity
     * (see the documentation for this class for a discussion of how this class determines whether or not an entity is "new"). The FlowFile will contain no
     * content. The attributes that will be included are exactly the attributes that are returned by this method.
     *
     * @param entity the entity represented by the FlowFile
     * @param context the ProcessContext for obtaining configuration information
     * @return a Map of attributes for this entity
     */
    protected abstract Map<String, String> createAttributes(T entity, ProcessContext context);

    /**
     * Returns the path to perform a listing on.
     * Many resources can be comprised of a "path" (or a "container" or "bucket", etc.) as well as name or identifier that is unique only
     * within that path. This method is responsible for returning the path that is currently being polled for entities. If this does concept
     * does not apply for the concrete implementation, it is recommended that the concrete implementation return "." or "/" for all invocations of this method.
     *
     * @param context the ProcessContex to use in order to obtain configuration
     * @return the path that is to be used to perform the listing, or <code>null</code> if not applicable.
     */
    protected abstract String getPath(final ProcessContext context);

    /**
     * Performs a listing of the remote entities that can be pulled. If any entity that is returned has already been "discovered" or "emitted"
     * by this Processor, it will be ignored. A discussion of how the Processor determines those entities that have already been emitted is
     * provided above in the documentation for this class. Any entity that is returned by this method with a timestamp prior to the minTimestamp
     * will be filtered out by the Processor. Therefore, it is not necessary that implementations perform this filtering but can be more efficient
     * if the filtering can be performed on the server side prior to retrieving the information.
     *
     * @param context the ProcessContex to use in order to pull the appropriate entities
     * @param minTimestamp the minimum timestamp of entities that should be returned.
     *
     * @return a Listing of entities that have a timestamp >= minTimestamp
     */
    protected abstract List<T> performListing(final ProcessContext context, final Long minTimestamp) throws IOException;

    /**
     * Determines whether or not the listing must be reset if the value of the given property is changed
     *
     * @param property the property that has changed
     * @return <code>true</code> if a change in value of the given property necessitates that the listing be reset, <code>false</code> otherwise.
     */
    protected abstract boolean isListingResetNecessary(final PropertyDescriptor property);

    /**
     * Returns a Scope that specifies where the state should be managed for this Processor
     *
     * @param context the ProcessContext to use in order to make a determination
     * @return a Scope that specifies where the state should be managed for this Processor
     */
    protected abstract Scope getStateScope(final ProcessContext context);


    private static class StringSerDe implements Serializer<String>, Deserializer<String> {
        @Override
        public String deserialize(final byte[] value) throws DeserializationException, IOException {
            if (value == null) {
                return null;
            }

            return new String(value, StandardCharsets.UTF_8);
        }

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
