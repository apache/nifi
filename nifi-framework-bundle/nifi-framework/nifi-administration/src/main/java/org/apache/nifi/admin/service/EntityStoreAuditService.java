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
package org.apache.nifi.admin.service;

import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterable;
import jetbrains.exodus.entitystore.PersistentEntityStore;
import jetbrains.exodus.entitystore.PersistentEntityStores;
import jetbrains.exodus.entitystore.StoreTransaction;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.component.details.ComponentDetails;
import org.apache.nifi.action.component.details.ExtensionDetails;
import org.apache.nifi.action.component.details.FlowChangeExtensionDetails;
import org.apache.nifi.action.component.details.FlowChangeRemoteProcessGroupDetails;
import org.apache.nifi.action.component.details.RemoteProcessGroupDetails;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.ConfigureDetails;
import org.apache.nifi.action.details.ConnectDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.action.details.FlowChangeConnectDetails;
import org.apache.nifi.action.details.FlowChangeMoveDetails;
import org.apache.nifi.action.details.FlowChangePurgeDetails;
import org.apache.nifi.action.details.MoveDetails;
import org.apache.nifi.action.details.PurgeDetails;
import org.apache.nifi.admin.service.entity.ActionEntity;
import org.apache.nifi.admin.service.entity.ActionLink;
import org.apache.nifi.admin.service.entity.ConfigureDetailsEntity;
import org.apache.nifi.admin.service.entity.ConnectDetailsEntity;
import org.apache.nifi.admin.service.entity.EntityProperty;
import org.apache.nifi.admin.service.entity.EntityType;
import org.apache.nifi.admin.service.entity.MoveDetailsEntity;
import org.apache.nifi.admin.service.entity.PurgeDetailsEntity;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Audit Service implementation based on JetBrains Xodus Entity Store
 */
public class EntityStoreAuditService implements AuditService, Closeable {
    private static final long FIRST_START_TIME = 0;

    private static final int PREVIOUS_VALUES_LIMIT = 5;

    private static final String ASCENDING_SORT_ORDER = "ASC";

    private static final int DEFAULT_COUNT = 100;

    private static final String BACKUP_FILE_NAME_FORMAT = "%s.backup.%d";

    private static final Logger logger = LoggerFactory.getLogger(EntityStoreAuditService.class);

    private final PersistentEntityStore entityStore;

    private final Environment environment;

    /**
     * Entity Store Audit Service constructor with required properties for persistent location
     *
     * @param directory Persistent Entity Store directory
     */
    public EntityStoreAuditService(final File directory) {
        environment = loadEnvironment(directory);
        entityStore = PersistentEntityStores.newInstance(environment);
        logger.info("Environment configured with directory [{}]", directory);
    }

    /**
     * Add Actions to Persistent Store
     *
     * @param actions Collections of Actions to be added
     */
    @Override
    public void addActions(final Collection<Action> actions) {
        Objects.requireNonNull(actions, "Actions required");

        entityStore.executeInExclusiveTransaction(storeTransaction -> {
            for (final Action action : actions) {
                addAction(storeTransaction, action);
            }
            logger.debug("Actions added [{}]", actions.size());
        });
    }

    /**
     * Get Previous Values for specified Component Identifier
     *
     * @param componentId Component Identifier for which previous property values should be retrieved
     * @return Map of Property Name to List of Previous Property Values
     */
    @Override
    public Map<String, List<PreviousValue>> getPreviousValues(final String componentId) {
        Objects.requireNonNull(componentId, "Component Identifier required");

        return entityStore.computeInReadonlyTransaction(storeTransaction -> {
            final Map<String, List<PreviousValue>> previousValuesFound = new LinkedHashMap<>();

            final EntityIterable actionEntities = storeTransaction.find(EntityType.ACTION.getEntityType(), ActionEntity.SOURCE_ID.getProperty(), componentId);
            // Reverse default ordering to return oldest entries before newest entries
            for (Entity actionEntity : actionEntities.reverse()) {
                final Entity configureDetails = actionEntity.getLink(ActionLink.CONFIGURE_DETAILS.getProperty());
                if (configureDetails != null) {
                    final String name = getProperty(configureDetails, ConfigureDetailsEntity.NAME);
                    final String value = getProperty(configureDetails, ConfigureDetailsEntity.VALUE);

                    final PreviousValue previousValue = new PreviousValue();
                    previousValue.setPreviousValue(value);
                    previousValue.setUserIdentity(getProperty(actionEntity, ActionEntity.USER_IDENTITY));
                    previousValue.setTimestamp(getDateProperty(actionEntity, ActionEntity.TIMESTAMP));

                    final List<PreviousValue> previousValues = previousValuesFound.get(name);
                    if (previousValues == null) {
                        final List<PreviousValue> newPreviousValues = new ArrayList<>();
                        newPreviousValues.add(previousValue);
                        previousValuesFound.put(name, newPreviousValues);
                    } else if (previousValues.size() < PREVIOUS_VALUES_LIMIT) {
                        previousValues.add(previousValue);
                    }
                }
            }

            return previousValuesFound;
        });
    }

    /**
     * Delete Previous Values for specified Component Identifier and Property
     *
     * @param propertyName Name of the property for which Previous Values should be deleted
     * @param componentId  Component Identifier for which Previous Values should be deleted
     */
    @Override
    public void deletePreviousValues(final String propertyName, final String componentId) {
        Objects.requireNonNull(propertyName, "Property Name required");
        Objects.requireNonNull(componentId, "Component Identifier required");

        entityStore.executeInExclusiveTransaction(storeTransaction -> {
            final EntityIterable actionEntities = storeTransaction.find(EntityType.ACTION.getEntityType(), ActionEntity.SOURCE_ID.getProperty(), componentId);
            for (Entity actionEntity : actionEntities) {
                final Entity configureDetails = actionEntity.getLink(ActionLink.CONFIGURE_DETAILS.getProperty());
                if (configureDetails != null) {
                    final Comparable<?> configureDetailsName = configureDetails.getProperty(ConfigureDetailsEntity.NAME.getProperty());
                    if (propertyName.equals(configureDetailsName)) {
                        actionEntity.deleteLinks(ActionLink.CONFIGURE_DETAILS.getProperty());
                    }
                }
            }
        });

        logger.info("Component [{}] Previous Property Values deleted", componentId);
    }

    /**
     * Get Actions based on query parameters
     *
     * @param actionQuery History Action Query
     * @return Actions found
     */
    @Override
    public History getActions(final HistoryQuery actionQuery) {
        Objects.requireNonNull(actionQuery, "History Query required");

        return entityStore.computeInReadonlyTransaction(storeTransaction -> {
            final Collection<Action> actionsFound = new ArrayList<>();
            final EntityIterable entities = findActionEntities(actionQuery, storeTransaction);
            final int total = Math.toIntExact(entities.size());

            final Integer queryOffset = actionQuery.getOffset();
            final int skip = Objects.requireNonNullElse(queryOffset, 0);

            final Integer queryCount = actionQuery.getCount();
            final int limit = Objects.requireNonNullElse(queryCount, DEFAULT_COUNT);

            final EntityIterable selected = entities.skip(skip).take(limit);
            for (final Entity entity : selected) {
                final Action action = readAction(entity);
                actionsFound.add(action);
            }

            final History history = new History();
            history.setActions(actionsFound);
            history.setTotal(total);
            history.setLastRefreshed(new Date());
            return history;
        });
    }

    /**
     * Get Actions starting with indicated Action Identifier
     *
     * @param firstActionId First Action Identifier
     * @param maxActions Maximum number of Actions to be returned
     * @return Actions found
     */
    @Override
    public History getActions(final int firstActionId, final int maxActions) {
        return entityStore.computeInReadonlyTransaction(storeTransaction -> {
            final Collection<Action> actions = new ArrayList<>();

            final int lastActionId = firstActionId + maxActions;
            final EntityIterable found = storeTransaction.findIds(EntityType.ACTION.getEntityType(), firstActionId, lastActionId);

            for (final Entity entity : found) {
                final Action action = readAction(entity);
                actions.add(action);
            }

            final History history = new History();
            history.setActions(actions);
            history.setLastRefreshed(new Date());
            history.setTotal(actions.size());

            return history;
        });
    }

    /**
     * Get Action for specified identifier
     *
     * @param actionId Action Identifier
     * @return Action or null when not found
     */
    @Override
    public Action getAction(final Integer actionId) {
        Objects.requireNonNull(actionId, "Action Identifier required");

        return entityStore.computeInReadonlyTransaction(storeTransaction -> {
            final Action action;

            final EntityIterable found = storeTransaction.findIds(EntityType.ACTION.getEntityType(), actionId, actionId);
            final Entity entity = found.getFirst();
            if (entity == null) {
                action = null;
            } else {
                action = readAction(entity);
            }

            return action;
        });
    }

    /**
     * Purge Actions from Persistent Entity Store clearing existing records and creating Purge Action
     *
     * @param end End time for purge action query
     * @param purgeAction Purge Action to be recorded
     */
    @Override
    public void purgeActions(final Date end, final Action purgeAction) {
        Objects.requireNonNull(end, "End date required");
        Objects.requireNonNull(purgeAction, "Purge Action required");

        final long endTime = end.getTime();
        entityStore.executeInExclusiveTransaction(storeTransaction -> {
            final EntityIterable entities = storeTransaction.find(EntityType.ACTION.getEntityType(), ActionEntity.TIMESTAMP.getProperty(), FIRST_START_TIME, endTime);
            for (final Entity entity : entities) {
                entity.delete();

                for (final ActionLink actionLink : ActionLink.values()) {
                    entity.deleteLinks(actionLink.getProperty());
                }
            }

            addAction(storeTransaction, purgeAction);
        });

        logger.info("User [{}] Purged Actions with end date [{}]", purgeAction.getUserIdentity(), end);
    }

    /**
     * Close Persistent Entity Store resources
     *
     * @throws IOException Thrown on failures to close Entity Store
     */
    @Override
    public void close() throws IOException {
        entityStore.close();
        environment.close();
        logger.info("Environment closed");
    }

    private EntityIterable findActionEntities(final HistoryQuery actionQuery, final StoreTransaction storeTransaction) {
        final long startTimestamp;
        final long endTimestamp;

        final Date startDate = actionQuery.getStartDate();
        if (startDate == null) {
            startTimestamp = FIRST_START_TIME;
        } else {
            startTimestamp = startDate.getTime();
        }

        final Date endDate = actionQuery.getEndDate();
        if (endDate == null) {
            endTimestamp = System.currentTimeMillis();
        } else {
            endTimestamp = endDate.getTime();
        }

        final EntityIterable entities = storeTransaction.find(EntityType.ACTION.getEntityType(), ActionEntity.TIMESTAMP.getProperty(), startTimestamp, endTimestamp);

        final EntityIterable sourceEntities;
        final String sourceId = actionQuery.getSourceId();
        if (sourceId == null) {
            sourceEntities = entities;
        } else {
            final EntityIterable sourceFiltered = storeTransaction.find(EntityType.ACTION.getEntityType(), ActionEntity.SOURCE_ID.getProperty(), sourceId);
            sourceEntities = entities.intersect(sourceFiltered);
        }

        final EntityIterable filteredEntities;
        final String userIdentity = actionQuery.getUserIdentity();
        if (userIdentity == null) {
            filteredEntities = sourceEntities;
        } else {
            final EntityIterable identityFiltered = storeTransaction.find(EntityType.ACTION.getEntityType(), ActionEntity.USER_IDENTITY.getProperty(), userIdentity);
            filteredEntities = sourceEntities.intersect(identityFiltered);
        }

        final ActionEntity sortEntityProperty = getSortEntityProperty(actionQuery);
        final boolean ascending = isAscending(actionQuery);
        return storeTransaction.sort(EntityType.ACTION.getEntityType(), sortEntityProperty.getProperty(), filteredEntities, ascending);
    }

    private boolean isAscending(final HistoryQuery historyQuery) {
        final String sortOrder = historyQuery.getSortOrder();

        final boolean ascending;
        if (sortOrder == null || sortOrder.isEmpty()) {
            ascending = false;
        } else {
            ascending = ASCENDING_SORT_ORDER.equalsIgnoreCase(sortOrder);
        }

        return ascending;
    }

    private ActionEntity getSortEntityProperty(final HistoryQuery historyQuery) {
        final String sortColumn = historyQuery.getSortColumn();

        final ActionEntity sortEntityProperty;

        if (sortColumn == null || sortColumn.isEmpty()) {
            sortEntityProperty = ActionEntity.TIMESTAMP;
        } else {
            ActionEntity foundActionEntity = null;
            for (final ActionEntity actionEntity : ActionEntity.values()) {
                if (actionEntity.getProperty().equals(sortColumn)) {
                    foundActionEntity = actionEntity;
                    break;
                }
            }

            if (foundActionEntity == null) {
                throw new IllegalArgumentException("Specified Sort Column not supported");
            } else {
                sortEntityProperty = foundActionEntity;
            }
        }


        return sortEntityProperty;
    }

    private void addAction(final StoreTransaction storeTransaction, final Action action) {
        final Entity actionEntity = storeTransaction.newEntity(EntityType.ACTION.getEntityType());
        actionEntity.setProperty(ActionEntity.TIMESTAMP.getProperty(), action.getTimestamp().getTime());
        actionEntity.setProperty(ActionEntity.USER_IDENTITY.getProperty(), action.getUserIdentity());
        actionEntity.setProperty(ActionEntity.SOURCE_ID.getProperty(), action.getSourceId());
        actionEntity.setProperty(ActionEntity.SOURCE_NAME.getProperty(), action.getSourceName());
        actionEntity.setProperty(ActionEntity.SOURCE_TYPE.getProperty(), action.getSourceType().name());
        actionEntity.setProperty(ActionEntity.OPERATION.getProperty(), action.getOperation().name());

        final ComponentDetails componentDetails = action.getComponentDetails();
        addComponentDetails(actionEntity, componentDetails);

        final ActionDetails actionDetails = action.getActionDetails();
        addActionDetails(storeTransaction, actionEntity, actionDetails);
    }

    private void addComponentDetails(final Entity actionEntity, final ComponentDetails componentDetails) {
        if (componentDetails instanceof ExtensionDetails extensionDetails) {
            actionEntity.setProperty(ActionEntity.EXTENSION_TYPE.getProperty(), extensionDetails.getType());
        } else if (componentDetails instanceof RemoteProcessGroupDetails remoteProcessGroupDetails) {
            actionEntity.setProperty(ActionEntity.REMOTE_PROCESS_GROUP_URI.getProperty(), remoteProcessGroupDetails.getUri());
        }
    }

    private void addActionDetails(final StoreTransaction storeTransaction, final Entity actionEntity, final ActionDetails actionDetails) {
        if (actionDetails instanceof ConnectDetails connectDetails) {
            addConnectDetails(storeTransaction, actionEntity, connectDetails);
        } else if (actionDetails instanceof MoveDetails moveDetails) {
            addMoveDetails(storeTransaction, actionEntity, moveDetails);
        } else if (actionDetails instanceof ConfigureDetails configureDetails) {
            addConfigureDetails(storeTransaction, actionEntity, configureDetails);
        } else if (actionDetails instanceof PurgeDetails purgeDetails) {
            addPurgeDetails(storeTransaction, actionEntity, purgeDetails);
        }
    }

    private void addConnectDetails(final StoreTransaction storeTransaction, final Entity actionEntity, final ConnectDetails connectDetails) {
        final Entity connectDetailsEntity = storeTransaction.newEntity(EntityType.CONNECT_DETAILS.getEntityType());
        connectDetailsEntity.setLink(ConnectDetailsEntity.ACTION.getProperty(), actionEntity);
        actionEntity.setLink(ActionLink.CONNECT_DETAILS.getProperty(), connectDetailsEntity);

        connectDetailsEntity.setProperty(ConnectDetailsEntity.SOURCE_ID.getProperty(), connectDetails.getSourceId());
        connectDetailsEntity.setProperty(ConnectDetailsEntity.SOURCE_TYPE.getProperty(), connectDetails.getSourceType().name());
        if (connectDetails.getSourceName() != null) {
            connectDetailsEntity.setProperty(ConnectDetailsEntity.SOURCE_NAME.getProperty(), connectDetails.getSourceName());
        }

        connectDetailsEntity.setProperty(ConnectDetailsEntity.DESTINATION_ID.getProperty(), connectDetails.getDestinationId());
        connectDetailsEntity.setProperty(ConnectDetailsEntity.DESTINATION_TYPE.getProperty(), connectDetails.getDestinationType().name());
        if (connectDetails.getDestinationName() != null) {
            connectDetailsEntity.setProperty(ConnectDetailsEntity.DESTINATION_NAME.getProperty(), connectDetails.getDestinationName());
        }

        if (connectDetails.getRelationship() != null) {
            connectDetailsEntity.setProperty(ConnectDetailsEntity.RELATIONSHIP.getProperty(), connectDetails.getRelationship());
        }
    }

    private void addMoveDetails(final StoreTransaction storeTransaction, final Entity actionEntity, final MoveDetails moveDetails) {
        final Entity moveDetailsEntity = storeTransaction.newEntity(EntityType.MOVE_DETAILS.getEntityType());
        moveDetailsEntity.setLink(MoveDetailsEntity.ACTION.getProperty(), actionEntity);
        actionEntity.setLink(ActionLink.MOVE_DETAILS.getProperty(), moveDetailsEntity);

        moveDetailsEntity.setProperty(MoveDetailsEntity.GROUP.getProperty(), moveDetails.getGroup());
        moveDetailsEntity.setProperty(MoveDetailsEntity.GROUP_ID.getProperty(), moveDetails.getGroupId());
        moveDetailsEntity.setProperty(MoveDetailsEntity.PREVIOUS_GROUP.getProperty(), moveDetails.getPreviousGroup());
        moveDetailsEntity.setProperty(MoveDetailsEntity.PREVIOUS_GROUP_ID.getProperty(), moveDetails.getPreviousGroupId());
    }

    private void addConfigureDetails(final StoreTransaction storeTransaction, final Entity actionEntity, final ConfigureDetails configureDetails) {
        final Entity configureDetailsEntity = storeTransaction.newEntity(EntityType.CONFIGURE_DETAILS.getEntityType());
        configureDetailsEntity.setLink(MoveDetailsEntity.ACTION.getProperty(), actionEntity);
        actionEntity.setLink(ActionLink.CONFIGURE_DETAILS.getProperty(), configureDetailsEntity);

        configureDetailsEntity.setProperty(ConfigureDetailsEntity.NAME.getProperty(), configureDetails.getName());

        final String previousValue = configureDetails.getPreviousValue();
        if (previousValue != null) {
            configureDetailsEntity.setProperty(ConfigureDetailsEntity.PREVIOUS_VALUE.getProperty(), previousValue);
        }

        final String value = configureDetails.getValue();
        if (value != null) {
            configureDetailsEntity.setProperty(ConfigureDetailsEntity.VALUE.getProperty(), value);
        }
    }

    private void addPurgeDetails(final StoreTransaction storeTransaction, final Entity actionEntity, final PurgeDetails purgeDetails) {
        final Entity purgeDetailsEntity = storeTransaction.newEntity(EntityType.PURGE_DETAILS.getEntityType());
        purgeDetailsEntity.setLink(PurgeDetailsEntity.ACTION.getProperty(), actionEntity);
        actionEntity.setLink(ActionLink.PURGE_DETAILS.getProperty(), purgeDetailsEntity);

        purgeDetailsEntity.setProperty(PurgeDetailsEntity.END_DATE.getProperty(), purgeDetails.getEndDate().getTime());
    }

    private Action readAction(final Entity entity) {
        final FlowChangeAction action = new FlowChangeAction();

        final EntityId entityId = entity.getId();
        final int id = Math.toIntExact(entityId.getLocalId());
        action.setId(id);

        action.setUserIdentity(getProperty(entity, ActionEntity.USER_IDENTITY));
        action.setSourceId(getProperty(entity, ActionEntity.SOURCE_ID));
        action.setSourceName(getProperty(entity, ActionEntity.SOURCE_NAME));
        action.setTimestamp(getDateProperty(entity, ActionEntity.TIMESTAMP));
        action.setSourceType(getEnumProperty(entity, ActionEntity.SOURCE_TYPE, Component.class));
        action.setOperation(getEnumProperty(entity, ActionEntity.OPERATION, Operation.class));

        final String extensionType = getProperty(entity, ActionEntity.EXTENSION_TYPE);
        if (extensionType != null) {
            final FlowChangeExtensionDetails extensionDetails = new FlowChangeExtensionDetails();
            extensionDetails.setType(extensionType);
            action.setComponentDetails(extensionDetails);
        }

        final String remoteProgressGroupUri = getProperty(entity, ActionEntity.REMOTE_PROCESS_GROUP_URI);
        if (remoteProgressGroupUri != null) {
            final FlowChangeRemoteProcessGroupDetails remoteProcessGroupDetails = new FlowChangeRemoteProcessGroupDetails();
            remoteProcessGroupDetails.setUri(remoteProgressGroupUri);
            action.setComponentDetails(remoteProcessGroupDetails);
        }

        final Entity purgeDetailsEntity = entity.getLink(ActionLink.PURGE_DETAILS.getProperty());
        if (purgeDetailsEntity != null) {
            final FlowChangePurgeDetails purgeDetails = new FlowChangePurgeDetails();
            purgeDetails.setEndDate(getDateProperty(purgeDetailsEntity, PurgeDetailsEntity.END_DATE));
            action.setActionDetails(purgeDetails);
        }
        final Entity configureDetailsEntity = entity.getLink(ActionLink.CONFIGURE_DETAILS.getProperty());
        if (configureDetailsEntity != null) {
            final ConfigureDetails configureDetails = getConfigureDetails(configureDetailsEntity);
            action.setActionDetails(configureDetails);
        }
        final Entity connectDetailsEntity = entity.getLink(ActionLink.CONNECT_DETAILS.getProperty());
        if (connectDetailsEntity != null) {
            final ConnectDetails connectDetails = getConnectDetails(connectDetailsEntity);
            action.setActionDetails(connectDetails);
        }
        final Entity moveDetailsEntity = entity.getLink(ActionLink.MOVE_DETAILS.getProperty());
        if (moveDetailsEntity != null) {
            final MoveDetails moveDetails = getMoveDetails(moveDetailsEntity);
            action.setActionDetails(moveDetails);
        }

        return action;
    }

    private ConfigureDetails getConfigureDetails(final Entity configureDetailsEntity) {
        final FlowChangeConfigureDetails configureDetails = new FlowChangeConfigureDetails();

        configureDetails.setName(getProperty(configureDetailsEntity, ConfigureDetailsEntity.NAME));
        configureDetails.setPreviousValue(getProperty(configureDetailsEntity, ConfigureDetailsEntity.PREVIOUS_VALUE));
        configureDetails.setValue(getProperty(configureDetailsEntity, ConfigureDetailsEntity.VALUE));

        return configureDetails;
    }

    private ConnectDetails getConnectDetails(final Entity connectDetailsEntity) {
        final FlowChangeConnectDetails connectDetails = new FlowChangeConnectDetails();

        connectDetails.setSourceId(getProperty(connectDetailsEntity, ConnectDetailsEntity.SOURCE_ID));
        connectDetails.setSourceName(getProperty(connectDetailsEntity, ConnectDetailsEntity.SOURCE_NAME));
        connectDetails.setSourceType(getEnumProperty(connectDetailsEntity, ConnectDetailsEntity.SOURCE_TYPE, Component.class));
        connectDetails.setDestinationId(getProperty(connectDetailsEntity, ConnectDetailsEntity.DESTINATION_ID));
        connectDetails.setDestinationName(getProperty(connectDetailsEntity, ConnectDetailsEntity.DESTINATION_NAME));
        connectDetails.setDestinationType(getEnumProperty(connectDetailsEntity, ConnectDetailsEntity.DESTINATION_TYPE, Component.class));
        connectDetails.setRelationship(getProperty(connectDetailsEntity, ConnectDetailsEntity.RELATIONSHIP));

        return connectDetails;
    }

    private MoveDetails getMoveDetails(final Entity moveDetailsEntity) {
        final FlowChangeMoveDetails moveDetails = new FlowChangeMoveDetails();

        moveDetails.setGroup(getProperty(moveDetailsEntity, MoveDetailsEntity.GROUP));
        moveDetails.setGroupId(getProperty(moveDetailsEntity, MoveDetailsEntity.GROUP_ID));
        moveDetails.setPreviousGroup(getProperty(moveDetailsEntity, MoveDetailsEntity.PREVIOUS_GROUP));
        moveDetails.setPreviousGroupId(getProperty(moveDetailsEntity, MoveDetailsEntity.PREVIOUS_GROUP_ID));

        return moveDetails;
    }

    private String getProperty(final Entity entity, final EntityProperty entityProperty) {
        final Comparable<?> property = entity.getProperty(entityProperty.getProperty());
        return property == null ? null : property.toString();
    }

    private <T extends Enum<T>> T getEnumProperty(final Entity entity, final EntityProperty entityProperty, final Class<T> enumType) {
        final Comparable<?> property = entity.getProperty(entityProperty.getProperty());
        return property == null ? null : Enum.valueOf(enumType, property.toString());
    }

    private Date getDateProperty(final Entity entity, final EntityProperty entityProperty) {
        final Comparable<?> property = entity.getProperty(entityProperty.getProperty());

        final Date dateProperty;

        if (property instanceof Long) {
            final long milliseconds = (Long) property;
            dateProperty = new Date(milliseconds);
        } else {
            dateProperty = null;
        }

        return dateProperty;
    }

    private Environment loadEnvironment(final File directory) {
        final EnvironmentConfig environmentConfig = new EnvironmentConfig();

        Environment loadedEnvironment;
        try {
            loadedEnvironment = Environments.newInstance(directory, environmentConfig);
        } catch (final Exception e) {
            logger.warn("Environment loading failed with directory [{}]", directory, e);

            try (Stream<Path> files = Files.list(directory.toPath())) {
                final List<Path> environmentFiles = files.filter(Files::isRegularFile).toList();
                final long now = System.currentTimeMillis();
                for (final Path environmentFile : environmentFiles) {
                    final String backupFileName = String.format(BACKUP_FILE_NAME_FORMAT, environmentFile.getFileName().toString(), now);
                    final Path backupStoreFile = environmentFile.resolveSibling(backupFileName);
                    try {
                        Files.move(environmentFile, backupStoreFile, StandardCopyOption.REPLACE_EXISTING);
                        logger.warn("Moved Environment file [{}] to [{}]", environmentFile, backupStoreFile);
                    } catch (final IOException ioe) {
                        throw new UncheckedIOException(String.format("Environment file move failed [%s]", environmentFile), ioe);
                    }
                }
            } catch (final IOException ioe) {
                throw new UncheckedIOException(String.format("Environment directory listing failed [%s]", directory), ioe);
            }

            // Retry loading after directory cleanup
            loadedEnvironment = Environments.newInstance(directory, environmentConfig);
        }

        return loadedEnvironment;
    }
}
