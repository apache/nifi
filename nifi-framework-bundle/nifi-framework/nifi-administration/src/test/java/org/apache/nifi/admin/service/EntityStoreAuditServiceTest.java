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

import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.ConnectDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.action.details.FlowChangeConnectDetails;
import org.apache.nifi.action.details.FlowChangePurgeDetails;
import org.apache.nifi.action.details.PurgeDetails;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityStoreAuditServiceTest {

    private static final Date ACTION_TIMESTAMP = new Date(86400);

    private static final Date SECOND_ACTION_TIMESTAMP = new Date(97500);

    private static final Date THIRD_ACTION_TIMESTAMP = new Date(172800);

    private static final Date PURGE_END_DATE = new Date(43200);

    private static final String USER_IDENTITY = "admin";

    private static final String SOURCE_ID = "00000000-0000-0000-0000-000000000000";

    private static final String SECOND_SOURCE_ID = "01010101-0000-0000-0000-000000000000";

    private static final String SOURCE_NAME = "GenerateFlowFile";

    private static final Component SOURCE_TYPE = Component.Processor;

    private static final String DESTINATION_NAME = "UpdateCounter";

    private static final Component DESTINATION_TYPE = Component.Funnel;

    private static final String RELATIONSHIP = "success";

    private static final Operation OPERATION = Operation.Add;

    private static final int ACTION_ID = 0;

    private static final int PURGE_ACTIONS = 1;

    private static final String FIRST_PROPERTY_NAME = "FirstProperty";

    private static final String SECOND_PROPERTY_NAME = "SecondProperty";

    private static final String FIRST_VALUE = "FirstValue";

    private static final String SECOND_VALUE = "SecondValue";

    private static final String THIRD_VALUE = "ThirdValue";

    private static final String DATABASE_FILE_EXTENSION = ".xd";

    private static final String SORT_ASCENDING = "ASC";

    @TempDir
    File directory;

    private EntityStoreAuditService service;

    @BeforeEach
    void setService() {
        service = new EntityStoreAuditService(directory);
    }

    @AfterEach
    void closeService() throws IOException {
        service.close();
    }

    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "Moving the database lock file on Windows causes exceptions")
    @Test
    void testCorruptedFilesHandling() throws IOException {
        service.close();

        // Write invalid string
        try (Stream<Path> files = Files.list(directory.toPath()).filter(file -> file.toString().endsWith(DATABASE_FILE_EXTENSION))) {
            final Optional<Path> databaseFileFound = files.findFirst();
            assertTrue(databaseFileFound.isPresent());

            final Path databaseFile = databaseFileFound.get();
            Files.writeString(databaseFile, SOURCE_ID);
        }

        // Create Service with corrupted directory
        service = new EntityStoreAuditService(directory);
        final Action action = newAction();
        final Collection<Action> actions = Collections.singletonList(action);
        service.addActions(actions);
    }

    @Test
    void testGetActionNotFound() {
        final Action action = service.getAction(ACTION_ID);

        assertNull(action);
    }

    @Test
    void testAddActionsGetAction() {
        final Action action = newAction();
        final Collection<Action> actions = Collections.singletonList(action);

        service.addActions(actions);

        final Action actionFound = service.getAction(ACTION_ID);
        assertEquals(ACTION_ID, actionFound.getId());
        assertActionFound(actionFound);
    }

    @Test
    void testAddActionsGetActions() {
        final Action firstAction = newAction();
        final Action secondAction = newAction();
        final Collection<Action> actions = Arrays.asList(firstAction, secondAction);

        service.addActions(actions);

        final History actionsHistory = service.getActions(ACTION_ID, actions.size());

        assertNotNull(actionsHistory);
        assertEquals(actions.size(), actionsHistory.getTotal());
        assertNotNull(actionsHistory.getLastRefreshed());

        final Collection<Action> actionsFound = actionsHistory.getActions();
        assertNotNull(actionsFound);
        for (final Action actionFound : actionsFound) {
            assertActionFound(actionFound);
        }
    }

    @Test
    void testAddActionsGetActionsQueryUnfiltered() {
        final Action firstAction = newAction();
        final Action secondAction = newAction();
        final Collection<Action> actions = Arrays.asList(firstAction, secondAction);

        service.addActions(actions);

        final HistoryQuery historyQuery = new HistoryQuery();
        final History actionsHistory = service.getActions(historyQuery);

        assertNotNull(actionsHistory);
        assertEquals(actions.size(), actionsHistory.getTotal());
        assertNotNull(actionsHistory.getLastRefreshed());

        final Collection<Action> actionsFound = actionsHistory.getActions();
        assertNotNull(actionsFound);
        for (final Action actionFound : actionsFound) {
            assertActionFound(actionFound);
        }
    }

    @Test
    void testAddActionsGetActionsQuerySourceNotFound() {
        final Action firstAction = newAction();
        final Action secondAction = newAction();
        final Collection<Action> actions = Arrays.asList(firstAction, secondAction);

        service.addActions(actions);

        final HistoryQuery historyQuery = new HistoryQuery();
        historyQuery.setSourceId(SECOND_SOURCE_ID);

        final History actionsHistory = service.getActions(historyQuery);

        assertNotNull(actionsHistory);
        assertEquals(0, actionsHistory.getTotal());
        assertNotNull(actionsHistory.getLastRefreshed());

        final Collection<Action> actionsFound = actionsHistory.getActions();
        assertNotNull(actionsFound);
        assertTrue(actionsFound.isEmpty());
    }

    @Test
    void testAddActionsGetActionsQueryStartDateFiltered() {
        final FlowChangeAction firstAction = newAction();
        final FlowChangeAction secondAction = newAction();
        secondAction.setTimestamp(SECOND_ACTION_TIMESTAMP);
        final Collection<Action> actions = Arrays.asList(firstAction, secondAction);

        service.addActions(actions);

        final HistoryQuery historyQuery = new HistoryQuery();
        historyQuery.setStartDate(SECOND_ACTION_TIMESTAMP);
        final History actionsHistory = service.getActions(historyQuery);

        assertNotNull(actionsHistory);
        assertEquals(actionsHistory.getTotal(), 1);
        assertNotNull(actionsHistory.getLastRefreshed());

        final Collection<Action> actionsFound = actionsHistory.getActions();
        assertNotNull(actionsFound);

        final Iterator<Action> actionsFiltered = actionsFound.iterator();
        assertTrue(actionsFiltered.hasNext());

        final Action firstActionFound = actionsFiltered.next();
        assertEquals(SECOND_ACTION_TIMESTAMP, firstActionFound.getTimestamp());

        assertFalse(actionsFiltered.hasNext());
    }

    @Test
    void testAddActionsGetActionsQueryTimestampSortedSourceIdFiltered() {
        final FlowChangeAction firstAction = newAction();
        final FlowChangeAction secondAction = newAction();
        secondAction.setTimestamp(SECOND_ACTION_TIMESTAMP);
        final FlowChangeAction thirdAction = newAction();
        thirdAction.setTimestamp(THIRD_ACTION_TIMESTAMP);
        final Collection<Action> actions = Arrays.asList(firstAction, secondAction, thirdAction);

        service.addActions(actions);

        final HistoryQuery historyQuery = new HistoryQuery();
        historyQuery.setSourceId(SOURCE_ID);
        historyQuery.setSortOrder(SORT_ASCENDING);
        final History actionsHistory = service.getActions(historyQuery);

        assertNotNull(actionsHistory);
        assertEquals(actionsHistory.getTotal(), actions.size());
        assertNotNull(actionsHistory.getLastRefreshed());

        final Collection<Action> actionsFound = actionsHistory.getActions();
        assertNotNull(actionsFound);

        final Iterator<Action> actionsFiltered = actionsFound.iterator();
        assertTrue(actionsFiltered.hasNext());

        final Action firstActionFound = actionsFiltered.next();
        assertEquals(ACTION_TIMESTAMP, firstActionFound.getTimestamp());

        final Action secondActionFound = actionsFiltered.next();
        assertEquals(SECOND_ACTION_TIMESTAMP, secondActionFound.getTimestamp());

        final Action thirdActionFound = actionsFiltered.next();
        assertEquals(THIRD_ACTION_TIMESTAMP, thirdActionFound.getTimestamp());
    }

    @Test
    void testAddActionsPurgeActionsGetAction() {
        final Action action = newAction();
        final Collection<Action> actions = Collections.singletonList(action);

        service.addActions(actions);

        final FlowChangeAction purgeAction = newAction();
        purgeAction.setOperation(Operation.Purge);

        final FlowChangePurgeDetails purgeDetails = new FlowChangePurgeDetails();
        purgeDetails.setEndDate(PURGE_END_DATE);
        purgeAction.setActionDetails(purgeDetails);

        service.purgeActions(new Date(), purgeAction);

        final History history = service.getActions(ACTION_ID, PURGE_ACTIONS);
        assertNotNull(history);
        assertEquals(PURGE_ACTIONS, history.getTotal());

        final Iterator<Action> actionsFound = history.getActions().iterator();
        assertTrue(actionsFound.hasNext());

        final Action actionFound = actionsFound.next();
        assertEquals(Operation.Purge, actionFound.getOperation());

        final ActionDetails actionDetails = actionFound.getActionDetails();
        assertInstanceOf(PurgeDetails.class, actionDetails);

        final PurgeDetails purgeDetailsFound = (PurgeDetails) actionDetails;
        assertEquals(PURGE_END_DATE, purgeDetailsFound.getEndDate());
    }

    @Test
    void testAddActionsDeletePreviousValuesGetActions() {
        final FlowChangeAction firstAction = newAction();
        firstAction.setOperation(Operation.Configure);
        final FlowChangeConfigureDetails firstConfigureDetails = new FlowChangeConfigureDetails();
        firstConfigureDetails.setName(FIRST_PROPERTY_NAME);
        firstConfigureDetails.setValue(FIRST_VALUE);
        firstAction.setActionDetails(firstConfigureDetails);

        final FlowChangeAction secondAction = newAction();
        secondAction.setOperation(Operation.Configure);
        final FlowChangeConfigureDetails secondConfigureDetails = new FlowChangeConfigureDetails();
        secondConfigureDetails.setName(SECOND_PROPERTY_NAME);
        secondConfigureDetails.setValue(SECOND_VALUE);
        secondAction.setActionDetails(secondConfigureDetails);

        final Collection<Action> actions = Arrays.asList(firstAction, secondAction);
        service.addActions(actions);

        service.deletePreviousValues(SECOND_PROPERTY_NAME, SOURCE_ID);

        final History actionsHistory = service.getActions(ACTION_ID, Integer.MAX_VALUE);
        assertNotNull(actionsHistory);
        assertEquals(actions.size(), actionsHistory.getTotal());

        final Iterator<Action> actionsFound = actionsHistory.getActions().iterator();
        assertTrue(actionsFound.hasNext());

        final Action firstActionFound = actionsFound.next();
        assertNotNull(firstActionFound.getActionDetails());

        assertTrue(actionsFound.hasNext());
        final Action secondActionFound = actionsFound.next();
        assertNull(secondActionFound.getActionDetails());
    }

    @Test
    void testAddActionsGetPreviousValues() {
        final FlowChangeAction firstAction = newAction();
        firstAction.setOperation(Operation.Configure);
        final FlowChangeConfigureDetails firstConfigureDetails = new FlowChangeConfigureDetails();
        firstConfigureDetails.setName(FIRST_PROPERTY_NAME);
        firstConfigureDetails.setValue(FIRST_VALUE);
        firstAction.setActionDetails(firstConfigureDetails);

        final FlowChangeAction secondAction = newAction();
        secondAction.setOperation(Operation.Configure);
        final FlowChangeConfigureDetails secondConfigureDetails = new FlowChangeConfigureDetails();
        secondConfigureDetails.setName(SECOND_PROPERTY_NAME);
        secondConfigureDetails.setValue(SECOND_VALUE);
        secondAction.setActionDetails(secondConfigureDetails);

        final FlowChangeAction thirdAction = newAction();
        thirdAction.setOperation(Operation.Configure);
        final FlowChangeConfigureDetails thirdConfigureDetails = new FlowChangeConfigureDetails();
        thirdConfigureDetails.setName(SECOND_PROPERTY_NAME);
        thirdConfigureDetails.setValue(THIRD_VALUE);
        thirdAction.setActionDetails(thirdConfigureDetails);

        final Collection<Action> actions = Arrays.asList(firstAction, secondAction, thirdAction);
        service.addActions(actions);

        final Map<String, List<PreviousValue>> previousValues = service.getPreviousValues(SOURCE_ID);
        assertNotNull(previousValues);
        assertFalse(previousValues.isEmpty());

        final List<PreviousValue> firstPreviousValues = previousValues.get(FIRST_PROPERTY_NAME);
        assertNotNull(firstPreviousValues);
        final PreviousValue firstPreviousValue = firstPreviousValues.getFirst();
        assertNotNull(firstPreviousValue);
        assertEquals(FIRST_VALUE, firstPreviousValue.getPreviousValue());
        assertNotNull(firstPreviousValue.getTimestamp());
        assertEquals(USER_IDENTITY, firstPreviousValue.getUserIdentity());

        final List<PreviousValue> secondPreviousValues = previousValues.get(SECOND_PROPERTY_NAME);
        assertNotNull(secondPreviousValues);

        final PreviousValue thirdPreviousValue = secondPreviousValues.getFirst();
        assertNotNull(thirdPreviousValue);
        assertEquals(THIRD_VALUE, thirdPreviousValue.getPreviousValue());
        assertNotNull(thirdPreviousValue.getTimestamp());
        assertEquals(USER_IDENTITY, thirdPreviousValue.getUserIdentity());

        final PreviousValue secondPreviousValue = secondPreviousValues.get(1);
        assertNotNull(secondPreviousValue);
        assertEquals(SECOND_VALUE, secondPreviousValue.getPreviousValue());
        assertNotNull(secondPreviousValue.getTimestamp());
        assertEquals(USER_IDENTITY, secondPreviousValue.getUserIdentity());
    }

    @Test
    void testAddActionsConnectDetailsMinimumPropertiesGetAction() {
        final FlowChangeAction action = newAction();

        final FlowChangeConnectDetails connectDetails = new FlowChangeConnectDetails();
        connectDetails.setSourceId(SOURCE_ID);
        connectDetails.setSourceType(SOURCE_TYPE);
        connectDetails.setDestinationId(SECOND_SOURCE_ID);
        connectDetails.setDestinationType(DESTINATION_TYPE);
        action.setActionDetails(connectDetails);

        final Collection<Action> actions = Collections.singletonList(action);

        service.addActions(actions);

        final Action actionFound = service.getAction(ACTION_ID);
        assertEquals(ACTION_ID, actionFound.getId());
        assertActionFound(actionFound);

        final ActionDetails actionDetails = actionFound.getActionDetails();
        assertConnectDetailsFound(connectDetails, actionDetails);
    }

    @Test
    void testAddActionsConnectDetailsGetAction() {
        final FlowChangeAction action = newAction();

        final FlowChangeConnectDetails connectDetails = new FlowChangeConnectDetails();
        connectDetails.setSourceName(SOURCE_NAME);
        connectDetails.setSourceId(SOURCE_ID);
        connectDetails.setSourceType(SOURCE_TYPE);
        connectDetails.setDestinationName(DESTINATION_NAME);
        connectDetails.setDestinationId(SECOND_SOURCE_ID);
        connectDetails.setDestinationType(DESTINATION_TYPE);
        connectDetails.setRelationship(RELATIONSHIP);
        action.setActionDetails(connectDetails);

        final Collection<Action> actions = Collections.singletonList(action);

        service.addActions(actions);

        final Action actionFound = service.getAction(ACTION_ID);
        assertEquals(ACTION_ID, actionFound.getId());
        assertActionFound(actionFound);

        final ActionDetails actionDetails = actionFound.getActionDetails();
        assertConnectDetailsFound(connectDetails, actionDetails);
    }

    private FlowChangeAction newAction() {
        final FlowChangeAction action = new FlowChangeAction();
        action.setTimestamp(ACTION_TIMESTAMP);
        action.setSourceId(SOURCE_ID);
        action.setSourceName(SOURCE_NAME);
        action.setSourceType(SOURCE_TYPE);
        action.setUserIdentity(USER_IDENTITY);
        action.setOperation(OPERATION);
        return action;
    }

    private void assertActionFound(final Action actionFound) {
        assertNotNull(actionFound);
        assertEquals(ACTION_TIMESTAMP, actionFound.getTimestamp());
        assertEquals(SOURCE_ID, actionFound.getSourceId());
        assertEquals(SOURCE_NAME, actionFound.getSourceName());
        assertEquals(USER_IDENTITY, actionFound.getUserIdentity());
        assertEquals(OPERATION, actionFound.getOperation());
    }

    private void assertConnectDetailsFound(final ConnectDetails connectDetails, final ActionDetails actionDetails) {
        assertNotNull(actionDetails);
        assertInstanceOf(ConnectDetails.class, actionDetails);

        final ConnectDetails connectDetailsFound = (ConnectDetails) actionDetails;
        assertEquals(connectDetails.getSourceName(), connectDetailsFound.getSourceName());
        assertEquals(connectDetails.getSourceId(), connectDetailsFound.getSourceId());
        assertEquals(connectDetails.getSourceType(), connectDetailsFound.getSourceType());
        assertEquals(connectDetails.getDestinationName(), connectDetailsFound.getDestinationName());
        assertEquals(connectDetails.getDestinationId(), connectDetailsFound.getDestinationId());
        assertEquals(connectDetails.getDestinationType(), connectDetailsFound.getDestinationType());
        assertEquals(connectDetails.getRelationship(), connectDetailsFound.getRelationship());
    }
}
