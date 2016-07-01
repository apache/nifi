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
package org.apache.nifi.admin.dao.impl;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.nifi.admin.RepositoryUtils;
import org.apache.nifi.admin.dao.ActionDAO;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class StandardActionDAO implements ActionDAO {

    // ------------
    // action table
    // ------------
    private static final String INSERT_ACTION = "INSERT INTO ACTION ("
            + "IDENTITY, SOURCE_ID, SOURCE_NAME, SOURCE_TYPE, OPERATION, ACTION_TIMESTAMP"
            + ") VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "? "
            + ")";

    // -----------------
    // component details
    // -----------------
    private static final String INSERT_EXTENSION_DETAILS = "INSERT INTO PROCESSOR_DETAILS ("
            + "ACTION_ID, TYPE"
            + ") VALUES ("
            + "?, "
            + "?"
            + ")";

    private static final String INSERT_REMOTE_PROCESS_GROUP_DETAILS = "INSERT INTO REMOTE_PROCESS_GROUP_DETAILS ("
            + "ACTION_ID, URI"
            + ") VALUES ("
            + "?, "
            + "?"
            + ")";

    // --------------
    // action details
    // --------------
    private static final String INSERT_CONFIGURE_DETAILS = "INSERT INTO CONFIGURE_DETAILS ("
            + "ACTION_ID, NAME, VALUE, PREVIOUS_VALUE"
            + ") VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?"
            + ")";

    private static final String INSERT_CONNECT_DETAILS = "INSERT INTO CONNECT_DETAILS ("
            + "ACTION_ID, SOURCE_ID, SOURCE_NAME, SOURCE_TYPE, RELATIONSHIP, DESTINATION_ID, DESTINATION_NAME, DESTINATION_TYPE"
            + ") VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?"
            + ")";

    private static final String INSERT_MOVE_DETAILS = "INSERT INTO MOVE_DETAILS ("
            + "ACTION_ID, GROUP_ID, GROUP_NAME, PREVIOUS_GROUP_ID, PREVIOUS_GROUP_NAME"
            + ") VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "?"
            + ")";

    private static final String INSERT_PURGE_DETAILS = "INSERT INTO PURGE_DETAILS ("
            + "ACTION_ID, END_DATE"
            + ") VALUES ("
            + "?, "
            + "?"
            + ")";

    // ------------
    // action table
    // ------------
    private static final String SELECT_ACTIONS = "SELECT * FROM ACTION";

    private static final String SELECT_ACTION_COUNT = "SELECT COUNT(*) AS ACTION_COUNT FROM ACTION";

    private static final String SELECT_ACTION_BY_ID = "SELECT * "
            + "FROM ACTION "
            + "WHERE "
            + "ID = ?";

    private static final String DELETE_ACTIONS = "DELETE FROM ACTION WHERE ACTION_TIMESTAMP < ?";

    private static final String DELETE_SPECIFIC_ACTIONS = "DELETE FROM %s WHERE %s IN (SELECT ID FROM ACTION WHERE ACTION_TIMESTAMP < ?)";

    // -----------------
    // component details
    // -----------------
    private static final String SELECT_EXTENSION_DETAILS_FOR_ACTION = "SELECT * FROM PROCESSOR_DETAILS WHERE ACTION_ID = ?";

    private static final String SELECT_REMOTE_PROCESS_GROUP_DETAILS_FOR_ACTION = "SELECT * FROM REMOTE_PROCESS_GROUP_DETAILS WHERE ACTION_ID = ?";

    // --------------
    // action details
    // --------------
    private static final String SELECT_MOVE_DETAILS_FOR_ACTION = "SELECT * FROM MOVE_DETAILS WHERE ACTION_ID = ?";

    private static final String SELECT_CONFIGURE_DETAILS_FOR_ACTION = "SELECT * FROM CONFIGURE_DETAILS WHERE ACTION_ID = ?";

    private static final String SELECT_CONNECT_DETAILS_FOR_ACTION = "SELECT * FROM CONNECT_DETAILS WHERE ACTION_ID = ?";

    private static final String SELECT_PURGE_DETAILS_FOR_ACTION = "SELECT * FROM PURGE_DETAILS WHERE ACTION_ID = ?";

    // ---------------
    // previous values
    // ---------------
    private static final String SELECT_PREVIOUSLY_CONFIGURED_FIELDS = "SELECT DISTINCT CD.NAME "
            + "FROM CONFIGURE_DETAILS CD "
            + "INNER JOIN ACTION A "
            + "ON CD.ACTION_ID = A.ID "
            + "WHERE A.SOURCE_ID = ?";

    private static final String SELECT_PREVIOUS_VALUES = "SELECT CD.VALUE, "
            + "A.ACTION_TIMESTAMP, "
            + "A.IDENTITY "
            + "FROM CONFIGURE_DETAILS CD "
            + "INNER JOIN ACTION A "
            + "ON CD.ACTION_ID = A.ID "
            + "WHERE A.SOURCE_ID = ? AND CD.NAME = ? "
            + "ORDER BY A.ACTION_TIMESTAMP DESC "
            + "LIMIT 4";

    private final Connection connection;
    private final Map<String, String> columnMap;

    public StandardActionDAO(Connection connection) {
        this.connection = connection;

        // initialize the column mappings
        this.columnMap = new HashMap<>();
        this.columnMap.put("timestamp", "ACTION_TIMESTAMP");
        this.columnMap.put("sourceName", "SOURCE_NAME");
        this.columnMap.put("sourceType", "SOURCE_TYPE");
        this.columnMap.put("operation", "OPERATION");
        this.columnMap.put("userIdentity", "IDENTITY");
    }

    @Override
    public Action createAction(Action action) throws DataAccessException {
        if (action.getUserIdentity() == null) {
            throw new IllegalArgumentException("User cannot be null.");
        }

        if (action.getTimestamp() == null) {
            throw new IllegalArgumentException("Action timestamp cannot be null.");
        }

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // obtain a statement to insert to the action table
            statement = connection.prepareStatement(INSERT_ACTION, Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, StringUtils.left(action.getUserIdentity(), 4096));
            statement.setString(2, action.getSourceId());
            statement.setString(3, StringUtils.left(action.getSourceName(), 1000));
            statement.setString(4, action.getSourceType().toString());
            statement.setString(5, action.getOperation().toString());
            statement.setTimestamp(6, new java.sql.Timestamp(action.getTimestamp().getTime()));

            // insert the action
            int updateCount = statement.executeUpdate();

            final FlowChangeAction createdAction = new FlowChangeAction();
            createdAction.setUserIdentity(action.getUserIdentity());
            createdAction.setSourceId(action.getSourceId());
            createdAction.setSourceName(action.getSourceName());
            createdAction.setSourceType(action.getSourceType());
            createdAction.setOperation(action.getOperation());
            createdAction.setTimestamp(action.getTimestamp());
            createdAction.setActionDetails(action.getActionDetails());
            createdAction.setComponentDetails(action.getComponentDetails());

            // get the action id
            rs = statement.getGeneratedKeys();
            if (updateCount == 1 && rs.next()) {
                createdAction.setId(rs.getInt(1));
            } else {
                throw new DataAccessException("Unable to insert action.");
            }

            // close the previous statement
            statement.close();

            // determine the type of component
            ComponentDetails componentDetails = createdAction.getComponentDetails();
            if (componentDetails instanceof FlowChangeExtensionDetails) {
                createExtensionDetails(createdAction.getId(), (ExtensionDetails) componentDetails);
            } else if (componentDetails instanceof FlowChangeRemoteProcessGroupDetails) {
                createRemoteProcessGroupDetails(createdAction.getId(), (RemoteProcessGroupDetails) componentDetails);
            }

            // determine the type of action
            ActionDetails details = createdAction.getActionDetails();
            if (details instanceof FlowChangeConnectDetails) {
                createConnectDetails(createdAction.getId(), (ConnectDetails) details);
            } else if (details instanceof FlowChangeMoveDetails) {
                createMoveDetails(createdAction.getId(), (MoveDetails) details);
            } else if (details instanceof FlowChangeConfigureDetails) {
                createConfigureDetails(createdAction.getId(), (ConfigureDetails) details);
            } else if (details instanceof FlowChangePurgeDetails) {
                createPurgeDetails(createdAction.getId(), (PurgeDetails) details);
            }

            return createdAction;
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }
    }

    private void createExtensionDetails(int actionId, ExtensionDetails extensionDetails) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // obtain a statement to insert to the extension action table
            statement = connection.prepareStatement(INSERT_EXTENSION_DETAILS);
            statement.setInt(1, actionId);
            statement.setString(2, StringUtils.left(extensionDetails.getType(), 1000));

            // insert the action
            int updateCount = statement.executeUpdate();

            // ensure the operation completed successfully
            if (updateCount != 1) {
                throw new DataAccessException("Unable to insert extension details.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    private void createRemoteProcessGroupDetails(int actionId, RemoteProcessGroupDetails remoteProcessGroupDetails) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // obtain a statement to insert to the processor action table
            statement = connection.prepareStatement(INSERT_REMOTE_PROCESS_GROUP_DETAILS);
            statement.setInt(1, actionId);
            statement.setString(2, StringUtils.left(remoteProcessGroupDetails.getUri(), 2500));

            // insert the action
            int updateCount = statement.executeUpdate();

            // ensure the operation completed successfully
            if (updateCount != 1) {
                throw new DataAccessException("Unable to insert remote prcoess group details.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    private void createConnectDetails(int actionId, ConnectDetails connectionDetails) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // obtain a statement to insert to the processor action table
            statement = connection.prepareStatement(INSERT_CONNECT_DETAILS);
            statement.setInt(1, actionId);
            statement.setString(2, connectionDetails.getSourceId());
            statement.setString(3, StringUtils.left(connectionDetails.getSourceName(), 1000));
            statement.setString(4, StringUtils.left(connectionDetails.getSourceType().toString(), 1000));
            statement.setString(5, StringUtils.left(connectionDetails.getRelationship(), 1000));
            statement.setString(6, connectionDetails.getDestinationId());
            statement.setString(7, StringUtils.left(connectionDetails.getDestinationName(), 1000));
            statement.setString(8, StringUtils.left(connectionDetails.getDestinationType().toString(), 1000));

            // insert the action
            int updateCount = statement.executeUpdate();

            // ensure the operation completed successfully
            if (updateCount != 1) {
                throw new DataAccessException("Unable to insert connection details.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    private void createMoveDetails(int actionId, MoveDetails moveDetails) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // obtain a statement to insert to the processor action table
            statement = connection.prepareStatement(INSERT_MOVE_DETAILS);
            statement.setInt(1, actionId);
            statement.setString(2, moveDetails.getGroupId());
            statement.setString(3, StringUtils.left(moveDetails.getGroup(), 1000));
            statement.setString(4, moveDetails.getPreviousGroupId());
            statement.setString(5, StringUtils.left(moveDetails.getPreviousGroup(), 1000));

            // insert the action
            int updateCount = statement.executeUpdate();

            // ensure the operation completed successfully
            if (updateCount != 1) {
                throw new DataAccessException("Unable to insert move details.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    private void createConfigureDetails(int actionId, ConfigureDetails configurationDetails) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // obtain a statement to insert to the processor action table
            statement = connection.prepareStatement(INSERT_CONFIGURE_DETAILS);
            statement.setInt(1, actionId);
            statement.setString(2, StringUtils.left(configurationDetails.getName(), 1000));
            statement.setString(3, StringUtils.left(configurationDetails.getValue(), 5000));
            statement.setString(4, StringUtils.left(configurationDetails.getPreviousValue(), 5000));

            // insert the action
            int updateCount = statement.executeUpdate();

            // ensure the operation completed successfully
            if (updateCount != 1) {
                throw new DataAccessException("Unable to insert configure details.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    private void createPurgeDetails(int actionId, PurgeDetails purgeDetails) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // obtain a statement to insert to the processor action table
            statement = connection.prepareStatement(INSERT_PURGE_DETAILS);
            statement.setInt(1, actionId);
            statement.setTimestamp(2, new java.sql.Timestamp(purgeDetails.getEndDate().getTime()));

            // insert the action
            int updateCount = statement.executeUpdate();

            // ensure the operation completed successfully
            if (updateCount != 1) {
                throw new DataAccessException("Unable to insert connection details.");
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

    @Override
    public History findActions(HistoryQuery historyQuery) throws DataAccessException {

        // get the sort column
        String sortColumn = "ACTION_TIMESTAMP";
        if (StringUtils.isNotBlank(historyQuery.getSortColumn())) {
            String rawColumnName = historyQuery.getSortColumn();
            if (!columnMap.containsKey(rawColumnName)) {
                throw new IllegalArgumentException(String.format("Unrecognized column name '%s'.", rawColumnName));
            }
            sortColumn = columnMap.get(rawColumnName);
        }

        // get the sort order
        String sortOrder = "desc";
        if (StringUtils.isNotBlank(historyQuery.getSortOrder())) {
            sortOrder = historyQuery.getSortOrder();
        }

        History actionResult = new History();
        Collection<Action> actions = new ArrayList<>();
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            List<String> where = new ArrayList<>();

            // append the start time
            if (historyQuery.getStartDate() != null) {
                where.add("ACTION_TIMESTAMP >= ?");
            }

            // append the end time
            if (historyQuery.getEndDate() != null) {
                where.add("ACTION_TIMESTAMP <= ?");
            }

            // append the user id as necessary
            if (historyQuery.getUserIdentity() != null) {
                where.add("UPPER(IDENTITY) LIKE ?");
            }

            // append the source id as necessary
            if (historyQuery.getSourceId() != null) {
                where.add("SOURCE_ID = ?");
            }

            String sql = SELECT_ACTION_COUNT;
            if (!where.isEmpty()) {
                sql += " WHERE " + StringUtils.join(where, " AND ");
            }

            // get the total number of actions
            statement = connection.prepareStatement(sql);
            int paramIndex = 1;

            // set the start date as necessary
            if (historyQuery.getStartDate() != null) {
                statement.setTimestamp(paramIndex++, new java.sql.Timestamp(historyQuery.getStartDate().getTime()));
            }

            // set the end date as necessary
            if (historyQuery.getEndDate() != null) {
                statement.setTimestamp(paramIndex++, new java.sql.Timestamp(historyQuery.getEndDate().getTime()));
            }

            // set the user id as necessary
            if (historyQuery.getUserIdentity() != null) {
                statement.setString(paramIndex++, "%" + historyQuery.getUserIdentity().toUpperCase() + "%");
            }

            // set the source id as necessary
            if (historyQuery.getSourceId() != null) {
                statement.setString(paramIndex, historyQuery.getSourceId());
            }

            // execute the statement
            rs = statement.executeQuery();

            // ensure there are results
            if (rs.next()) {
                actionResult.setTotal(rs.getInt("ACTION_COUNT"));
            } else {
                throw new DataAccessException("Unable to determine total action count.");
            }

            sql = SELECT_ACTIONS;
            if (!where.isEmpty()) {
                sql += " WHERE " + StringUtils.join(where, " AND ");
            }

            // append the sort criteria
            sql += (" ORDER BY " + sortColumn + " " + sortOrder);

            // append the offset and limit
            sql += " LIMIT ? OFFSET ?";

            // close the previous statement
            statement.close();

            // create the statement
            statement = connection.prepareStatement(sql);
            paramIndex = 1;

            // set the start date as necessary
            if (historyQuery.getStartDate() != null) {
                statement.setTimestamp(paramIndex++, new java.sql.Timestamp(historyQuery.getStartDate().getTime()));
            }

            // set the end date as necessary
            if (historyQuery.getEndDate() != null) {
                statement.setTimestamp(paramIndex++, new java.sql.Timestamp(historyQuery.getEndDate().getTime()));
            }

            // set the user id as necessary
            if (historyQuery.getUserIdentity() != null) {
                statement.setString(paramIndex++, "%" + historyQuery.getUserIdentity().toUpperCase() + "%");
            }

            // set the source id as necessary
            if (historyQuery.getSourceId() != null) {
                statement.setString(paramIndex++, historyQuery.getSourceId());
            }

            // set the limit
            statement.setInt(paramIndex++, historyQuery.getCount());

            // set the offset according to the currented page calculated above
            statement.setInt(paramIndex, historyQuery.getOffset());

            // execute the query
            rs = statement.executeQuery();

            // create each corresponding action
            while (rs.next()) {
                final Integer actionId = rs.getInt("ID");
                final Operation operation = Operation.valueOf(rs.getString("OPERATION"));
                final Component component = Component.valueOf(rs.getString("SOURCE_TYPE"));

                FlowChangeAction action = new FlowChangeAction();
                action.setId(actionId);
                action.setUserIdentity(rs.getString("IDENTITY"));
                action.setOperation(Operation.valueOf(rs.getString("OPERATION")));
                action.setTimestamp(new Date(rs.getTimestamp("ACTION_TIMESTAMP").getTime()));
                action.setSourceId(rs.getString("SOURCE_ID"));
                action.setSourceName(rs.getString("SOURCE_NAME"));
                action.setSourceType(Component.valueOf(rs.getString("SOURCE_TYPE")));

                // get the component details if appropriate
                ComponentDetails componentDetails = null;
                if (Component.Processor.equals(component) || Component.ControllerService.equals(component) || Component.ReportingTask.equals(component)) {
                    componentDetails = getExtensionDetails(actionId);
                } else if (Component.RemoteProcessGroup.equals(component)) {
                    componentDetails = getRemoteProcessGroupDetails(actionId);
                }

                if (componentDetails != null) {
                    action.setComponentDetails(componentDetails);
                }

                // get the action details if appropriate
                ActionDetails actionDetails = null;
                if (Operation.Move.equals(operation)) {
                    actionDetails = getMoveDetails(actionId);
                } else if (Operation.Configure.equals(operation)) {
                    actionDetails = getConfigureDetails(actionId);
                } else if (Operation.Connect.equals(operation) || Operation.Disconnect.equals(operation)) {
                    actionDetails = getConnectDetails(actionId);
                } else if (Operation.Purge.equals(operation)) {
                    actionDetails = getPurgeDetails(actionId);
                }

                // set the action details
                if (actionDetails != null) {
                    action.setActionDetails(actionDetails);
                }

                // add the action
                actions.add(action);
            }

            // populate the action result
            actionResult.setActions(actions);
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return actionResult;
    }

    @Override
    public Action getAction(Integer actionId) throws DataAccessException {
        FlowChangeAction action = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the statement
            statement = connection.prepareStatement(SELECT_ACTION_BY_ID);
            statement.setInt(1, actionId);

            // execute the query
            rs = statement.executeQuery();

            // ensure results
            if (rs.next()) {
                Operation operation = Operation.valueOf(rs.getString("OPERATION"));
                Component component = Component.valueOf(rs.getString("SOURCE_TYPE"));

                // populate the action
                action = new FlowChangeAction();
                action.setId(rs.getInt("ID"));
                action.setUserIdentity(rs.getString("IDENTITY"));
                action.setOperation(operation);
                action.setTimestamp(new Date(rs.getTimestamp("ACTION_TIMESTAMP").getTime()));
                action.setSourceId(rs.getString("SOURCE_ID"));
                action.setSourceName(rs.getString("SOURCE_NAME"));
                action.setSourceType(component);

                // get the component details if appropriate
                ComponentDetails componentDetails = null;
                if (Component.Processor.equals(component) || Component.ControllerService.equals(component) || Component.ReportingTask.equals(component)) {
                    componentDetails = getExtensionDetails(actionId);
                } else if (Component.RemoteProcessGroup.equals(component)) {
                    componentDetails = getRemoteProcessGroupDetails(actionId);
                }

                if (componentDetails != null) {
                    action.setComponentDetails(componentDetails);
                }

                // get the action details if appropriate
                ActionDetails actionDetails = null;
                if (Operation.Move.equals(operation)) {
                    actionDetails = getMoveDetails(actionId);
                } else if (Operation.Configure.equals(operation)) {
                    actionDetails = getConfigureDetails(actionId);
                } else if (Operation.Connect.equals(operation) || Operation.Disconnect.equals(operation)) {
                    actionDetails = getConnectDetails(actionId);
                } else if (Operation.Purge.equals(operation)) {
                    actionDetails = getPurgeDetails(actionId);
                }

                // set the action details
                if (actionDetails != null) {
                    action.setActionDetails(actionDetails);
                }
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return action;
    }

    private ExtensionDetails getExtensionDetails(Integer actionId) throws DataAccessException {
        FlowChangeExtensionDetails extensionDetails = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the statement
            statement = connection.prepareStatement(SELECT_EXTENSION_DETAILS_FOR_ACTION);
            statement.setInt(1, actionId);

            // execute the query
            rs = statement.executeQuery();

            // ensure results
            if (rs.next()) {
                extensionDetails = new FlowChangeExtensionDetails();
                extensionDetails.setType(rs.getString("TYPE"));
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return extensionDetails;
    }

    private RemoteProcessGroupDetails getRemoteProcessGroupDetails(Integer actionId) throws DataAccessException {
        FlowChangeRemoteProcessGroupDetails remoteProcessGroupDetails = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the statement
            statement = connection.prepareStatement(SELECT_REMOTE_PROCESS_GROUP_DETAILS_FOR_ACTION);
            statement.setInt(1, actionId);

            // execute the query
            rs = statement.executeQuery();

            // ensure results
            if (rs.next()) {
                remoteProcessGroupDetails = new FlowChangeRemoteProcessGroupDetails();
                remoteProcessGroupDetails.setUri(rs.getString("URI"));
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return remoteProcessGroupDetails;
    }

    private MoveDetails getMoveDetails(Integer actionId) throws DataAccessException {
        FlowChangeMoveDetails moveDetails = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the statement
            statement = connection.prepareStatement(SELECT_MOVE_DETAILS_FOR_ACTION);
            statement.setInt(1, actionId);

            // execute the query
            rs = statement.executeQuery();

            // ensure results
            if (rs.next()) {
                moveDetails = new FlowChangeMoveDetails();
                moveDetails.setGroupId(rs.getString("GROUP_ID"));
                moveDetails.setGroup(rs.getString("GROUP_NAME"));
                moveDetails.setPreviousGroupId(rs.getString("PREVIOUS_GROUP_ID"));
                moveDetails.setPreviousGroup(rs.getString("PREVIOUS_GROUP_NAME"));
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return moveDetails;
    }

    private ConnectDetails getConnectDetails(Integer actionId) throws DataAccessException {
        FlowChangeConnectDetails connectionDetails = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the statement
            statement = connection.prepareStatement(SELECT_CONNECT_DETAILS_FOR_ACTION);
            statement.setInt(1, actionId);

            // execute the query
            rs = statement.executeQuery();

            // ensure results
            if (rs.next()) {
                final Component sourceComponent = Component.valueOf(rs.getString("SOURCE_TYPE"));
                final Component destinationComponent = Component.valueOf(rs.getString("DESTINATION_TYPE"));

                connectionDetails = new FlowChangeConnectDetails();
                connectionDetails.setSourceId(rs.getString("SOURCE_ID"));
                connectionDetails.setSourceName(rs.getString("SOURCE_NAME"));
                connectionDetails.setSourceType(sourceComponent);
                connectionDetails.setRelationship(rs.getString("RELATIONSHIP"));
                connectionDetails.setDestinationId(rs.getString("DESTINATION_ID"));
                connectionDetails.setDestinationName(rs.getString("DESTINATION_NAME"));
                connectionDetails.setDestinationType(destinationComponent);
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return connectionDetails;
    }

    private ConfigureDetails getConfigureDetails(Integer actionId) throws DataAccessException {
        FlowChangeConfigureDetails configurationDetails = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the statement
            statement = connection.prepareStatement(SELECT_CONFIGURE_DETAILS_FOR_ACTION);
            statement.setInt(1, actionId);

            // execute the query
            rs = statement.executeQuery();

            // ensure results
            if (rs.next()) {
                configurationDetails = new FlowChangeConfigureDetails();
                configurationDetails.setName(rs.getString("NAME"));
                configurationDetails.setValue(rs.getString("VALUE"));
                configurationDetails.setPreviousValue(rs.getString("PREVIOUS_VALUE"));
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return configurationDetails;
    }

    private PurgeDetails getPurgeDetails(Integer actionId) throws DataAccessException {
        FlowChangePurgeDetails purgeDetails = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the statement
            statement = connection.prepareStatement(SELECT_PURGE_DETAILS_FOR_ACTION);
            statement.setInt(1, actionId);

            // execute the query
            rs = statement.executeQuery();

            // ensure results
            if (rs.next()) {
                purgeDetails = new FlowChangePurgeDetails();
                purgeDetails.setEndDate(new Date(rs.getTimestamp("END_DATE").getTime()));
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return purgeDetails;
    }

    @Override
    public Map<String, List<PreviousValue>> getPreviousValues(String componentId) {
        Map<String, List<PreviousValue>> previousValues = new LinkedHashMap<>();

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the statement
            statement = connection.prepareStatement(SELECT_PREVIOUSLY_CONFIGURED_FIELDS);
            statement.setString(1, componentId);

            // execute the query
            rs = statement.executeQuery();

            // ensure results
            while (rs.next()) {
                final String property = rs.getString("NAME");
                previousValues.put(property, getPreviousValuesForProperty(componentId, property));
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return previousValues;
    }

    private List<PreviousValue> getPreviousValuesForProperty(final String componentId, final String property) {
        List<PreviousValue> previousValues = new ArrayList<>();

        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
            // create the statement
            statement = connection.prepareStatement(SELECT_PREVIOUS_VALUES);
            statement.setString(1, componentId);
            statement.setString(2, property);

            // execute the query
            rs = statement.executeQuery();

            // ensure results
            while (rs.next()) {
                // get the previous value
                final PreviousValue previousValue = new PreviousValue();
                previousValue.setPreviousValue(rs.getString("VALUE"));
                previousValue.setTimestamp(new Date(rs.getTimestamp("ACTION_TIMESTAMP").getTime()));
                previousValue.setUserIdentity(rs.getString("IDENTITY"));
                previousValues.add(previousValue);
            }
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(rs);
            RepositoryUtils.closeQuietly(statement);
        }

        return previousValues;
    }

    @Override
    public void deleteActions(Date endDate) throws DataAccessException {
        PreparedStatement statement = null;
        try {
            // -----------------
            // component details
            // -----------------

            // create the move delete statement
            statement = connection.prepareStatement(String.format(DELETE_SPECIFIC_ACTIONS, "PROCESSOR_DETAILS", "ACTION_ID"));
            statement.setTimestamp(1, new java.sql.Timestamp(endDate.getTime()));
            statement.executeUpdate();
            statement.close();

            // create the move delete statement
            statement = connection.prepareStatement(String.format(DELETE_SPECIFIC_ACTIONS, "REMOTE_PROCESS_GROUP_DETAILS", "ACTION_ID"));
            statement.setTimestamp(1, new java.sql.Timestamp(endDate.getTime()));
            statement.executeUpdate();
            statement.close();

            // --------------
            // action details
            // --------------
            // create the move delete statement
            statement = connection.prepareStatement(String.format(DELETE_SPECIFIC_ACTIONS, "MOVE_DETAILS", "ACTION_ID"));
            statement.setTimestamp(1, new java.sql.Timestamp(endDate.getTime()));
            statement.executeUpdate();
            statement.close();

            // create the configure delete statement
            statement = connection.prepareStatement(String.format(DELETE_SPECIFIC_ACTIONS, "CONFIGURE_DETAILS", "ACTION_ID"));
            statement.setTimestamp(1, new java.sql.Timestamp(endDate.getTime()));
            statement.executeUpdate();
            statement.close();

            // create the connect delete statement
            statement = connection.prepareStatement(String.format(DELETE_SPECIFIC_ACTIONS, "CONNECT_DETAILS", "ACTION_ID"));
            statement.setTimestamp(1, new java.sql.Timestamp(endDate.getTime()));
            statement.executeUpdate();
            statement.close();

            // create the relationship delete statement
            statement = connection.prepareStatement(String.format(DELETE_SPECIFIC_ACTIONS, "PURGE_DETAILS", "ACTION_ID"));
            statement.setTimestamp(1, new java.sql.Timestamp(endDate.getTime()));
            statement.executeUpdate();
            statement.close();

            // -------
            // actions
            // -------
            // create the action delete statement
            statement = connection.prepareStatement(DELETE_ACTIONS);
            statement.setTimestamp(1, new java.sql.Timestamp(endDate.getTime()));
            statement.executeUpdate();
        } catch (SQLException sqle) {
            throw new DataAccessException(sqle);
        } finally {
            RepositoryUtils.closeQuietly(statement);
        }
    }

}
