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

package org.apache.nifi.processors.standard.sql;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.db.JdbcCommon;
import org.apache.nifi.util.db.SensitiveValueWrapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ExecuteSQLFetchSession implements Runnable {

    private final ProcessContext context;
    private final ProcessSession session;
    private final ExecuteSQLConfiguration config;
    private final ComponentLog logger;
    private final SqlWriter sqlWriter;
    private final DBCPService dbcpService;
    private final ResultSetFragments resultSetFragments;

    public ExecuteSQLFetchSession(ProcessContext context, ProcessSession session, ExecuteSQLConfiguration config,
                                  DBCPService dbcpService, ComponentLog logger, SqlWriter sqlWriter, ResultSetFragments resultSetFragments) {
        this.context = context;
        this.session = session;
        this.config = config;
        this.logger = logger;
        this.sqlWriter = sqlWriter;
        this.dbcpService = dbcpService;
        this.resultSetFragments = resultSetFragments;
    }

    @Override
    public void run() {
        String selectQuery = config.getSelectQuery();

        try (final Connection con = dbcpService.getConnection(config.getConnectionAttributes())) {
            setupAutoCommit(context, con, logger);

            try (final PreparedStatement st = con.prepareStatement(selectQuery)) {
                setupFetchSize(config.getFetchSize(), st, logger);
                st.setQueryTimeout(config.getQueryTimeout()); // timeout in seconds

                executePreQuery(con, config);

                setupQueryParameters(context, resultSetFragments.getInputFlowFile(), st);

                final int resultCount = executeMainQuery(selectQuery, st, resultSetFragments);

                executePostQuery(con, config, resultSetFragments);

                // If the auto commit is set to false, commit() is called for consistency
                if (!con.getAutoCommit()) {
                    con.commit();
                }

                // Transfer any remaining files to SUCCESS
                resultSetFragments.transferAllToSuccess();

                finishProcessingWithSuccess(context, session, resultSetFragments.getInputFlowFile(), resultCount, sqlWriter);
            }
        } catch (final PrePostQueryWrappedException e) {
            // In case of Pre-/Post-Query failures, we need to get the respective query from the exception
            finishProcessingWithError(context, session, e.getCause(), resultSetFragments.getInputFlowFile(), logger, e.getQuery());
        } catch (final ProcessException | SQLException e) {
            finishProcessingWithError(context, session, e, resultSetFragments.getInputFlowFile(), logger, selectQuery);
        }
    }

    private int executeMainQuery(String selectQuery, PreparedStatement st, ResultSetFragments fragments) throws SQLException {
        logger.debug("Executing query {}", selectQuery);

        int resultCount = 0;

        final StopWatch executionTime = new StopWatch(true);

        boolean hasResults = st.execute();

        final long executionTimeElapsed = executionTime.getElapsed(TimeUnit.MILLISECONDS);

        boolean hasUpdateCount = st.getUpdateCount() != -1;

        while (hasResults || hasUpdateCount) {
            //getMoreResults() and execute() return false to indicate that the result of the statement is just a number and not a ResultSet
            if (hasResults) {
                try {
                    final ResultSet resultSet = st.getResultSet();
                    final AtomicLong nrOfRows = new AtomicLong(0L);
                    do {
                        final StopWatch fetchTime = new StopWatch(true);

                        FlowFile resultSetFF = fragments.createNewFlowFile();

                        try {
                            resultSetFF = writeResultSetToFlowFile(session, resultSetFF, nrOfRows, sqlWriter, resultSet);

                            // if fragmented ResultSet, determine if we should keep this fragment
                            if (config.isMaxRowsPerFlowFileSet() && !fragments.isFirst() && nrOfRows.get() == 0) {
                                // if row count is zero and this is not the first fragment, drop it instead of committing it.
                                session.remove(resultSetFF);
                                break;
                            }

                            long fetchTimeElapsed = fetchTime.getElapsed(TimeUnit.MILLISECONDS);

                            resultSetFF = putResultSetAttributes(
                                    session,
                                    resultSetFF,
                                    fragments.getInputFileAttributeMap(),
                                    nrOfRows,
                                    executionTimeElapsed,
                                    fetchTimeElapsed,
                                    resultCount,
                                    fragments.getInputFileUUID(),
                                    sqlWriter
                            );
                            sqlWriter.updateCounters(session);

                            logger.info("{} contains {} records; transferring to 'success'", resultSetFF, nrOfRows.get());

                            reportProvenance(context, session, resultSetFF, nrOfRows, executionTimeElapsed, fetchTimeElapsed);
                            fragments.add(resultSetFF);
                        } catch (Exception e) {
                            // Remove any result set flow file(s) and propagate the exception
                            session.remove(resultSetFF);
                            fragments.removeAll();
                            if (e instanceof ProcessException) {
                                throw (ProcessException) e;
                            } else {
                                throw new ProcessException(e);
                            }
                        }
                    } while (config.isMaxRowsPerFlowFileSet()
                            && nrOfRows.get() == config.getMaxRowsPerFlowFile());
                } catch (final SQLException e) {
                    throw new ProcessException(e);
                }

                resultCount++;
            }

            // are there anymore result sets?
            try {
                hasResults = st.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
                hasUpdateCount = st.getUpdateCount() != -1;
            } catch (SQLException ex) {
                hasResults = false;
                hasUpdateCount = false;
            }
        }
        return resultCount;
    }

    private void executePreQuery(Connection con, ExecuteSQLConfiguration config) {
        // Execute pre-query, throw exception and cleanup Flow Files if fail
        final Pair<String, SQLException> failure = executeConfigStatements(con, config.getPreQueries());
        if (failure != null) {
            throw new PrePostQueryWrappedException(failure.getLeft(), failure.getRight());
        }
    }

    private void executePostQuery(Connection con, ExecuteSQLConfiguration config, ResultSetFragments resultSetFragments) {
        // Execute post-query, throw exception and cleanup Flow Files if fail
        final Pair<String, SQLException> failure = executeConfigStatements(con, config.getPostQueries());
        if (failure != null) {
            resultSetFragments.removeAll();
            throw new PrePostQueryWrappedException(failure.getLeft(), failure.getRight());
        }
    }

    private FlowFile writeResultSetToFlowFile(ProcessSession session, FlowFile resultSetFF, AtomicLong nrOfRows, SqlWriter sqlWriter,
                                              ResultSet resultSet) {
        return session.write(resultSetFF, out -> {
            try {
                nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, logger, null));
            } catch (Exception e) {
                throw (e instanceof ProcessException) ? (ProcessException) e : new ProcessException(e);
            }
        });
    }

    private FlowFile setFlowFileEmptyResults(final ProcessSession session, FlowFile flowFile, SqlWriter sqlWriter) {
        flowFile = session.write(flowFile, out -> sqlWriter.writeEmptyResultSet(out, logger));
        final Map<String, String> attributesToAdd = new HashMap<>();
        attributesToAdd.put(ExecuteSQLCommonAttributes.RESULT_ROW_COUNT, "0");
        attributesToAdd.put(CoreAttributes.MIME_TYPE.key(), sqlWriter.getMimeType());
        return session.putAllAttributes(flowFile, attributesToAdd);
    }

    /*
     * Executes given queries using pre-defined connection.
     * Returns null on success, or a query string if failed.
     */
    private Pair<String, SQLException> executeConfigStatements(final Connection con, final List<String> configQueries) {
        if (configQueries == null || configQueries.isEmpty()) {
            return null;
        }

        for (String confSQL : configQueries) {
            try (final Statement st = con.createStatement()) {
                st.execute(confSQL);
            } catch (SQLException e) {
                return Pair.of(confSQL, e);
            }
        }
        return null;
    }

    private void setupAutoCommit(ProcessContext context, Connection con, ComponentLog logger) throws SQLException {
        final boolean isAutoCommit = con.getAutoCommit();
        final boolean setAutoCommitValue = context.getProperty(ExecuteSQLCommonProperties.AUTO_COMMIT).asBoolean();
        // Only set auto-commit if necessary, log any "feature not supported" exceptions
        if (isAutoCommit != setAutoCommitValue) {
            try {
                con.setAutoCommit(setAutoCommitValue);
            } catch (SQLFeatureNotSupportedException sfnse) {
                logger.debug("setAutoCommit({}) not supported by this driver", setAutoCommitValue);
            }
        }
    }

    private void setupFetchSize(Integer fetchSize, PreparedStatement st, ComponentLog logger) {
        if (fetchSize != null && fetchSize > 0) {
            try {
                st.setFetchSize(fetchSize);
            } catch (SQLException se) {
                // Not all drivers support this, just log the error (at debug level) and move on
                logger.debug("Cannot set fetch size to {} due to {}", fetchSize, se.getLocalizedMessage(), se);
            }
        }
    }

    private void setupQueryParameters(ProcessContext context, FlowFile fileToProcess, PreparedStatement st)
            throws SQLException {
        final Map<String, SensitiveValueWrapper> sqlParameters = context.getProperties()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().isDynamic())
                .collect(Collectors.toMap(e -> e.getKey().getName(), e -> new SensitiveValueWrapper(e.getValue(), e.getKey().isSensitive())));

        if (fileToProcess != null) {
            for (Map.Entry<String, String> entry : fileToProcess.getAttributes().entrySet()) {
                sqlParameters.put(entry.getKey(), new SensitiveValueWrapper(entry.getValue(), false));
            }
        }

        if (!sqlParameters.isEmpty()) {
            JdbcCommon.setSensitiveParameters(st, sqlParameters);
        }
    }

    private FlowFile putResultSetAttributes(ProcessSession session, FlowFile resultSetFF, Map<String, String> inputFileAttrMap,
                                            AtomicLong nrOfRows, long executionTimeElapsed, long fetchTimeElapsed, int resultCount, String inputFileUUID, SqlWriter sqlWriter) {
        // set attributes
        final Map<String, String> attributesToAdd = new HashMap<>();
        if (inputFileAttrMap != null) {
            attributesToAdd.putAll(inputFileAttrMap);
        }
        attributesToAdd.put(ExecuteSQLCommonAttributes.RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
        attributesToAdd.put(ExecuteSQLCommonAttributes.RESULT_QUERY_DURATION, String.valueOf(executionTimeElapsed + fetchTimeElapsed));
        attributesToAdd.put(ExecuteSQLCommonAttributes.RESULT_QUERY_EXECUTION_TIME, String.valueOf(executionTimeElapsed));
        attributesToAdd.put(ExecuteSQLCommonAttributes.RESULT_QUERY_FETCH_TIME, String.valueOf(fetchTimeElapsed));
        attributesToAdd.put(ExecuteSQLCommonAttributes.RESULTSET_INDEX, String.valueOf(resultCount));
        if (inputFileUUID != null) {
            attributesToAdd.put(ExecuteSQLCommonAttributes.INPUT_FLOWFILE_UUID, inputFileUUID);
        }
        attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
        return session.putAllAttributes(resultSetFF, attributesToAdd);
    }

    private void reportProvenance(ProcessContext context, ProcessSession session, FlowFile resultSetFF,
                                  AtomicLong nrOfRows, long executionTimeElapsed, long fetchTimeElapsed) {
        // Report a FETCH event if there was an incoming flow file, or a RECEIVE event otherwise
        if (context.hasIncomingConnection()) {
            session.getProvenanceReporter().fetch(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed
                    + fetchTimeElapsed);
        } else {
            session.getProvenanceReporter().receive(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed
                    + fetchTimeElapsed);
        }
    }

    private void finishProcessingWithSuccess(ProcessContext context, ProcessSession session, FlowFile fileToProcess, int resultCount,
                                             SqlWriter sqlWriter) {
        if (fileToProcess != null) {
            if (resultCount > 0) {
                // If we had at least one result then it's OK to drop the original file
                session.remove(fileToProcess);
            } else {
                // If we had no results then transfer the original flow file downstream to trigger processors
                final ContentOutputStrategy contentOutputStrategy = context.getProperty(ExecuteSQLCommonProperties.CONTENT_OUTPUT_STRATEGY).asAllowableValue(ContentOutputStrategy.class);
                if (ContentOutputStrategy.ORIGINAL == contentOutputStrategy) {
                    session.transfer(fileToProcess, ExecuteSQLCommonRelationships.REL_SUCCESS);
                } else {
                    // Set Empty Results as the default behavior based on strategy or null property
                    session.transfer(setFlowFileEmptyResults(session, fileToProcess, sqlWriter), ExecuteSQLCommonRelationships.REL_SUCCESS);
                }
            }
        } else if (resultCount == 0) {
            // If we had no inbound FlowFile, no exceptions, and the SQL generated no result sets (Insert/Update/Delete statements only)
            // Then generate an empty Output FlowFile
            FlowFile resultSetFF = session.create();
            session.transfer(setFlowFileEmptyResults(session, resultSetFF, sqlWriter), ExecuteSQLCommonRelationships.REL_SUCCESS);
        }
    }

    private void finishProcessingWithError(ProcessContext context, ProcessSession session, Throwable e, FlowFile fileToProcess,
                                           ComponentLog logger, String selectQuery) {
        //If we had at least one result then it's OK to drop the original file, but if we had no results then
        //  pass the original flow file down the line to trigger downstream processors
        if (fileToProcess == null) {
            // This can happen if any exceptions occur while setting up the connection, statement, etc.
            logger.error("Unable to execute SQL select query [{}]. No FlowFile to route to failure", selectQuery, e);
            context.yield();
        } else {
            if (context.hasIncomingConnection()) {
                logger.error("Unable to execute SQL select query [{}] for {} routing to failure", selectQuery,
                        fileToProcess, e);
                fileToProcess = session.penalize(fileToProcess);
            } else {
                logger.error("Unable to execute SQL select query [{}] routing to failure", selectQuery, e);
                context.yield();
            }
            session.putAttribute(fileToProcess, ExecuteSQLCommonAttributes.RESULT_ERROR_MESSAGE, e.getMessage());
            session.transfer(fileToProcess, ExecuteSQLCommonRelationships.REL_FAILURE);
        }
    }
}
