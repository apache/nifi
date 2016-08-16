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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SupportsBatching
@SeeAlso(ConvertJSONToSQL.class)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "put", "rdbms", "database", "update", "insert", "relational"})
@CapabilityDescription("Executes a SQL UPDATE or INSERT command. The content of an incoming FlowFile is expected to be the SQL command "
    + "to execute. The SQL command may use the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes "
    + "with the naming convention sql.args.N.type and sql.args.N.value, where N is a positive integer. The sql.args.N.type is expected to be "
    + "a number indicating the JDBC Type. The content of the FlowFile is expected to be in UTF-8 format.")
@ReadsAttributes({
    @ReadsAttribute(attribute = "fragment.identifier", description = "If the <Support Fragment Transactions> property is true, this attribute is used to determine whether or "
        + "not two FlowFiles belong to the same transaction."),
    @ReadsAttribute(attribute = "fragment.count", description = "If the <Support Fragment Transactions> property is true, this attribute is used to determine how many FlowFiles "
        + "are needed to complete the transaction."),
    @ReadsAttribute(attribute = "fragment.index", description = "If the <Support Fragment Transactions> property is true, this attribute is used to determine the order that the FlowFiles "
        + "in a transaction should be evaluated."),
    @ReadsAttribute(attribute = "sql.args.N.type", description = "Incoming FlowFiles are expected to be parameterized SQL statements. The type of each Parameter is specified as an integer "
        + "that represents the JDBC Type of the parameter."),
    @ReadsAttribute(attribute = "sql.args.N.value", description = "Incoming FlowFiles are expected to be parameterized SQL statements. The value of the Parameters are specified as "
        + "sql.args.1.value, sql.args.2.value, sql.args.3.value, and so on. The type of the sql.args.1.value Parameter is specified by the sql.args.1.type attribute.")
})
@WritesAttributes({
    @WritesAttribute(attribute = "sql.generated.key", description = "If the database generated a key for an INSERT statement and the Obtain Generated Keys property is set to true, "
        + "this attribute will be added to indicate the generated key, if possible. This feature is not supported by all database vendors.")
})
public class PutSQL extends AbstractProcessor {

    static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
        .name("JDBC Connection Pool")
        .description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
            + "The Connection Pool is necessary in order to determine the appropriate database column types.")
        .identifiesControllerService(DBCPService.class)
        .required(true)
        .build();
    static final PropertyDescriptor SUPPORT_TRANSACTIONS = new PropertyDescriptor.Builder()
        .name("Support Fragmented Transactions")
        .description("If true, when a FlowFile is consumed by this Processor, the Processor will first check the fragment.identifier and fragment.count attributes of that FlowFile. "
            + "If the fragment.count value is greater than 1, the Processor will not process any FlowFile will that fragment.identifier until all are available; "
            + "at that point, it will process all FlowFiles with that fragment.identifier as a single transaction, in the order specified by the FlowFiles' fragment.index attributes. "
            + "This Provides atomicity of those SQL statements. If this value is false, these attributes will be ignored and the updates will occur independent of one another.")
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();
    static final PropertyDescriptor TRANSACTION_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Transaction Timeout")
        .description("If the <Support Fragmented Transactions> property is set to true, specifies how long to wait for all FlowFiles for a particular fragment.identifier attribute "
            + "to arrive before just transferring all of the FlowFiles with that identifier to the 'failure' relationship")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .description("The preferred number of FlowFiles to put to the database in a single transaction")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("100")
        .build();
    static final PropertyDescriptor OBTAIN_GENERATED_KEYS = new PropertyDescriptor.Builder()
        .name("Obtain Generated Keys")
        .description("If true, any key that is automatically generated by the database will be added to the FlowFile that generated it using the sql.generate.key attribute. "
            + "This may result in slightly slower performance and is not supported by all databases.")
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("A FlowFile is routed to this relationship after the database is successfully updated")
        .build();
    static final Relationship REL_RETRY = new Relationship.Builder()
        .name("retry")
        .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
            + "such as an invalid query or an integrity constraint violation")
        .build();

    private static final Pattern SQL_TYPE_ATTRIBUTE_PATTERN = Pattern.compile("sql\\.args\\.(\\d+)\\.type");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");

    private static final String FRAGMENT_ID_ATTR = "fragment.identifier";
    private static final String FRAGMENT_INDEX_ATTR = "fragment.index";
    private static final String FRAGMENT_COUNT_ATTR = "fragment.count";

    private static final Pattern LONG_PATTERN = Pattern.compile("^\\d{1,19}$");

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_POOL);
        properties.add(SUPPORT_TRANSACTIONS);
        properties.add(TRANSACTION_TIMEOUT);
        properties.add(BATCH_SIZE);
        properties.add(OBTAIN_GENERATED_KEYS);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_RETRY);
        rels.add(REL_FAILURE);
        return rels;
    }



    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFilePoll poll = pollFlowFiles(context, session);
        if (poll == null) {
            return;
        }

        final List<FlowFile> flowFiles = poll.getFlowFiles();
        if (flowFiles == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        final boolean obtainKeys = context.getProperty(OBTAIN_GENERATED_KEYS).asBoolean();
        final Map<String, StatementFlowFileEnclosure> statementMap = new HashMap<>(); // Map SQL to a PreparedStatement and FlowFiles
        final List<FlowFile> sentFlowFiles = new ArrayList<>(); // flowfiles that have been sent
        final List<FlowFile> processedFlowFiles = new ArrayList<>(); // all flowfiles that we have processed
        final Set<StatementFlowFileEnclosure> enclosuresToExecute = new LinkedHashSet<>(); // the enclosures that we've processed

        // Because we can have a transaction that is necessary across multiple FlowFiles, things get complicated when
        // some FlowFiles have been transferred to a relationship and then there is a failure. As a result, we will just
        // map all FlowFiles to their destination relationship and do the session.transfer at the end. This way, if there
        // is a failure, we can route all FlowFiles to failure if we need to.
        final Map<FlowFile, Relationship> destinationRelationships = new HashMap<>();

        final DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
        try (final Connection conn = dbcpService.getConnection()) {
            final boolean originalAutoCommit = conn.getAutoCommit();
            try {
                conn.setAutoCommit(false);

                for (final FlowFile flowFile : flowFiles) {
                    processedFlowFiles.add(flowFile);
                    final String sql = getSQL(session, flowFile);

                    // Get the appropriate PreparedStatement to use.
                    final StatementFlowFileEnclosure enclosure;
                    try {
                        enclosure = getEnclosure(sql, conn, statementMap, obtainKeys, poll.isFragmentedTransaction());
                    } catch (final SQLNonTransientException e) {
                        getLogger().error("Failed to update database for {} due to {}; routing to failure", new Object[] {flowFile, e});
                        destinationRelationships.put(flowFile, REL_FAILURE);
                        continue;
                    }

                    final PreparedStatement stmt = enclosure.getStatement();

                    // set the appropriate parameters on the statement.
                    try {
                        setParameters(stmt, flowFile.getAttributes());
                    } catch (final SQLException | ProcessException pe) {
                        getLogger().error("Cannot update database for {} due to {}; routing to failure", new Object[] {flowFile, pe.toString()}, pe);
                        destinationRelationships.put(flowFile, REL_FAILURE);
                        continue;
                    }

                    // If we need to obtain keys, we cannot do so in a a Batch Update. So we have to execute the statement and close it.
                    if (obtainKeys) {
                        try {
                            // Execute the actual update.
                            stmt.executeUpdate();

                            // attempt to determine the key that was generated, if any. This is not supported by all
                            // database vendors, so if we cannot determine the generated key (or if the statement is not an INSERT),
                            // we will just move on without setting the attribute.
                            FlowFile sentFlowFile = flowFile;
                            final String generatedKey = determineGeneratedKey(stmt);
                            if (generatedKey != null) {
                                sentFlowFile = session.putAttribute(sentFlowFile, "sql.generated.key", generatedKey);
                            }

                            stmt.close();
                            sentFlowFiles.add(sentFlowFile);
                        } catch (final SQLNonTransientException e) {
                            getLogger().error("Failed to update database for {} due to {}; routing to failure", new Object[] {flowFile, e});
                            destinationRelationships.put(flowFile, REL_FAILURE);
                            continue;
                        } catch (final SQLException e) {
                            getLogger().error("Failed to update database for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry", new Object[] {flowFile, e});
                            destinationRelationships.put(flowFile, REL_RETRY);
                            continue;
                        }
                    } else {
                        // We don't need to obtain keys. Just add the statement to the batch.
                        stmt.addBatch();
                        enclosure.addFlowFile(flowFile);
                        enclosuresToExecute.add(enclosure);
                    }
                }

                // If we are not trying to obtain the generated keys, we will have
                // PreparedStatement's that have batches added to them. We need to execute each batch and close
                // the PreparedStatement.
                for (final StatementFlowFileEnclosure enclosure : enclosuresToExecute) {
                    try {
                        final PreparedStatement stmt = enclosure.getStatement();
                        stmt.executeBatch();
                        sentFlowFiles.addAll(enclosure.getFlowFiles());
                    } catch (final BatchUpdateException e) {
                        // If we get a BatchUpdateException, then we want to determine which FlowFile caused the failure,
                        // and route that FlowFile to failure while routing those that finished processing to success and those
                        // that have not yet been executed to retry. If the FlowFile was
                        // part of a fragmented transaction, then we must roll back all updates for this connection, because
                        // other statements may have been successful and been part of this transaction.
                        final int[] updateCounts = e.getUpdateCounts();
                        final int offendingFlowFileIndex = updateCounts.length;
                        final List<FlowFile> batchFlowFiles = enclosure.getFlowFiles();

                        if (poll.isFragmentedTransaction()) {
                            // There are potentially multiple statements for this one transaction. As a result,
                            // we need to roll back the entire transaction and route all of the FlowFiles to failure.
                            conn.rollback();
                            final FlowFile offendingFlowFile = batchFlowFiles.get(offendingFlowFileIndex);
                            getLogger().error("Failed to update database due to a failed batch update. A total of {} FlowFiles are required for this transaction, so routing all to failure. "
                                + "Offending FlowFile was {}, which caused the following error: {}", new Object[] {flowFiles.size(), offendingFlowFile, e});
                            session.transfer(flowFiles, REL_FAILURE);
                            return;
                        }

                        // In the presence of a BatchUpdateException, the driver has the option of either stopping when an error
                        // occurs, or continuing. If it continues, then it must account for all statements in the batch and for
                        // those that fail return a Statement.EXECUTE_FAILED for the number of rows updated.
                        // So we will iterate over all of the update counts returned. If any is equal to Statement.EXECUTE_FAILED,
                        // we will route the corresponding FlowFile to failure. Otherwise, the FlowFile will go to success
                        // unless it has not yet been processed (its index in the List > updateCounts.length).
                        int failureCount = 0;
                        int successCount = 0;
                        int retryCount = 0;
                        for (int i = 0; i < updateCounts.length; i++) {
                            final int updateCount = updateCounts[i];
                            final FlowFile flowFile = batchFlowFiles.get(i);
                            if (updateCount == Statement.EXECUTE_FAILED) {
                                destinationRelationships.put(flowFile, REL_FAILURE);
                                failureCount++;
                            } else {
                                destinationRelationships.put(flowFile, REL_SUCCESS);
                                successCount++;
                            }
                        }

                        if (failureCount == 0) {
                            // if no failures found, the driver decided not to execute the statements after the
                            // failure, so route the last one to failure.
                            final FlowFile failedFlowFile = batchFlowFiles.get(updateCounts.length);
                            destinationRelationships.put(failedFlowFile, REL_FAILURE);
                            failureCount++;
                        }

                        if (updateCounts.length < batchFlowFiles.size()) {
                            final List<FlowFile> unexecuted = batchFlowFiles.subList(updateCounts.length + 1, batchFlowFiles.size());
                            for (final FlowFile flowFile : unexecuted) {
                                destinationRelationships.put(flowFile, REL_RETRY);
                                retryCount++;
                            }
                        }

                        getLogger().error("Failed to update database due to a failed batch update. There were a total of {} FlowFiles that failed, {} that succeeded, "
                            + "and {} that were not execute and will be routed to retry; ", new Object[] {failureCount, successCount, retryCount});
                    } catch (final SQLNonTransientException e) {
                        getLogger().error("Failed to update database for {} due to {}; routing to failure", new Object[] {enclosure.getFlowFiles(), e});

                        for (final FlowFile flowFile : enclosure.getFlowFiles()) {
                            destinationRelationships.put(flowFile, REL_FAILURE);
                        }
                        continue;
                    } catch (final SQLException e) {
                        getLogger().error("Failed to update database for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry",
                            new Object[] {enclosure.getFlowFiles(), e});

                        for (final FlowFile flowFile : enclosure.getFlowFiles()) {
                            destinationRelationships.put(flowFile, REL_RETRY);
                        }
                        continue;
                    } finally {
                        enclosure.getStatement().close();
                    }
                }
            } finally {
                try {
                    conn.commit();
                } finally {
                    // make sure that we try to set the auto commit back to whatever it was.
                    if (originalAutoCommit) {
                        try {
                            conn.setAutoCommit(originalAutoCommit);
                        } catch (final SQLException se) {
                        }
                    }
                }
            }

            // Determine the database URL
            String url = "jdbc://unknown-host";
            try {
                url = conn.getMetaData().getURL();
            } catch (final SQLException sqle) {
            }

            // Emit a Provenance SEND event
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            for (final FlowFile flowFile : sentFlowFiles) {
                session.getProvenanceReporter().send(flowFile, url, transmissionMillis, true);
            }

            for (final FlowFile flowFile : sentFlowFiles) {
                destinationRelationships.put(flowFile, REL_SUCCESS);
            }
        } catch (final SQLException e) {
            // Failed FlowFiles are all of them that we have processed minus those that were successfully sent
            final List<FlowFile> failedFlowFiles = processedFlowFiles;
            failedFlowFiles.removeAll(sentFlowFiles);

            // All FlowFiles yet to be processed is all FlowFiles minus those processed
            final List<FlowFile> retry = flowFiles;
            retry.removeAll(processedFlowFiles);

            final Relationship rel;
            if (e instanceof SQLNonTransientException) {
                getLogger().error("Failed to update database for {} due to {}; routing to failure", new Object[] {failedFlowFiles, e});
                rel = REL_FAILURE;
            } else {
                getLogger().error("Failed to update database for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry", new Object[] {failedFlowFiles, e});
                rel = REL_RETRY;
            }

            for (final FlowFile flowFile : failedFlowFiles) {
                destinationRelationships.put(flowFile, rel);
            }

            for (final FlowFile flowFile : retry) {
                destinationRelationships.put(flowFile, Relationship.SELF);
            }
        }

        for (final Map.Entry<FlowFile, Relationship> entry : destinationRelationships.entrySet()) {
            session.transfer(entry.getKey(), entry.getValue());
        }
    }


    /**
     * Pulls a batch of FlowFiles from the incoming queues. If no FlowFiles are available, returns <code>null</code>.
     * Otherwise, a List of FlowFiles will be returned.
     *
     * If all FlowFiles pulled are not eligible to be processed, the FlowFiles will be penalized and transferred back
     * to the input queue and an empty List will be returned.
     *
     * Otherwise, if the Support Fragmented Transactions property is true, all FlowFiles that belong to the same
     * transaction will be sorted in the order that they should be evaluated.
     *
     * @param context the process context for determining properties
     * @param session the process session for pulling flowfiles
     * @return a FlowFilePoll containing a List of FlowFiles to process, or <code>null</code> if there are no FlowFiles to process
     */
    private FlowFilePoll pollFlowFiles(final ProcessContext context, final ProcessSession session) {
        // Determine which FlowFile Filter to use in order to obtain FlowFiles.
        final boolean useTransactions = context.getProperty(SUPPORT_TRANSACTIONS).asBoolean();
        boolean fragmentedTransaction = false;

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles;
        if (useTransactions) {
            final TransactionalFlowFileFilter filter = new TransactionalFlowFileFilter();
            flowFiles = session.get(filter);
            fragmentedTransaction = filter.isFragmentedTransaction();
        } else {
            flowFiles = session.get(batchSize);
        }

        if (flowFiles.isEmpty()) {
            return null;
        }

        // If we are supporting fragmented transactions, verify that all FlowFiles are correct
        if (fragmentedTransaction) {
            final Relationship relationship = determineRelationship(flowFiles, context.getProperty(TRANSACTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS));
            if (relationship != null) {
                // if transferring back to self, penalize the FlowFiles.
                if (relationship == Relationship.SELF) {
                    // penalize all of the FlowFiles that we are going to route to SELF.
                    final ListIterator<FlowFile> itr = flowFiles.listIterator();
                    while (itr.hasNext()) {
                        final FlowFile flowFile = itr.next();
                        final FlowFile penalized = session.penalize(flowFile);
                        itr.remove();
                        itr.add(penalized);
                    }
                }

                session.transfer(flowFiles, relationship);
                return null;
            }

            // sort by fragment index.
            Collections.sort(flowFiles, new Comparator<FlowFile>() {
                @Override
                public int compare(final FlowFile o1, final FlowFile o2) {
                    return Integer.compare(Integer.parseInt(o1.getAttribute(FRAGMENT_INDEX_ATTR)), Integer.parseInt(o2.getAttribute(FRAGMENT_INDEX_ATTR)));
                }
            });
        }

        return new FlowFilePoll(flowFiles, fragmentedTransaction);
    }


    /**
     * Returns the key that was generated from the given statement, or <code>null</code> if no key
     * was generated or it could not be determined.
     *
     * @param stmt the statement that generated a key
     * @return the key that was generated from the given statement, or <code>null</code> if no key
     *         was generated or it could not be determined.
     */
    private String determineGeneratedKey(final PreparedStatement stmt) {
        try {
            final ResultSet generatedKeys = stmt.getGeneratedKeys();
            if (generatedKeys != null && generatedKeys.next()) {
                return generatedKeys.getString(1);
            }
        } catch (final SQLException sqle) {
            // This is not supported by all vendors. This is a best-effort approach.
        }

        return null;
    }


    /**
     * Returns the StatementFlowFileEnclosure that should be used for executing the given SQL statement
     *
     * @param sql the SQL to execute
     * @param conn the connection from which a PreparedStatement can be created
     * @param stmtMap the existing map of SQL to PreparedStatements
     * @param obtainKeys whether or not we need to obtain generated keys for INSERT statements
     * @param fragmentedTransaction whether or not the SQL pertains to a fragmented transaction
     *
     * @return a StatementFlowFileEnclosure to use for executing the given SQL statement
     *
     * @throws SQLException if unable to create the appropriate PreparedStatement
     */
    private StatementFlowFileEnclosure getEnclosure(final String sql, final Connection conn, final Map<String, StatementFlowFileEnclosure> stmtMap,
        final boolean obtainKeys, final boolean fragmentedTransaction) throws SQLException {
        StatementFlowFileEnclosure enclosure = stmtMap.get(sql);
        if (enclosure != null) {
            return enclosure;
        }

        if (obtainKeys) {
            // Create a new Prepared Statement, requesting that it return the generated keys.
            PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

            if (stmt == null) {
                // since we are passing Statement.RETURN_GENERATED_KEYS, calls to conn.prepareStatement will
                // in some cases (at least for DerbyDB) return null.
                // We will attempt to recompile the statement without the generated keys being returned.
                stmt = conn.prepareStatement(sql);
            }

            // If we need to obtain keys, then we cannot do a Batch Update. In this case,
            // we don't need to store the PreparedStatement in the Map because we aren't
            // doing an addBatch/executeBatch. Instead, we will use the statement once
            // and close it.
            return new StatementFlowFileEnclosure(stmt);
        } else if (fragmentedTransaction) {
            // We cannot use Batch Updates if we have a transaction that spans multiple FlowFiles.
            // If we did, we could end up processing the statements out of order. It's quite possible
            // that we could refactor the code some to allow for this, but as it is right now, this
            // could cause problems. This is because we have a Map<String, StatementFlowFileEnclosure>.
            // If we had a transaction that needed to execute Stmt A with some parameters, then Stmt B with
            // some parameters, then Stmt A with different parameters, this would become problematic because
            // the executeUpdate would be evaluated first for Stmt A (the 1st and 3rd statements, and then
            // the second statement would be evaluated).
            final PreparedStatement stmt = conn.prepareStatement(sql);
            return new StatementFlowFileEnclosure(stmt);
        }

        final PreparedStatement stmt = conn.prepareStatement(sql);
        enclosure = new StatementFlowFileEnclosure(stmt);
        stmtMap.put(sql, enclosure);
        return enclosure;
    }


    /**
     * Determines the SQL statement that should be executed for the given FlowFile
     *
     * @param session the session that can be used to access the given FlowFile
     * @param flowFile the FlowFile whose SQL statement should be executed
     *
     * @return the SQL that is associated with the given FlowFile
     */
    private String getSQL(final ProcessSession session, final FlowFile flowFile) {
        // Read the SQL from the FlowFile's content
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });

        // Create the PreparedStatement to use for this FlowFile.
        final String sql = new String(buffer, StandardCharsets.UTF_8);
        return sql;
    }


    /**
     * Sets all of the appropriate parameters on the given PreparedStatement, based on the given FlowFile attributes.
     *
     * @param stmt the statement to set the parameters on
     * @param attributes the attributes from which to derive parameter indices, values, and types
     * @throws SQLException if the PreparedStatement throws a SQLException when the appropriate setter is called
     */
    private void setParameters(final PreparedStatement stmt, final Map<String, String> attributes) throws SQLException {
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Matcher matcher = SQL_TYPE_ATTRIBUTE_PATTERN.matcher(key);
            if (matcher.matches()) {
                final int parameterIndex = Integer.parseInt(matcher.group(1));

                final boolean isNumeric = NUMBER_PATTERN.matcher(entry.getValue()).matches();
                if (!isNumeric) {
                    throw new ProcessException("Value of the " + key + " attribute is '" + entry.getValue() + "', which is not a valid JDBC numeral type");
                }

                final int jdbcType = Integer.parseInt(entry.getValue());
                final String valueAttrName = "sql.args." + parameterIndex + ".value";
                final String parameterValue = attributes.get(valueAttrName);

                try {
                    setParameter(stmt, valueAttrName, parameterIndex, parameterValue, jdbcType);
                } catch (final NumberFormatException nfe) {
                    throw new ProcessException("The value of the " + valueAttrName + " is '" + parameterValue + "', which cannot be converted into the necessary data type", nfe);
                } catch (ParseException pe) {
                    throw new ProcessException("The value of the " + valueAttrName + " is '" + parameterValue + "', which cannot be converted to a timestamp", pe);
                }
            }
        }
    }


    /**
     * Determines which relationship the given FlowFiles should go to, based on a transaction timing out or
     * transaction information not being present. If the FlowFiles should be processed and not transferred
     * to any particular relationship yet, will return <code>null</code>
     *
     * @param flowFiles the FlowFiles whose relationship is to be determined
     * @param transactionTimeoutMillis the maximum amount of time (in milliseconds) that we should wait
     *            for all FlowFiles in a transaction to be present before routing to failure
     * @return the appropriate relationship to route the FlowFiles to, or <code>null</code> if the FlowFiles
     *         should instead be processed
     */
    Relationship determineRelationship(final List<FlowFile> flowFiles, final Long transactionTimeoutMillis) {
        int selectedNumFragments = 0;
        final BitSet bitSet = new BitSet();

        for (final FlowFile flowFile : flowFiles) {
            final String fragmentCount = flowFile.getAttribute(FRAGMENT_COUNT_ATTR);
            if (fragmentCount == null && flowFiles.size() == 1) {
                return null;
            } else if (fragmentCount == null) {
                getLogger().error("Cannot process {} because there are {} FlowFiles with the same fragment.identifier "
                    + "attribute but not all FlowFiles have a fragment.count attribute; routing all to failure", new Object[] {flowFile, flowFiles.size()});
                return REL_FAILURE;
            }

            final int numFragments;
            try {
                numFragments = Integer.parseInt(fragmentCount);
            } catch (final NumberFormatException nfe) {
                getLogger().error("Cannot process {} because the fragment.count attribute has a value of '{}', which is not an integer; "
                    + "routing all FlowFiles with this fragment.identifier to failure", new Object[] {flowFile, fragmentCount});
                return REL_FAILURE;
            }

            if (numFragments < 1) {
                getLogger().error("Cannot process {} because the fragment.count attribute has a value of '{}', which is not a positive integer; "
                    + "routing all FlowFiles with this fragment.identifier to failure", new Object[] {flowFile, fragmentCount});
                return REL_FAILURE;
            }

            if (selectedNumFragments == 0) {
                selectedNumFragments = numFragments;
            } else if (numFragments != selectedNumFragments) {
                getLogger().error("Cannot process {} because the fragment.count attribute has different values for different FlowFiles with the same fragment.identifier; "
                    + "routing all FlowFiles with this fragment.identifier to failure", new Object[] {flowFile});
                return REL_FAILURE;
            }

            final String fragmentIndex = flowFile.getAttribute(FRAGMENT_INDEX_ATTR);
            if (fragmentIndex == null) {
                getLogger().error("Cannot process {} because the fragment.index attribute is missing; "
                    + "routing all FlowFiles with this fragment.identifier to failure", new Object[] {flowFile});
                return REL_FAILURE;
            }

            final int idx;
            try {
                idx = Integer.parseInt(fragmentIndex);
            } catch (final NumberFormatException nfe) {
                getLogger().error("Cannot process {} because the fragment.index attribute has a value of '{}', which is not an integer; "
                    + "routing all FlowFiles with this fragment.identifier to failure", new Object[] {flowFile, fragmentIndex});
                return REL_FAILURE;
            }

            if (idx < 0) {
                getLogger().error("Cannot process {} because the fragment.index attribute has a value of '{}', which is not a positive integer; "
                    + "routing all FlowFiles with this fragment.identifier to failure", new Object[] {flowFile, fragmentIndex});
                return REL_FAILURE;
            }

            if (bitSet.get(idx)) {
                getLogger().error("Cannot process {} because it has the same value for the fragment.index attribute as another FlowFile with the same fragment.identifier; "
                    + "routing all FlowFiles with this fragment.identifier to failure", new Object[] {flowFile});
                return REL_FAILURE;
            }

            bitSet.set(idx);
        }

        if (selectedNumFragments == flowFiles.size()) {
            return null; // no relationship to route FlowFiles to yet - process the FlowFiles.
        }

        long latestQueueTime = 0L;
        for (final FlowFile flowFile : flowFiles) {
            if (flowFile.getLastQueueDate() != null && flowFile.getLastQueueDate() > latestQueueTime) {
                latestQueueTime = flowFile.getLastQueueDate();
            }
        }

        if (transactionTimeoutMillis != null) {
            if (latestQueueTime > 0L && System.currentTimeMillis() - latestQueueTime > transactionTimeoutMillis) {
                getLogger().error("The transaction timeout has expired for the following FlowFiles; they will be routed to failure: {}", new Object[] {flowFiles});
                return REL_FAILURE;
            }
        }

        getLogger().debug("Not enough FlowFiles for transaction. Returning all FlowFiles to queue");
        return Relationship.SELF; // not enough FlowFiles for this transaction. Return them all to queue.
    }

    /**
     * Determines how to map the given value to the appropriate JDBC data type and sets the parameter on the
     * provided PreparedStatement
     *
     * @param stmt the PreparedStatement to set the parameter on
     * @param attrName the name of the attribute that the parameter is coming from - for logging purposes
     * @param parameterIndex the index of the SQL parameter to set
     * @param parameterValue the value of the SQL parameter to set
     * @param jdbcType the JDBC Type of the SQL parameter to set
     * @throws SQLException if the PreparedStatement throws a SQLException when calling the appropriate setter
     */
    private void setParameter(final PreparedStatement stmt, final String attrName, final int parameterIndex, final String parameterValue, final int jdbcType) throws SQLException, ParseException {
        if (parameterValue == null) {
            stmt.setNull(parameterIndex, jdbcType);
        } else {
            switch (jdbcType) {
                case Types.BIT:
                case Types.BOOLEAN:
                    stmt.setBoolean(parameterIndex, Boolean.parseBoolean(parameterValue));
                    break;
                case Types.TINYINT:
                    stmt.setByte(parameterIndex, Byte.parseByte(parameterValue));
                    break;
                case Types.SMALLINT:
                    stmt.setShort(parameterIndex, Short.parseShort(parameterValue));
                    break;
                case Types.INTEGER:
                    stmt.setInt(parameterIndex, Integer.parseInt(parameterValue));
                    break;
                case Types.BIGINT:
                    stmt.setLong(parameterIndex, Long.parseLong(parameterValue));
                    break;
                case Types.REAL:
                    stmt.setFloat(parameterIndex, Float.parseFloat(parameterValue));
                    break;
                case Types.FLOAT:
                case Types.DOUBLE:
                    stmt.setDouble(parameterIndex, Double.parseDouble(parameterValue));
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    stmt.setBigDecimal(parameterIndex, new BigDecimal(parameterValue));
                    break;
                case Types.DATE:
                    stmt.setDate(parameterIndex, new Date(Long.parseLong(parameterValue)));
                    break;
                case Types.TIME:
                    stmt.setTime(parameterIndex, new Time(Long.parseLong(parameterValue)));
                    break;
                case Types.TIMESTAMP:
                    long lTimestamp=0L;

                    if(LONG_PATTERN.matcher(parameterValue).matches()){
                        lTimestamp = Long.parseLong(parameterValue);
                    }else {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                        java.util.Date parsedDate = dateFormat.parse(parameterValue);
                        lTimestamp = parsedDate.getTime();
                    }

                    stmt.setTimestamp(parameterIndex, new Timestamp(lTimestamp));

                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                    stmt.setString(parameterIndex, parameterValue);
                    break;
                default:
                    stmt.setObject(parameterIndex, parameterValue, jdbcType);
                    break;
            }
        }
    }


    /**
     * A FlowFileFilter that is responsible for ensuring that the FlowFiles returned either belong
     * to the same "fragmented transaction" (i.e., 1 transaction whose information is fragmented
     * across multiple FlowFiles) or that none of the FlowFiles belongs to a fragmented transaction
     */
    static class TransactionalFlowFileFilter implements FlowFileFilter {
        private String selectedId = null;
        private int numSelected = 0;
        private boolean ignoreFragmentIdentifiers = false;

        public boolean isFragmentedTransaction() {
            return !ignoreFragmentIdentifiers;
        }

        @Override
        public FlowFileFilterResult filter(final FlowFile flowFile) {
            final String fragmentId = flowFile.getAttribute(FRAGMENT_ID_ATTR);
            final String fragCount = flowFile.getAttribute(FRAGMENT_COUNT_ATTR);

            // if first FlowFile selected is not part of a fragmented transaction, then
            // we accept any FlowFile that is also not part of a fragmented transaction.
            if (ignoreFragmentIdentifiers) {
                if (fragmentId == null || "1".equals(fragCount)) {
                    return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                } else {
                    return FlowFileFilterResult.REJECT_AND_CONTINUE;
                }
            }

            if (fragmentId == null || "1".equals(fragCount)) {
                if (selectedId == null) {
                    // Only one FlowFile in the transaction.
                    ignoreFragmentIdentifiers = true;
                    return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                } else {
                    // we've already selected 1 FlowFile, and this one doesn't match.
                    return FlowFileFilterResult.REJECT_AND_CONTINUE;
                }
            }

            if (selectedId == null) {
                // select this fragment id as the chosen one.
                selectedId = fragmentId;
                numSelected++;
                return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
            }

            if (selectedId.equals(fragmentId)) {
                // fragment id's match. Find out if we have all of the necessary fragments or not.
                final int numFragments;
                if (NUMBER_PATTERN.matcher(fragCount).matches()) {
                    numFragments = Integer.parseInt(fragCount);
                } else {
                    numFragments = Integer.MAX_VALUE;
                }

                if (numSelected >= numFragments - 1) {
                    // We have all of the fragments we need for this transaction.
                    return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                } else {
                    // We still need more fragments for this transaction, so accept this one and continue.
                    numSelected++;
                    return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                }
            } else {
                return FlowFileFilterResult.REJECT_AND_CONTINUE;
            }
        }
    }


    /**
     * A simple, immutable data structure to hold a List of FlowFiles and an indicator as to whether
     * or not those FlowFiles represent a "fragmented transaction" - that is, a collection of FlowFiles
     * that all must be executed as a single transaction (we refer to it as a fragment transaction
     * because the information for that transaction, including SQL and the parameters, is fragmented
     * across multiple FlowFiles).
     */
    private static class FlowFilePoll {
        private final List<FlowFile> flowFiles;
        private final boolean fragmentedTransaction;

        public FlowFilePoll(final List<FlowFile> flowFiles, final boolean fragmentedTransaction) {
            this.flowFiles = flowFiles;
            this.fragmentedTransaction = fragmentedTransaction;
        }

        public List<FlowFile> getFlowFiles() {
            return flowFiles;
        }

        public boolean isFragmentedTransaction() {
            return fragmentedTransaction;
        }
    }


    /**
     * A simple, immutable data structure to hold a Prepared Statement and a List of FlowFiles
     * for which that statement should be evaluated.
     */
    private static class StatementFlowFileEnclosure {
        private final PreparedStatement statement;
        private final List<FlowFile> flowFiles = new ArrayList<>();

        public StatementFlowFileEnclosure(final PreparedStatement statement) {
            this.statement = statement;
        }

        public PreparedStatement getStatement() {
            return statement;
        }

        public List<FlowFile> getFlowFiles() {
            return flowFiles;
        }

        public void addFlowFile(final FlowFile flowFile) {
            this.flowFiles.add(flowFile);
        }

        @Override
        public int hashCode() {
            return statement.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return false;
            }
            if (!(obj instanceof StatementFlowFileEnclosure)) {
                return false;
            }

            final StatementFlowFileEnclosure other = (StatementFlowFileEnclosure) obj;
            return statement.equals(other.getStatement());
        }
    }
}
