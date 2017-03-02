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
package org.apache.nifi.processors.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.hive.HiveDBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@SeeAlso(SelectHiveQL.class)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "hive", "put", "database", "update", "insert"})
@CapabilityDescription("Executes a HiveQL DDL/DML command (UPDATE, INSERT, e.g.). The content of an incoming FlowFile is expected to be the HiveQL command "
        + "to execute. The HiveQL command may use the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes "
        + "with the naming convention hiveql.args.N.type and hiveql.args.N.value, where N is a positive integer. The hiveql.args.N.type is expected to be "
        + "a number indicating the JDBC Type. The content of the FlowFile is expected to be in UTF-8 format.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "hiveql.args.N.type", description = "Incoming FlowFiles are expected to be parametrized HiveQL statements. The type of each Parameter is specified as an integer "
                + "that represents the JDBC Type of the parameter."),
        @ReadsAttribute(attribute = "hiveql.args.N.value", description = "Incoming FlowFiles are expected to be parametrized HiveQL statements. The value of the Parameters are specified as "
                + "hiveql.args.1.value, hiveql.args.2.value, hiveql.args.3.value, and so on. The type of the hiveql.args.1.value Parameter is specified by the hiveql.args.1.type attribute.")
})
public class PutHiveQL extends AbstractHiveQLProcessor {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("hive-batch-size")
            .displayName("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor STATEMENT_DELIMITER = new PropertyDescriptor.Builder()
            .name("statement-delimiter")
            .displayName("Statement Delimiter")
            .description("Statement Delimiter used to separate SQL statements in a multiple statement script")
            .required(true)
            .defaultValue(";")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();
    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();


    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is built only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(HIVE_DBCP_SERVICE);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(CHARSET);
        _propertyDescriptors.add(STATEMENT_DELIMITER);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final List<FlowFile> flowFiles = session.get(batchSize);

        if (flowFiles.isEmpty()) {
            return;
        }

        final long startNanos = System.nanoTime();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
        final HiveDBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(HiveDBCPService.class);
        final String statementDelimiter =   context.getProperty(STATEMENT_DELIMITER).getValue();

        try (final Connection conn = dbcpService.getConnection()) {

            for (FlowFile flowFile : flowFiles) {
                try {
                    final String script = getHiveQL(session, flowFile, charset);
                    String regex = "(?<!\\\\)" + Pattern.quote(statementDelimiter);

                    String[] hiveQLs = script.split(regex);

                    int loc = 1;
                    for (String hiveQL: hiveQLs) {
                        getLogger().debug("HiveQL: {}", new Object[]{hiveQL});

                        if (!StringUtils.isEmpty(hiveQL.trim())) {
                            final PreparedStatement stmt = conn.prepareStatement(hiveQL.trim());

                            // Get ParameterMetadata
                            // Hive JDBC Doesn't support this yet:
                            // ParameterMetaData pmd = stmt.getParameterMetaData();
                            // int paramCount = pmd.getParameterCount();

                            int paramCount = StringUtils.countMatches(hiveQL, "?");

                            if (paramCount > 0) {
                                loc = setParameters(loc, stmt, paramCount, flowFile.getAttributes());
                            }

                            // Execute the statement
                            stmt.execute();
                        }
                    }
                    // Emit a Provenance SEND event
                    final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                    session.getProvenanceReporter().send(flowFile, dbcpService.getConnectionURL(), transmissionMillis, true);
                    session.transfer(flowFile, REL_SUCCESS);

                } catch (final SQLException e) {

                    if (e instanceof SQLNonTransientException) {
                        getLogger().error("Failed to update Hive for {} due to {}; routing to failure", new Object[]{flowFile, e});
                        session.transfer(flowFile, REL_FAILURE);
                    } else {
                        getLogger().error("Failed to update Hive for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry", new Object[]{flowFile, e});
                        flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, REL_RETRY);
                    }

                }
            }
        } catch (final SQLException sqle) {
            // There was a problem getting the connection, yield and retry the flowfiles
            getLogger().error("Failed to get Hive connection due to {}; it is possible that retrying the operation will succeed, so routing to retry", new Object[]{sqle});
            session.transfer(flowFiles, REL_RETRY);
            context.yield();
        }
    }
}