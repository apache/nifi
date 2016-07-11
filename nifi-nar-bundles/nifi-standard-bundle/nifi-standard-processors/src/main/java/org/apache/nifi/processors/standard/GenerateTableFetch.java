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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;


@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "select", "jdbc", "query", "database", "fetch"})
@CapabilityDescription("Generates SQL select queries that fetch \"pages\" of rows from a table. The partition size property, along with the db.table.count "
        + "FlowFile attribute, determine the size and number of pages and generated FlowFiles.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "db.table.fullname", description = "Contains the fully-qualified name of the table to fetch rows from."),
        @ReadsAttribute(attribute = "db.table.count", description = "Contains the number of rows in the specified table")
})
public class GenerateTableFetch extends AbstractProcessor {

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile containing SQL query.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to process the incoming FlowFile.")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile input")
            .build();

    private static final Set<Relationship> relationships;

    public static final PropertyDescriptor DB_TYPE;

    public static final PropertyDescriptor COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("gen-table-fetch-column-names")
            .displayName("Columns to Return")
            .description("A comma-separated list of column names to be used in the query. If your database requires "
                    + "special treatment of the names (quoting, e.g.), each name should include such treatment. If no "
                    + "column names are supplied, all columns in the specified table will be returned.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PARTITION_SIZE = new PropertyDescriptor.Builder()
            .name("gen-table-fetch-partition-size")
            .displayName("Partition Size")
            .description("The number of result rows to be fetched by each generated SQL statement. The total number of rows in "
                    + "the table divided by the partition size gives the number of SQL statements (i.e. FlowFiles) generated. A "
                    + "value of zero indicates that a single FlowFile is to be generated whose SQL statement will fetch all rows "
                    + "in the table.")
            .defaultValue("10000")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();


    private static final List<PropertyDescriptor> propDescriptors;
    protected final static Map<String, DatabaseAdapter> dbAdapters = new HashMap<>();

    static {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        r.add(REL_ORIGINAL);
        relationships = Collections.unmodifiableSet(r);


        // Load the DatabaseAdapters
        ServiceLoader<DatabaseAdapter> dbAdapterLoader = ServiceLoader.load(DatabaseAdapter.class);
        dbAdapterLoader.forEach(it -> dbAdapters.put(it.getName(), it));

        DB_TYPE = new PropertyDescriptor.Builder()
                .name("gen-table-fetch-db-type")
                .displayName("Database Type")
                .description("The type/flavor of database, used for generating database-specific code. In many cases the Generic type "
                        + "should suffice, but some databases (such as Oracle) require custom SQL clauses. ")
                .allowableValues(dbAdapters.keySet())
                .defaultValue(dbAdapters.values().stream().findFirst().get().getName())
                .required(true)
                .build();


        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(COLUMN_NAMES);
        pds.add(DB_TYPE);
        pds.add(PARTITION_SIZE);
        propDescriptors = Collections.unmodifiableList(pds);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final String tableName = flowFile.getAttribute(ListDatabaseTables.DB_TABLE_FULLNAME);
        if (StringUtils.isEmpty(tableName)) {
            logger.error("FlowFile attribute " + ListDatabaseTables.DB_TABLE_FULLNAME + " not set, routing to failure");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        final String rowCountAttribute = flowFile.getAttribute(ListDatabaseTables.DB_TABLE_COUNT);
        if (StringUtils.isEmpty(rowCountAttribute)) {
            logger.error("FlowFile attribute " + ListDatabaseTables.DB_TABLE_COUNT + " not set, routing to failure");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        final int rowCount = Integer.parseInt(rowCountAttribute);
        final String columnNames = context.getProperty(COLUMN_NAMES).getValue();
        final int partitionSize = context.getProperty(PARTITION_SIZE).evaluateAttributeExpressions(flowFile).asInteger();
        final int numberOfFetches = (partitionSize == 0) ? rowCount : (rowCount / partitionSize) + (rowCount % partitionSize == 0 ? 0 : 1);

        final DatabaseAdapter dbAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());

        // If the partition size is zero, get everything
        if (partitionSize == 0) {
            String query = dbAdapter.getSelectStatement(tableName, columnNames, null, null, null, null);
            FlowFile sqlFlowFile = session.create(flowFile);
            sqlFlowFile = session.write(sqlFlowFile, out -> {
                out.write(query.getBytes());
            });
            session.transfer(sqlFlowFile, REL_SUCCESS);
        } else {

            // Generate SQL statements to read "pages" of data
            for (int i = 0; i < numberOfFetches; i++) {
                FlowFile sqlFlowFile = null;
                try {
                    String query = dbAdapter.getSelectStatement(tableName, columnNames, null, null, partitionSize, i * partitionSize);
                    sqlFlowFile = session.create(flowFile);
                    sqlFlowFile = session.write(sqlFlowFile, out -> {
                        out.write(query.getBytes());
                    });
                    session.transfer(sqlFlowFile, REL_SUCCESS);

                } catch (Exception e) {
                    logger.error("Error while generating SQL statement", e);
                    if (sqlFlowFile != null) {
                        session.remove(sqlFlowFile);
                    }
                }
            }
        }
        session.transfer(flowFile, REL_ORIGINAL);

    }
}
