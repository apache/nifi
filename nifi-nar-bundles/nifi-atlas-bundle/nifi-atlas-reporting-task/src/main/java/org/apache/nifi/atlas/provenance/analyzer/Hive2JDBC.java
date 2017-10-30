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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.Tuple;

import java.net.URI;
import java.util.Set;

import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.ATTR_INPUT_TABLES;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.ATTR_OUTPUT_TABLES;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.parseTableNames;

/**
 * Analyze provenance events for Hive2 using JDBC.
 * <ul>
 * <li>If a Provenance event has 'query.input.tables' or 'query.output.tables' attributes then 'hive_table' DataSet reference is created:
 * <ul>
 * <li>qualifiedName=tableName@clusterName (example: myTable@cl1)
 * <li>name=tableName (example: myTable)
 * </ul>
 * </li>
 * <li>If not, 'hive_database' DataSet reference is created from transit URI:
 * <ul>
 * <li>qualifiedName=dbName@clusterName (example: default@cl1)
 * <li>dbName (example: default)
 * </ul>
 * </li>
 * </ul>
 */
public class Hive2JDBC extends AbstractHiveAnalyzer {

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        // Replace the colon so that the schema in the URI can be parsed correctly.
        final String transitUri = event.getTransitUri().replaceFirst("^jdbc:hive2", "jdbc-hive2");
        final URI uri = parseUri(transitUri);
        final String clusterName = context.getClusterResolver().fromHostNames(uri.getHost());
        // Remove the heading '/'
        final String path = uri.getPath();
        // If uri does not contain database name, then use 'default' as database name.
        final String connectedDatabaseName = path == null || path.isEmpty() ? "default" : path.substring(1);

        final Set<Tuple<String, String>> inputTables = parseTableNames(connectedDatabaseName, event.getAttribute(ATTR_INPUT_TABLES));
        final Set<Tuple<String, String>> outputTables = parseTableNames(connectedDatabaseName, event.getAttribute(ATTR_OUTPUT_TABLES));

        if (inputTables.isEmpty() && outputTables.isEmpty()) {
            // If input/output tables are unknown, create database level lineage.
            return getDatabaseRef(event.getComponentId(), event.getEventType(),
                    clusterName, connectedDatabaseName);
        }

        final DataSetRefs refs = new DataSetRefs(event.getComponentId());
        addRefs(refs, true, clusterName, inputTables);
        addRefs(refs, false, clusterName, outputTables);
        return refs;
    }

    private DataSetRefs getDatabaseRef(String componentId, ProvenanceEventType eventType,
                                       String clusterName, String databaseName) {
        final Referenceable ref = createDatabaseRef(clusterName, databaseName);

        return singleDataSetRef(componentId, eventType, ref);
    }

    private void addRefs(DataSetRefs refs, boolean isInput, String clusterName,
                                       Set<Tuple<String, String>> tableNames) {
        tableNames.forEach(tableName -> {
            final Referenceable ref = createTableRef(clusterName, tableName);
            if (isInput) {
                refs.addInput(ref);
            } else {
                refs.addOutput(ref);
            }
        });
    }

    @Override
    public String targetTransitUriPattern() {
        return "^jdbc:hive2://.+$";
    }
}
