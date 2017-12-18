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
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.util.Tuple;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_CLUSTER_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.toTableNameStr;

public abstract class AbstractHiveAnalyzer extends AbstractNiFiProvenanceEventAnalyzer {

    static final String TYPE_DATABASE = "hive_db";
    static final String TYPE_TABLE = "hive_table";
    static final String ATTR_DB = "db";

    protected Referenceable createDatabaseRef(String clusterName, String databaseName) {
        final Referenceable ref = new Referenceable(TYPE_DATABASE);
        ref.set(ATTR_NAME, databaseName);
        ref.set(ATTR_CLUSTER_NAME, clusterName);
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, databaseName));
        return ref;
    }

    protected Referenceable createTableRef(String clusterName, Tuple<String, String> tableName) {
        final Referenceable ref = new Referenceable(TYPE_TABLE);
        ref.set(ATTR_NAME, tableName.getValue());
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, toTableNameStr(tableName)));
        ref.set(ATTR_DB, createDatabaseRef(clusterName, tableName.getKey()));
        return ref;
    }

}
