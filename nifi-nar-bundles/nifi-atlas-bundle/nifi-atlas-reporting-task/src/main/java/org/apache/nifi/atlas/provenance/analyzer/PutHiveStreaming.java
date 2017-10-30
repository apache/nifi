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
import org.apache.nifi.util.Tuple;

import java.net.URI;
import java.util.Set;

import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.ATTR_OUTPUT_TABLES;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.parseTableNames;

/**
 * Analyze provenance events for PutHiveStreamingProcessor.
 * <li>qualifiedName=tableName@clusterName (example: myTable@cl1)
 * <li>name=tableName (example: myTable)
 */
public class PutHiveStreaming extends AbstractHiveAnalyzer {

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        final URI uri = parseUri(event.getTransitUri());
        final String clusterName = context.getClusterResolver().fromHostNames(uri.getHost());
        final Set<Tuple<String, String>> outputTables = parseTableNames(null, event.getAttribute(ATTR_OUTPUT_TABLES));
        if (outputTables.isEmpty()) {
            return null;
        }

        final DataSetRefs refs = new DataSetRefs(event.getComponentId());
        outputTables.forEach(tableName -> {
            final Referenceable ref = createTableRef(clusterName, tableName);
            refs.addOutput(ref);
        });
        return refs;
    }

    @Override
    public String targetComponentTypePattern() {
        return "^PutHiveStreaming$";
    }
}
