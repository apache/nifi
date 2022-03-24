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

import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.net.URI;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_CLUSTER_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_PATH;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze a transit URI as a HDFS path. Return file or directory path depending on FilesystemPathsLevel setting.
 * <li>qualifiedName=/path[/fileName]@namespace (example: /app/warehouse/hive/db/default[/datafile]@ns1)
 * <li>name=/path[/fileName] (example: /app/warehouse/hive/db/default[/datafile])
 */
public class HDFSPath extends AbstractFileSystemPathAnalyzer {

    private static final String TYPE = "hdfs_path";

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        final Referenceable ref = new Referenceable(TYPE);
        final URI uri = parseUri(event.getTransitUri());
        final String namespace = context.getNamespaceResolver().fromHostNames(uri.getHost());

        final String path = getPath(context, uri);

        ref.set(ATTR_NAME, path);
        ref.set(ATTR_PATH, path);
        // The attribute 'clusterName' is in the 'hdfs_path' Atlas entity so it cannot be changed.
        //  Using 'namespace' as value for lack of better solution.
        ref.set(ATTR_CLUSTER_NAME, namespace);
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(namespace, path));

        return singleDataSetRef(event.getComponentId(), event.getEventType(), ref);
    }

    @Override
    public String targetTransitUriPattern() {
        return "^hdfs://.+$";
    }
}
