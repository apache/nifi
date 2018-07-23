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
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_PATH;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze a transit URI as a file system path.
 * <li>qualifiedName=/path/fileName@hostname (example: /tmp/dir/filename.txt@host.example.com)
 * <li>name=/path/fileName (example: /tmp/dir/filename.txt)
 */
public class FilePath extends AbstractNiFiProvenanceEventAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(FilePath.class);

    private static final String TYPE = "fs_path";

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        final Referenceable ref = new Referenceable(TYPE);
        final URI uri = parseUri(event.getTransitUri());
        final String clusterName;
        try {
            // use hostname in uri if available for remote path.
            final String uriHost = uri.getHost();
            final String hostname = StringUtils.isEmpty(uriHost) ? InetAddress.getLocalHost().getHostName() : uriHost;
            clusterName = context.getClusterResolver().fromHostNames(hostname);
        } catch (UnknownHostException e) {
            logger.warn("Failed to get localhost name due to " + e, e);
            return null;
        }

        final String path = uri.getPath();
        ref.set(ATTR_NAME, path);
        ref.set(ATTR_PATH, path);
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, path));

        return singleDataSetRef(event.getComponentId(), event.getEventType(), ref);
    }

    @Override
    public String targetTransitUriPattern() {
        return "^file:/.+$";
    }
}
