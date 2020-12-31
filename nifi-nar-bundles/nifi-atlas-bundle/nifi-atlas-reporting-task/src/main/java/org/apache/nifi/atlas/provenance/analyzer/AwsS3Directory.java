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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.net.URI;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze a transit URI as an AWS S3 directory (skipping the object name).
 * <li>qualifiedName=s3a://bucket/path@namespace (example: s3a://mybucket/mydir@ns1)
 * <li>name=/path (example: /mydir)
 */
public class AwsS3Directory extends AbstractNiFiProvenanceEventAnalyzer {

    private static final String TYPE_DIRECTORY = "aws_s3_pseudo_dir";
    private static final String TYPE_BUCKET = "aws_s3_bucket";

    public static final String ATTR_OBJECT_PREFIX = "objectPrefix";
    public static final String ATTR_BUCKET = "bucket";

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        final String transitUri = event.getTransitUri();
        if (transitUri == null) {
            return null;
        }

        final String directoryUri;
        if (StringUtils.countMatches(transitUri, '/') > 3) {
            // directory exists => drop last '/' and the file name
            directoryUri = transitUri.substring(0, transitUri.lastIndexOf('/'));
        } else {
            // no directory => keep last '/', drop only the file name
            directoryUri = transitUri.substring(0, transitUri.lastIndexOf('/') + 1);
        }
        final URI uri = parseUri(directoryUri);

        final String namespace = context.getNamespaceResolver().fromHostNames(uri.getHost());

        final Referenceable ref = createDirectoryRef(uri, namespace);

        return singleDataSetRef(event.getComponentId(), event.getEventType(), ref);
    }

    @Override
    public String targetTransitUriPattern() {
        return "^s3a://.+/.+$";
    }

    private Referenceable createDirectoryRef(URI uri, String namespace) {
        final Referenceable ref = new Referenceable(TYPE_DIRECTORY);

        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(namespace, uri.toString().toLowerCase()));
        ref.set(ATTR_NAME, uri.getPath().toLowerCase());
        ref.set(ATTR_OBJECT_PREFIX, uri.getPath().toLowerCase());
        ref.set(ATTR_BUCKET, createBucketRef(uri, namespace));

        return ref;
    }

    private Referenceable createBucketRef(URI uri, String namespace) {
        final Referenceable ref = new Referenceable(TYPE_BUCKET);

        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(namespace, String.format("%s://%s", uri.getScheme(), uri.getAuthority())));
        ref.set(ATTR_NAME, uri.getAuthority());

        return ref;
    }
}
