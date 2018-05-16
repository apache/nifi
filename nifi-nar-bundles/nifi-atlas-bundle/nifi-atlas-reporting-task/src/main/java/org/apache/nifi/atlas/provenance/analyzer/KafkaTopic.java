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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URI;

/**
 * Analyze a transit URI as a Kafka topic.
 * <li>qualifiedName=topicName@clusterName (example: testTopic@cl1)
 * <li>name=topicName (example: testTopic)
 */
public class KafkaTopic extends AbstractNiFiProvenanceEventAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTopic.class);

    private static final String TYPE = "kafka_topic";
    private static final String ATTR_TOPIC = "topic";

    // PLAINTEXT://0.example.com:6667,1.example.com:6667/topicA
    private static final Pattern URI_PATTERN = Pattern.compile("^.+://([^/]+)/(.+)$");

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        final Referenceable ref = new Referenceable(TYPE);

        final String transitUri = event.getTransitUri();
        if (transitUri == null) {
            return null;
        }

        final Matcher uriMatcher = URI_PATTERN.matcher(transitUri);
        if (!uriMatcher.matches()) {
            logger.warn("Unexpected transit URI: {}", new Object[]{transitUri});
            return null;
        }

        final String[] hostNames = splitHostNames(uriMatcher.group(1));
        final String clusterName = context.getClusterResolver().fromHostNames(hostNames);

        final String topicName = uriMatcher.group(2);

        ref.set(ATTR_NAME, topicName);
        ref.set(ATTR_TOPIC, topicName);
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(clusterName, topicName));
        ref.set(ATTR_URI, transitUri);

        return singleDataSetRef(event.getComponentId(), event.getEventType(), ref);
    }

    @Override
    public String targetComponentTypePattern() {
        return "^(Publish|Consume)Kafka.*$";
    }
}
