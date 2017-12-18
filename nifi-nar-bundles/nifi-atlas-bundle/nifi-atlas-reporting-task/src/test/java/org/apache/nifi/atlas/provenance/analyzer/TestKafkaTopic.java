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
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.atlas.resolver.ClusterResolvers;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.when;

public class TestKafkaTopic {

    @Test
    public void testPublishKafka() {
        final String processorName = "PublishKafka";
        final String transitUri = "PLAINTEXT://0.example.com:6667/topicA";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("topicA", ref.get(ATTR_NAME));
        assertEquals("topicA", ref.get("topic"));
        assertEquals("topicA@cluster1", ref.get(ATTR_QUALIFIED_NAME));
    }

    @Test
    public void testPublishKafkaMultipleBrokers() {
        final String processorName = "PublishKafka";
        final String transitUri = "PLAINTEXT://0.example.com:6667,1.example.com:6667/topicA";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("topicA", ref.get(ATTR_NAME));
        assertEquals("topicA", ref.get("topic"));
        assertEquals("topicA@cluster1", ref.get(ATTR_QUALIFIED_NAME));
    }

    @Test
    public void testConsumeKafka() {
        final String processorName = "ConsumeKafka";
        final String transitUri = "PLAINTEXT://0.example.com:6667/topicA";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.RECEIVE);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(1, refs.getInputs().size());
        assertEquals(0, refs.getOutputs().size());
        Referenceable ref = refs.getInputs().iterator().next();
        assertEquals("kafka_topic", ref.getTypeName());
        assertEquals("topicA", ref.get(ATTR_NAME));
        assertEquals("topicA", ref.get("topic"));
        assertEquals("topicA@cluster1", ref.get(ATTR_QUALIFIED_NAME));
    }

    @Test
    public void testConsumeKafkaRecord_0_10() {
        final String processorName = "ConsumeKafkaRecord_0_10";
        final String transitUri = "PLAINTEXT://0.example.com:6667/topicA";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.RECEIVE);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(1, refs.getInputs().size());
        assertEquals(0, refs.getOutputs().size());
        Referenceable ref = refs.getInputs().iterator().next();
        assertEquals("kafka_topic", ref.getTypeName());
        assertEquals("topicA", ref.get(ATTR_NAME));
        assertEquals("topicA", ref.get("topic"));
        assertEquals("topicA@cluster1", ref.get(ATTR_QUALIFIED_NAME));
    }

}
