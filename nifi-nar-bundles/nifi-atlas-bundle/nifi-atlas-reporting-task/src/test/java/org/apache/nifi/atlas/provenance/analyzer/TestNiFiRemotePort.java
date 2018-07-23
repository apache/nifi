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
import org.apache.nifi.atlas.reporting.ITReportLineageToAtlas;
import org.apache.nifi.atlas.resolver.ClusterResolvers;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.flowfile.attributes.SiteToSiteAttributes;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.when;

/**
 * Tests for RemotePorts.
 * More complex and detailed tests are available at {@link ITReportLineageToAtlas}.
 */
public class TestNiFiRemotePort {

    @Test
    public void testRemoteInputPortHTTP() {
        final String componentType = "Remote Input Port";
        final String transitUri = "http://0.example.com:8080/nifi-api/data-transfer/input-ports/port-guid/transactions/tx-guid/flow-files";
        final ProvenanceEventRecord sendEvent = Mockito.mock(ProvenanceEventRecord.class);
        when(sendEvent.getEventId()).thenReturn(123L);
        when(sendEvent.getComponentId()).thenReturn("port-guid");
        when(sendEvent.getComponentType()).thenReturn(componentType);
        when(sendEvent.getTransitUri()).thenReturn(transitUri);
        when(sendEvent.getEventType()).thenReturn(ProvenanceEventType.SEND);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final List<ConnectionStatus> connections = new ArrayList<>();
        final ConnectionStatus connection = new ConnectionStatus();
        connection.setDestinationId("port-guid");
        connection.setDestinationName("inputPortA");
        connections.add(connection);

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);
        when(context.findConnectionTo(matches("port-guid"))).thenReturn(connections);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(componentType, transitUri, sendEvent.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, sendEvent);
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());
        assertEquals(1, refs.getComponentIds().size());
        // Should report connected componentId.
        assertTrue(refs.getComponentIds().contains("port-guid"));

        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals(TYPE_NIFI_INPUT_PORT, ref.getTypeName());
        assertEquals("inputPortA", ref.get(ATTR_NAME));
        assertEquals("port-guid@cluster1", ref.get(ATTR_QUALIFIED_NAME));
    }

    @Test
    public void testRemoteOutputPortHTTP() {
        final String componentType = "Remote Output Port";
        final String transitUri = "http://0.example.com:8080/nifi-api/data-transfer/output-ports/port-guid/transactions/tx-guid/flow-files";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentId()).thenReturn("port-guid");
        when(record.getComponentType()).thenReturn(componentType);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.RECEIVE);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final List<ConnectionStatus> connections = new ArrayList<>();
        final ConnectionStatus connection = new ConnectionStatus();
        connection.setSourceId("port-guid");
        connection.setSourceName("outputPortA");
        connections.add(connection);

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);
        when(context.findConnectionFrom(matches("port-guid"))).thenReturn(connections);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(componentType, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(1, refs.getInputs().size());
        assertEquals(0, refs.getOutputs().size());
        Referenceable ref = refs.getInputs().iterator().next();
        assertEquals(TYPE_NIFI_OUTPUT_PORT, ref.getTypeName());
        assertEquals("outputPortA", ref.get(ATTR_NAME));
        assertEquals("port-guid@cluster1", ref.get(ATTR_QUALIFIED_NAME));
    }

    @Test
    public void testRemoteInputPortRAW() {
        final String componentType = "Remote Input Port";
        // The UUID in a Transit Uri is a FlowFile UUID
        final String transitUri = "nifi://0.example.com:8081/580b7989-a80b-4089-b25b-3f5e0103af82";
        final ProvenanceEventRecord sendEvent = Mockito.mock(ProvenanceEventRecord.class);
        when(sendEvent.getEventId()).thenReturn(123L);
        // Component Id is an UUID of the RemoteGroupPort instance acting as a S2S client.
        when(sendEvent.getComponentId()).thenReturn("s2s-client-component-guid");
        when(sendEvent.getComponentType()).thenReturn(componentType);
        when(sendEvent.getTransitUri()).thenReturn(transitUri);
        when(sendEvent.getEventType()).thenReturn(ProvenanceEventType.SEND);
        when(sendEvent.getAttribute(SiteToSiteAttributes.S2S_PORT_ID.key())).thenReturn("remote-port-guid");

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final List<ConnectionStatus> connections = new ArrayList<>();
        final ConnectionStatus connection = new ConnectionStatus();
        connection.setDestinationId("s2s-client-component-guid");
        connection.setDestinationName("inputPortA");
        connections.add(connection);

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);
        when(context.findConnectionTo(matches("s2s-client-component-guid"))).thenReturn(connections);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(componentType, transitUri, sendEvent.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, sendEvent);
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());
        assertEquals(1, refs.getComponentIds().size());
        // Should report connected componentId.
        assertTrue(refs.getComponentIds().contains("s2s-client-component-guid"));

        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals(TYPE_NIFI_INPUT_PORT, ref.getTypeName());
        assertEquals("inputPortA", ref.get(ATTR_NAME));
        assertEquals("remote-port-guid@cluster1", ref.get(ATTR_QUALIFIED_NAME));
    }

    @Test
    public void testRemoteOutputPortRAW() {
        final String componentType = "Remote Output Port";
        // The UUID in a Transit Uri is a FlowFile UUID
        final String transitUri = "nifi://0.example.com:8081/232018cc-a147-40c6-b148-21f9f814e93c";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        // Component Id is an UUID of the RemoteGroupPort instance acting as a S2S client.
        when(record.getComponentId()).thenReturn("s2s-client-component-guid");
        when(record.getComponentType()).thenReturn(componentType);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.RECEIVE);
        when(record.getAttribute(SiteToSiteAttributes.S2S_PORT_ID.key())).thenReturn("remote-port-guid");

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final List<ConnectionStatus> connections = new ArrayList<>();
        final ConnectionStatus connection = new ConnectionStatus();
        connection.setSourceId("s2s-client-component-guid");
        connection.setSourceName("outputPortA");
        connections.add(connection);

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);
        when(context.findConnectionFrom(matches("s2s-client-component-guid"))).thenReturn(connections);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(componentType, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(1, refs.getInputs().size());
        assertEquals(0, refs.getOutputs().size());
        Referenceable ref = refs.getInputs().iterator().next();
        assertEquals(TYPE_NIFI_OUTPUT_PORT, ref.getTypeName());
        assertEquals("outputPortA", ref.get(ATTR_NAME));
        assertEquals("remote-port-guid@cluster1", ref.get(ATTR_QUALIFIED_NAME));
    }

}
