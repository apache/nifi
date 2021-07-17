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
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.atlas.resolver.NamespaceResolvers;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.ATTR_OUTPUT_TABLES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.when;

public class TestPutHiveStreaming {

    @Test
    public void testTableLineageHive1() {
        testTableLineage("PutHiveStreaming");
    }

    @Test
    public void testTableLineageHive3() {
        testTableLineage("PutHive3Streaming");
    }

    private void testTableLineage(String processorName) {
        final String transitUri = "thrift://0.example.com:9083";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);
        when(record.getAttribute(ATTR_OUTPUT_TABLES)).thenReturn("database_A.table_A");

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("namespace1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("hive_table", ref.getTypeName());
        assertEquals("table_a", ref.get(ATTR_NAME));
        assertEquals("database_a.table_a@namespace1", ref.get(ATTR_QUALIFIED_NAME));
    }
}
