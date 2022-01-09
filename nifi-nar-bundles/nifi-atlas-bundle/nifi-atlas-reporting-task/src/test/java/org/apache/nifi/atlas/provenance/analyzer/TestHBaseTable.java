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

import static org.apache.nifi.atlas.NiFiTypes.ATTR_CLUSTER_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URI;
import static org.apache.nifi.atlas.provenance.analyzer.HBaseTable.ATTR_NAMESPACE;
import static org.apache.nifi.atlas.provenance.analyzer.HBaseTable.TYPE_HBASE_NAMESPACE;
import static org.apache.nifi.atlas.provenance.analyzer.HBaseTable.TYPE_HBASE_TABLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.when;

public class TestHBaseTable {

    private static final String PROCESSOR_NAME = "FetchHBaseRow";
    private static final String ATLAS_METADATA_NAMESPACE = "namespace1";

    @Test
    public void testHBaseTableImplicitDefaultNamespace() {
        final String transitUri = "hbase://0.example.com/tableA/rowB";
        final ProvenanceEventRecord record = mockProvenanceEventRecord(transitUri);

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn(ATLAS_METADATA_NAMESPACE);

        executeTest(record, namespaceResolvers, "tableA", "default:tableA@namespace1", "tableA", "default", "default@namespace1");
    }

    @Test
    public void testHBaseTableExplicitDefaultNamespace() {
        final String transitUri = "hbase://0.example.com/default:tableA/rowB";
        final ProvenanceEventRecord record = mockProvenanceEventRecord(transitUri);

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("namespace1");

        executeTest(record, namespaceResolvers, "tableA", "default:tableA@namespace1", "tableA", "default", "default@namespace1");
    }

    @Test
    public void testHBaseTableCustomNamespace() {
        final String transitUri = "hbase://0.example.com/namespaceA:tableA/rowB";
        final ProvenanceEventRecord record = mockProvenanceEventRecord(transitUri);

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("namespace1");

        executeTest(record, namespaceResolvers, "namespaceA:tableA", "namespaceA:tableA@namespace1", "namespaceA:tableA", "namespaceA", "namespaceA@namespace1");
    }

    @Test
    public void testHBaseTableWithMultipleZkHosts() {
        final String transitUri = "hbase://zk0.example.com,zk2.example.com,zk3.example.com/tableA/rowB";
        final ProvenanceEventRecord record = mockProvenanceEventRecord(transitUri);

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(
                matches("zk0.example.com"),
                matches("zk2.example.com"),
                matches("zk3.example.com"))).thenReturn("namespace1");

        executeTest(record, namespaceResolvers, "tableA", "default:tableA@namespace1", "tableA", "default", "default@namespace1");
    }

    private ProvenanceEventRecord mockProvenanceEventRecord(String transitUri) {
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);

        when(record.getEventType()).thenReturn(ProvenanceEventType.FETCH);
        when(record.getComponentType()).thenReturn(PROCESSOR_NAME);
        when(record.getTransitUri()).thenReturn(transitUri);

        return record;
    }

    private void executeTest(ProvenanceEventRecord record, NamespaceResolvers namespaceResolvers, String expectedTableName, String expectedTableQualifiedName, String expectedTableUri,
                             String expectedNamespaceName, String expectedNamespaceQualifiedName) {
        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(PROCESSOR_NAME, record.getTransitUri(), record.getEventType());

        final DataSetRefs refs = analyzer.analyze(context, record);

        assertAnalysisResult(refs, expectedTableName, expectedTableQualifiedName, expectedTableUri, expectedNamespaceName, expectedNamespaceQualifiedName);
    }

    private void assertAnalysisResult(DataSetRefs refs, String expectedTableName, String expectedTableQualifiedName, String expectedTableUri,
                                      String expectedNamespaceName, String expectedNamespaceQualifiedName) {
        assertEquals(1, refs.getInputs().size());
        assertEquals(0, refs.getOutputs().size());

        Referenceable tableRef = refs.getInputs().iterator().next();
        assertEquals(TYPE_HBASE_TABLE, tableRef.getTypeName());
        assertEquals(expectedTableName, tableRef.get(ATTR_NAME));
        assertEquals(expectedTableQualifiedName, tableRef.get(ATTR_QUALIFIED_NAME));
        assertEquals(expectedTableUri, tableRef.get(ATTR_URI));

        Referenceable namespaceRef = (Referenceable) tableRef.get(ATTR_NAMESPACE);
        assertEquals(TYPE_HBASE_NAMESPACE, namespaceRef.getTypeName());
        assertEquals(expectedNamespaceName, namespaceRef.get(ATTR_NAME));
        assertEquals(expectedNamespaceQualifiedName, namespaceRef.get(ATTR_QUALIFIED_NAME));
        assertEquals(ATLAS_METADATA_NAMESPACE, namespaceRef.get(ATTR_CLUSTER_NAME));
    }

}
