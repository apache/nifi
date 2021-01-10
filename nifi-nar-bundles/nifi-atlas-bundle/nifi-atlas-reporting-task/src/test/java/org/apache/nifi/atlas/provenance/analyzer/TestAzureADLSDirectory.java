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
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.atlas.resolver.NamespaceResolver;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.provenance.analyzer.AzureADLSDirectory.ATTR_ACCOUNT;
import static org.apache.nifi.atlas.provenance.analyzer.AzureADLSDirectory.ATTR_PARENT;
import static org.apache.nifi.atlas.provenance.analyzer.AzureADLSDirectory.TYPE_ACCOUNT;
import static org.apache.nifi.atlas.provenance.analyzer.AzureADLSDirectory.TYPE_CONTAINER;
import static org.apache.nifi.atlas.provenance.analyzer.AzureADLSDirectory.TYPE_DIRECTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestAzureADLSDirectory {

    private static final ProvenanceEventType PROVENANCE_EVENT_TYPE = ProvenanceEventType.SEND;
    private static final String ATLAS_NAMESPACE = "namespace1";
    private static final String ADLS_ACCOUNT = "account1";
    private static final String ADLS_FILESYSTEM = "filesystem1";
    private static final String ADLS_FILENAME = "file1";

    @Test
    public void testSimpleDirectory() {
        String processorName = "PutHDFS";
        String path = "/dir1";

        executeTest(processorName, path);
    }

    @Test
    public void testCompoundDirectory() {
        String processorName = "PutHDFS";
        String path = "/dir1/dir2/dir3/dir4/dir5";

        executeTest(processorName, path);
    }

    @Test
    public void testRootDirectory() {
        String processorName = "PutHDFS";
        String path = "";

        executeTest(processorName, path);
    }

    @Test
    public void testWithPutORC() {
        String processorName = "PutORC";
        String path = "/dir1";

        executeTest(processorName, path);
    }

    private void executeTest(String processorName, String path) {
        String transitUri = String.format("abfs://%s@%s.dfs.core.windows.net%s/%s", ADLS_FILESYSTEM, ADLS_ACCOUNT, path, ADLS_FILENAME);

        ProvenanceEventRecord provenanceEvent = mockProvenanceEvent(processorName, transitUri);
        AnalysisContext analysisContext = mockAnalysisContext();

        NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, PROVENANCE_EVENT_TYPE);
        assertAnalyzer(analyzer);

        DataSetRefs refs = analyzer.analyze(analysisContext, provenanceEvent);
        assertAnalysisResult(refs, path);
    }

    private ProvenanceEventRecord mockProvenanceEvent(String processorName, String transitUri) {
        ProvenanceEventRecord provenanceEvent = Mockito.mock(ProvenanceEventRecord.class);

        when(provenanceEvent.getComponentType()).thenReturn(processorName);
        when(provenanceEvent.getTransitUri()).thenReturn(transitUri);
        when(provenanceEvent.getEventType()).thenReturn(PROVENANCE_EVENT_TYPE);

        return provenanceEvent;
    }

    private AnalysisContext mockAnalysisContext() {
        NamespaceResolver namespaceResolver = Mockito.mock(NamespaceResolver.class);
        when(namespaceResolver.fromHostNames(any())).thenReturn(ATLAS_NAMESPACE);

        AnalysisContext analysisContext = Mockito.mock(AnalysisContext.class);
        when(analysisContext.getNamespaceResolver()).thenReturn(namespaceResolver);

        return analysisContext;
    }

    private void assertAnalyzer(NiFiProvenanceEventAnalyzer analyzer) {
        assertNotNull(analyzer);
        assertEquals(AzureADLSDirectory.class, analyzer.getClass());
    }

    private void assertAnalysisResult(DataSetRefs refs, String path) {
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());

        Referenceable ref = refs.getOutputs().iterator().next();

        String actualPath = path;
        while (StringUtils.isNotEmpty(actualPath)) {
            String directory = StringUtils.substringAfterLast(actualPath, "/");

            assertEquals(TYPE_DIRECTORY, ref.getTypeName());
            assertEquals(String.format("abfs://%s@%s%s/@%s", ADLS_FILESYSTEM, ADLS_ACCOUNT, actualPath, ATLAS_NAMESPACE), ref.get(ATTR_QUALIFIED_NAME));
            assertEquals(directory, ref.get(ATTR_NAME));
            assertNotNull(ref.get(ATTR_PARENT));

            ref = (Referenceable) ref.get(ATTR_PARENT);
            actualPath = StringUtils.substringBeforeLast(actualPath, "/");
        }

        assertEquals(TYPE_CONTAINER, ref.getTypeName());
        assertEquals(String.format("abfs://%s@%s@%s", ADLS_FILESYSTEM, ADLS_ACCOUNT, ATLAS_NAMESPACE), ref.get(ATTR_QUALIFIED_NAME));
        assertEquals(ADLS_FILESYSTEM, ref.get(ATTR_NAME));
        assertNotNull(ref.get(ATTR_ACCOUNT));

        ref = (Referenceable) ref.get(ATTR_ACCOUNT);

        assertEquals(TYPE_ACCOUNT, ref.getTypeName());
        assertEquals(String.format("abfs://%s@%s", ADLS_ACCOUNT, ATLAS_NAMESPACE), ref.get(ATTR_QUALIFIED_NAME));
        assertEquals(ADLS_ACCOUNT, ref.get(ATTR_NAME));
    }
}
