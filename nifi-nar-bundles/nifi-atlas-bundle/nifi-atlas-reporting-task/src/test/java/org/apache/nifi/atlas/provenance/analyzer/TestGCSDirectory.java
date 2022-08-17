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

import org.apache.atlas.utils.AtlasPathExtractorUtil;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.atlas.resolver.NamespaceResolver;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.provenance.analyzer.GCSDirectory.GCP_STORAGE_VIRTUAL_DIRECTORY;
import static org.apache.nifi.atlas.provenance.analyzer.GCSDirectory.REL_PARENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestGCSDirectory {

    private static final ProvenanceEventType PROVENANCE_EVENT_TYPE = ProvenanceEventType.SEND;
    private static final String ATLAS_NAMESPACE = "namespace1";
    private static final String GCS_BUCKET = "bucket1";
    private static final String GCS_FILENAME = "file1";
    private static final String GCS_QUALIFIED_NAME_FORMAT = "gs://%s%s/@%s";
    private static final String GCS_ROOT_QUALIFIED_NAME_FORMAT = "gs://%s@%s";
    private static final String GCS_TRANSIT_URI_FORMAT = "gs://%s%s/%s";
    private static final String GCS_ROOT_TRANSIT_URI_FORMAT = "gs://%s/%s";


    @Test
    public void testSimpleDirectory() {
        String processorName = "PutHDFS";
        String dirPath = "/dir1";
        String expectedDirectoryQualifiedName = String.format(GCS_QUALIFIED_NAME_FORMAT, GCS_BUCKET, dirPath, ATLAS_NAMESPACE);
        String transitUri = String.format(GCS_TRANSIT_URI_FORMAT, GCS_BUCKET, dirPath, GCS_FILENAME);

        executeTest(processorName, transitUri, "dir1", "/", GCS_BUCKET,
                GCP_STORAGE_VIRTUAL_DIRECTORY, expectedDirectoryQualifiedName, AtlasPathExtractorUtil.GCS_BUCKET);
    }

    @Test
    public void testCompoundDirectory() {
        String processorName = "PutHDFS";
        String dirPath = "/dir1/dir2/dir3/dir4/dir5";
        String expectedDirectoryQualifiedName = String.format(GCS_QUALIFIED_NAME_FORMAT, GCS_BUCKET, dirPath, ATLAS_NAMESPACE);
        String transitUri = String.format(GCS_TRANSIT_URI_FORMAT, GCS_BUCKET, dirPath, GCS_FILENAME);

        executeTest(processorName, transitUri, "dir5", "/dir1/dir2/dir3/dir4/", "dir4",
                GCP_STORAGE_VIRTUAL_DIRECTORY, expectedDirectoryQualifiedName, AtlasPathExtractorUtil.GCS_VIRTUAL_DIR);
    }

    @Test
    public void testRootDirectory() {
        String processorName = "PutHDFS";
        String expectedDirectoryQualifiedName = String.format(GCS_ROOT_QUALIFIED_NAME_FORMAT, GCS_BUCKET, ATLAS_NAMESPACE);
        String transitUri = String.format(GCS_ROOT_TRANSIT_URI_FORMAT, GCS_BUCKET, GCS_FILENAME);

        executeTest(processorName, transitUri, GCS_BUCKET, null, "/",
                "gcp_storage_bucket", expectedDirectoryQualifiedName, AtlasPathExtractorUtil.GCS_BUCKET);
    }

    protected void executeTest(String processorName, String transitUri, String lastDirName, String parentPath, String parentName,
                               String directoryType, String expectedDirectoryQualifiedName, String parentType) {

        ProvenanceEventRecord provenanceEvent = mockProvenanceEvent(processorName, transitUri);
        AnalysisContext analysisContext = mockAnalysisContext();

        NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, PROVENANCE_EVENT_TYPE);
        assertAnalyzer(analyzer);

        DataSetRefs refs = analyzer.analyze(analysisContext, provenanceEvent);
        assertAnalysisResult(refs, lastDirName, parentPath, parentName, directoryType, expectedDirectoryQualifiedName, parentType);
    }

    protected void assertAnalysisResult(DataSetRefs refs, String lastDirName, String parentPath, String parentName,
                                        String directoryType, String expectedDirectoryQualifiedName, String parentType) {


        Assertions.assertEquals(0, refs.getInputs().size());
        Assertions.assertEquals(1, refs.getOutputs().size());

        Referenceable directoryRef = refs.getOutputs().iterator().next();

        Assertions.assertEquals(directoryType, directoryRef.getTypeName());
        Assertions.assertEquals(expectedDirectoryQualifiedName, directoryRef.get(ATTR_QUALIFIED_NAME));
        Assertions.assertEquals(lastDirName, directoryRef.get(ATTR_NAME));
        Assertions.assertEquals(parentPath, directoryRef.get(AtlasPathExtractorUtil.ATTRIBUTE_OBJECT_PREFIX));

        if (parentPath != null) {
            Referenceable bucketRef = (Referenceable) directoryRef.get(REL_PARENT);
            Assertions.assertNotNull(bucketRef);
            Assertions.assertEquals(parentType, bucketRef.getTypeName());
            Assertions.assertEquals(parentName, bucketRef.get(ATTR_NAME));
        }
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
        Assertions.assertNotNull(analyzer);
        Assertions.assertEquals(GCSDirectory.class, analyzer.getClass());
    }
}
