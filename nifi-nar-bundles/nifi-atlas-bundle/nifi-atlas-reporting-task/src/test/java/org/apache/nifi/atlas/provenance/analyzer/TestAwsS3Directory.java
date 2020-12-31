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
import org.apache.nifi.atlas.resolver.NamespaceResolver;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestAwsS3Directory {

    private static final ProvenanceEventType PROVENANCE_EVENT_TYPE = ProvenanceEventType.SEND;
    private static final String ATLAS_NAMESPACE = "namespace1";
    private static final String AWS_BUCKET = "bucket1";
    private static final String AWS_FILENAME = "file1";

    @Test
    public void testSimpleDirectory() {
        String processorName = "PutHDFS";
        String directory = "/dir1";

        executeTest(processorName, directory);
    }

    @Test
    public void testCompoundDirectory() {
        String processorName = "PutHDFS";
        String directory = "/dir1/dir2/dir3/dir4/dir5";

        executeTest(processorName, directory);
    }

    @Test
    public void testRootDirectory() {
        String processorName = "PutHDFS";
        String directory = "/";

        executeTest(processorName, directory);
    }

    @Test
    public void testWithPutORC() {
        String processorName = "PutORC";
        String directory = "/dir1";

        executeTest(processorName, directory);
    }

    public void executeTest(String processorName, String directory) {
        String transitUri = createTransitUri(directory);

        ProvenanceEventRecord provenanceEvent = mockProvenanceEvent(processorName, transitUri);
        AnalysisContext analysisContext = mockAnalysisContext();

        NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, PROVENANCE_EVENT_TYPE);
        assertAnalyzer(analyzer);

        DataSetRefs refs = analyzer.analyze(analysisContext, provenanceEvent);
        assertAnalysisResult(refs, directory);
    }

    private String createTransitUri(String directory) {
        if (directory.equals("/")) {
            return String.format("s3a://%s/%s", AWS_BUCKET, AWS_FILENAME);
        } else {
            return String.format("s3a://%s%s/%s", AWS_BUCKET, directory, AWS_FILENAME);
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
        assertNotNull(analyzer);
        assertEquals(AwsS3Directory.class, analyzer.getClass());
    }

    private void assertAnalysisResult(DataSetRefs refs, String directory) {
        String expectedDirectoryQualifiedName = String.format("s3a://%s%s@%s", AWS_BUCKET, directory, ATLAS_NAMESPACE);
        String expectedBucketQualifiedName = String.format("s3a://%s@%s", AWS_BUCKET, ATLAS_NAMESPACE);

        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());

        Referenceable directoryRef = refs.getOutputs().iterator().next();

        assertEquals("aws_s3_pseudo_dir", directoryRef.getTypeName());
        assertEquals(expectedDirectoryQualifiedName, directoryRef.get(ATTR_QUALIFIED_NAME));
        assertEquals(directory, directoryRef.get(ATTR_NAME));
        assertEquals(directory, directoryRef.get("objectPrefix"));

        Referenceable bucketRef = (Referenceable) directoryRef.get("bucket");
        assertNotNull(bucketRef);
        assertEquals("aws_s3_bucket", bucketRef.getTypeName());
        assertEquals(expectedBucketQualifiedName, bucketRef.get(ATTR_QUALIFIED_NAME));
        assertEquals(AWS_BUCKET, bucketRef.get(ATTR_NAME));
    }
}
