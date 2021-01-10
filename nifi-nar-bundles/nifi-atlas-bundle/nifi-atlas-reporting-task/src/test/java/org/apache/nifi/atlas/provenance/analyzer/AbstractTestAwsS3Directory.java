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

import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.atlas.resolver.NamespaceResolver;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public abstract class AbstractTestAwsS3Directory {

    protected static final ProvenanceEventType PROVENANCE_EVENT_TYPE = ProvenanceEventType.SEND;
    protected static final String ATLAS_NAMESPACE = "namespace1";
    protected static final String AWS_BUCKET = "bucket1";
    protected static final String AWS_FILENAME = "file1";

    protected abstract String getAwsS3ModelVersion();

    protected abstract void assertAnalysisResult(DataSetRefs refs, String directory);

    protected void executeTest(String processorName, String directory) {
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
        when(analysisContext.getAwsS3ModelVersion()).thenReturn(getAwsS3ModelVersion());

        return analysisContext;
    }

    private void assertAnalyzer(NiFiProvenanceEventAnalyzer analyzer) {
        assertNotNull(analyzer);
        assertEquals(AwsS3Directory.class, analyzer.getClass());
    }
}
