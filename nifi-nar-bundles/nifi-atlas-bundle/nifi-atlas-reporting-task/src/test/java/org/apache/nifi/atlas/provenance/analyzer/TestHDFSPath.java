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
import org.apache.nifi.atlas.provenance.FilesystemPathsLevel;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.atlas.resolver.NamespaceResolvers;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.when;

public class TestHDFSPath {

    @Test
    public void testHDFSPathWithFileLevel() {
        // TODO: what if with HA namenode?
        final String transitUri = "hdfs://0.example.com:8020/user/nifi/fileA";
        final FilesystemPathsLevel filesystemPathsLevel = FilesystemPathsLevel.FILE;
        final String expectedPath = "/user/nifi/fileA";
        testHDFSPath(transitUri, filesystemPathsLevel, expectedPath);
    }

    @Test
    public void testHDFSPathWithDirectoryLevel() {
        final String transitUri = "hdfs://0.example.com:8020/user/nifi/fileA";
        final FilesystemPathsLevel filesystemPathsLevel = FilesystemPathsLevel.DIRECTORY;
        final String expectedPath = "/user/nifi";
        testHDFSPath(transitUri, filesystemPathsLevel, expectedPath);
    }

    @Test
    public void testHDFSPathRootDirWithFileLevel() {
        final String transitUri = "hdfs://0.example.com:8020/fileA";
        final FilesystemPathsLevel filesystemPathsLevel = FilesystemPathsLevel.FILE;
        final String expectedPath = "/fileA";
        testHDFSPath(transitUri, filesystemPathsLevel, expectedPath);
    }

    @Test
    public void testHDFSPathRootDirWithDirectoryLevel() {
        final String transitUri = "hdfs://0.example.com:8020/fileA";
        final FilesystemPathsLevel filesystemPathsLevel = FilesystemPathsLevel.DIRECTORY;
        final String expectedPath = "/";
        testHDFSPath(transitUri, filesystemPathsLevel, expectedPath);
    }

    private void testHDFSPath(String transitUri, FilesystemPathsLevel filesystemPathsLevel, String expectedPath) {
        final String processorName = "PutHDFS";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("namespace1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);
        when(context.getFilesystemPathsLevel()).thenReturn(filesystemPathsLevel);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);
        assertEquals(HDFSPath.class, analyzer.getClass());

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("hdfs_path", ref.getTypeName());
        assertEquals(expectedPath, ref.get(ATTR_NAME));
        assertEquals(expectedPath + "@namespace1", ref.get(ATTR_QUALIFIED_NAME));
    }
}
