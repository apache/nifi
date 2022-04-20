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
package org.apache.nifi.processors.azure.storage;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ITFetchAzureBlobStorage_v12 extends AbstractAzureBlobStorage_v12IT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return FetchAzureBlobStorage_v12.class;
    }

    @BeforeEach
    public void setUp() {
        runner.setProperty(DeleteAzureBlobStorage_v12.BLOB_NAME, BLOB_NAME);
    }

    @Test
    public void testFetchBlobWithSimpleName() throws Exception {
        uploadBlob(BLOB_NAME, BLOB_DATA);

        runProcessor();

        assertSuccess(BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testFetchBlobWithSimpleNameUsingProxyConfigurationService() throws Exception {
        uploadBlob(BLOB_NAME, BLOB_DATA);

        configureProxyService();

        runProcessor();

        assertSuccess(BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testFetchBlobWithCompoundName() throws Exception {
        String blobName = "dir1/dir2/blob1";
        runner.setProperty(DeleteAzureBlobStorage_v12.BLOB_NAME, blobName);
        uploadBlob(blobName, BLOB_DATA);

        runProcessor();

        assertSuccess(blobName, BLOB_DATA);
    }

    @Test
    public void testFetchNonExistingBlob() throws Exception {
        runProcessor();

        assertFailure();
    }

    @Test
    public void testFetchBlobWithSpacesInBlobName() throws Exception {
        String blobName = "dir 1/blob 1";
        runner.setProperty(FetchAzureBlobStorage_v12.BLOB_NAME, blobName);
        uploadBlob(blobName, BLOB_DATA);

        runProcessor();

        assertSuccess(blobName, BLOB_DATA);
    }

    @Test
    public void testFetchEmptyBlob() throws Exception {
        uploadBlob(BLOB_NAME, EMPTY_CONTENT);

        runProcessor();

        assertSuccess(BLOB_NAME, EMPTY_CONTENT);
    }

    @Test
    public void testFetchBigBlob() throws Exception {
        Random random = new Random();
        byte[] blobData = new byte[120_000_000];
        random.nextBytes(blobData);

        uploadBlob(BLOB_NAME, blobData);

        runProcessor();

        assertSuccess(BLOB_NAME, blobData);
    }

    @Test
    public void testFetchBlobUsingExpressionLanguage() throws Exception {
        uploadBlob(BLOB_NAME, BLOB_DATA);

        Map<String, String> attributes = initCommonExpressionLanguageAttributes();

        runProcessor(attributes);

        assertSuccess(BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testFetchBlobRangeWithStart() throws Exception {
        int start = 3;
        runner.setProperty(FetchAzureBlobStorage_v12.RANGE_START, String.format("%d B", start));

        uploadBlob(BLOB_NAME, BLOB_DATA);

        runProcessor();

        assertSuccess(BLOB_NAME, Arrays.copyOfRange(BLOB_DATA, start, BLOB_DATA.length), BLOB_DATA.length);
    }

    @Test
    public void testFetchBlobRangeWithStartAndLength() throws Exception {
        int start = 3;
        int length = 2;
        runner.setProperty(FetchAzureBlobStorage_v12.RANGE_START, String.format("%d B", start));
        runner.setProperty(FetchAzureBlobStorage_v12.RANGE_LENGTH, String.format("%d B", length));

        uploadBlob(BLOB_NAME, BLOB_DATA);

        runProcessor();

        assertSuccess(BLOB_NAME, Arrays.copyOfRange(BLOB_DATA, start, start + length), BLOB_DATA.length);
    }

    @Test
    public void testFetchBlobRangeWithStartAndOverSizeLength() throws Exception {
        int start = 3;
        int length = 10_000;
        runner.setProperty(FetchAzureBlobStorage_v12.RANGE_START, String.format("%d B", start));
        runner.setProperty(FetchAzureBlobStorage_v12.RANGE_LENGTH, String.format("%d B", length));

        uploadBlob(BLOB_NAME, BLOB_DATA);

        runProcessor();

        assertSuccess(BLOB_NAME, Arrays.copyOfRange(BLOB_DATA, start, BLOB_DATA.length), BLOB_DATA.length);
    }

    private void runProcessor() {
        runProcessor(Collections.emptyMap());
    }

    private void runProcessor(Map<String, String> attributes) {
        runner.assertValid();
        runner.enqueue(EMPTY_CONTENT, attributes);
        runner.run();
    }

    private void assertSuccess(String blobName, byte[] blobData) throws Exception {
        assertFlowFile(blobName, blobData, blobData.length);
        assertProvenanceEvents();
    }

    private void assertSuccess(String blobName, byte[] blobData, int originalLength) throws Exception {
        assertFlowFile(blobName, blobData, originalLength);
        assertProvenanceEvents();
    }

    private void assertFlowFile(String blobName, byte[] blobData, int originalLength) throws Exception {
        runner.assertAllFlowFilesTransferred(FetchAzureBlobStorage_v12.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchAzureBlobStorage_v12.REL_SUCCESS).get(0);

        assertFlowFileBlobAttributes(flowFile, getContainerName(), blobName, originalLength);

        flowFile.assertContentEquals(blobData);
    }

    private void assertProvenanceEvents() {
        Set<ProvenanceEventType> expectedEventTypes = Collections.singleton(ProvenanceEventType.FETCH);

        Set<ProvenanceEventType> actualEventTypes = runner.getProvenanceEvents().stream()
                .map(ProvenanceEventRecord::getEventType)
                .collect(Collectors.toSet());
        assertEquals(expectedEventTypes, actualEventTypes);
    }

    private void assertFailure() throws Exception {
        runner.assertAllFlowFilesTransferred(FetchAzureBlobStorage_v12.REL_FAILURE, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchAzureBlobStorage_v12.REL_FAILURE).get(0);
        flowFile.assertContentEquals(EMPTY_CONTENT);
    }
}
