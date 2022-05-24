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
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ITListAzureBlobStorage extends AbstractAzureBlobStorageIT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return ListAzureBlobStorage.class;
    }

    @BeforeEach
    public void setUp() throws Exception {
        uploadTestBlob();
        waitForUpload();
    }

    @Test
    public void testListBlobs() throws Exception {
        runner.assertValid();
        runner.run(1);

        assertResult();
    }

    @Test
    public void testListBlobsUsingCredentialService() throws Exception {
        configureCredentialsService();

        runner.assertValid();
        runner.run(1);

        assertResult();
    }

    @Test
    public void testListWithMinAge() throws Exception {
        runner.setProperty(ListAzureBlobStorage.MIN_AGE, "1 hour");

        runner.assertValid();
        runner.run(1);

        runner.assertTransferCount(ListAzureBlobStorage.REL_SUCCESS, 0);
    }

    @Test
    public void testListWithMaxAge() throws Exception {
        runner.setProperty(ListAzureBlobStorage.MAX_AGE, "1 hour");

        runner.assertValid();
        runner.run(1);

        assertResult(TEST_FILE_CONTENT);
    }

    @Test
    public void testListWithMinSize() throws Exception {
        uploadTestBlob("nifi-test-blob2", "Test");
        waitForUpload();
        assertListCount();
        runner.setProperty(ListAzureBlobStorage.MIN_SIZE, "5 B");

        runner.assertValid();
        runner.run(1);

        assertResult(TEST_FILE_CONTENT);
    }

    @Test
    public void testListWithMaxSize() throws Exception {
        uploadTestBlob("nifi-test-blob2", "Test");
        waitForUpload();
        assertListCount();
        runner.setProperty(ListAzureBlobStorage.MAX_SIZE, "5 B");

        runner.assertValid();
        runner.run(1);

        assertResult("Test");
    }

    private void waitForUpload() throws InterruptedException {
        Thread.sleep(ListAzureBlobStorage.LISTING_LAG_MILLIS.get(TimeUnit.SECONDS) * 2);
    }

    private void assertResult() {
        assertResult(TEST_FILE_CONTENT);
    }

    private void assertResult(final String content) {
        runner.assertTransferCount(ListAzureBlobStorage.REL_SUCCESS, 1);
        runner.assertAllFlowFilesTransferred(ListAzureBlobStorage.REL_SUCCESS, 1);

        for (MockFlowFile entry : runner.getFlowFilesForRelationship(ListAzureBlobStorage.REL_SUCCESS)) {
            entry.assertAttributeEquals("azure.length", String.valueOf(content.getBytes(StandardCharsets.UTF_8).length));
            entry.assertAttributeEquals("mime.type", "application/octet-stream");
        }
    }

    private void assertListCount() {
        final long listCount = StreamSupport.stream(container.listBlobs().spliterator(), false).count();
        assertEquals(2, listCount, "There should be 2 uploaded files but found only " + listCount);
    }
}
