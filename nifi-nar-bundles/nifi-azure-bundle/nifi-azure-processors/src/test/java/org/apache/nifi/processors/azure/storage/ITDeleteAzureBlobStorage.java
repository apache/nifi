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

import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.nifi.processor.Processor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class ITDeleteAzureBlobStorage extends AbstractAzureBlobStorageIT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return DeleteAzureBlobStorage.class;
    }

    @Before
    public void setUp() throws Exception {
        runner.setProperty(DeleteAzureBlobStorage.BLOB, TEST_BLOB_NAME);

        uploadTestBlob();
    }

    @Test
    public void testDeleteBlob() {
        runner.assertValid();
        runner.enqueue(new byte[0]);
        runner.run(1);

        assertResult();
    }

    @Test
    public void testDeleteBlobUsingCredentialsService() throws Exception {
        configureCredentialsService();

        runner.assertValid();
        runner.enqueue(new byte[0]);
        runner.run(1);

        assertResult();
    }

    private void assertResult() {
        runner.assertAllFlowFilesTransferred(DeleteAzureBlobStorage.REL_SUCCESS);

        Iterable<ListBlobItem> blobs = container.listBlobs(TEST_BLOB_NAME);
        assertFalse(blobs.iterator().hasNext());
    }
}
