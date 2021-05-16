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
import org.junit.Before;
import org.junit.Test;
import java.util.concurrent.TimeUnit;

public class ITListAzureBlobStorage extends AbstractAzureBlobStorageIT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return ListAzureBlobStorage.class;
    }

    @Before
    public void setUp() throws Exception {
        uploadTestBlob();

        Thread.sleep(ListAzureBlobStorage.LISTING_LAG_MILLIS.get(TimeUnit.SECONDS) * 2);
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

    private void assertResult() {
        runner.assertTransferCount(ListAzureBlobStorage.REL_SUCCESS, 1);
        runner.assertAllFlowFilesTransferred(ListAzureBlobStorage.REL_SUCCESS, 1);

        for (MockFlowFile entry : runner.getFlowFilesForRelationship(ListAzureBlobStorage.REL_SUCCESS)) {
            entry.assertAttributeEquals("azure.length", "36");
            entry.assertAttributeEquals("mime.type", "application/octet-stream");
        }
    }
}
