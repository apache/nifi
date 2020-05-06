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
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.standard.LookupAttribute;
import org.apache.nifi.services.azure.storage.AzureBlobStorageUserMetadataStringLookupService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ITLookupAttributeAzureBlobStorage extends AbstractAzureBlobStorageIT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return LookupAttribute.class;
    }

    @Before
    public void setup() throws Exception {
        // We override the runner created by the Abstract super class, we don't need the connections strings on the Processor
        runner = TestRunners.newTestRunner(getProcessorClass());

        AzureBlobStorageUserMetadataStringLookupService lookupService = new AzureBlobStorageUserMetadataStringLookupService();

        runner.addControllerService("azure-blob-string-lookup", lookupService);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "azure-blob-string-lookup");
        runner.setProperty(lookupService, AzureStorageUtils.CONTAINER, containerName);
        runner.setProperty(lookupService, AzureStorageUtils.ACCOUNT_NAME, getAccountName());
        runner.setProperty(lookupService, AzureStorageUtils.ACCOUNT_KEY, getAccountKey());
        runner.setProperty(LookupAttribute.INCLUDE_EMPTY_VALUES, "false");
        runner.setProperty("lookup_key", "${key}");

        runner.enableControllerService(lookupService);

        uploadTestBlob();
    }

    @Test
    public void testLookupBlob() throws Exception {
        runner.assertValid();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("azure.blobname", TEST_BLOB_NAME);
        attributes.put("key", TEST_USER_METADATA_KEY);
        runner.enqueue("0123456789", attributes);
        attributes = new HashMap<>();
        attributes.put("azure.blobname", TEST_BLOB_NAME);
        attributes.put("key", "dontexists");
        runner.enqueue("0123456789", attributes);
        attributes = new HashMap<>();
        attributes.put("azure.blobname", "dontexists");
        attributes.put("key", "dontexists");
        runner.enqueue("0123456789", attributes);
        runner.run();

        assertResult();
    }

    private void assertResult() throws Exception {
        runner.assertTransferCount(LookupAttribute.REL_MATCHED, 1);
        runner.assertTransferCount(LookupAttribute.REL_UNMATCHED, 2);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(LookupAttribute.REL_MATCHED);
        for (MockFlowFile flowFile : flowFilesForRelationship) {
            flowFile.assertContentEquals("0123456789".getBytes());
            flowFile.assertAttributeEquals("lookup_key", TEST_USER_METADATA_VALUE);
        }
    }

}
