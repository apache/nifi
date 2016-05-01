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

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processors.azure.AzureConstants;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import com.microsoft.azure.storage.StorageException;

public class ITFetchAzureBlobStorage extends AbstractAzureIT {

    @Test
    public void testFetchingBlob() throws InvalidKeyException, URISyntaxException, StorageException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(new FetchAzureBlobStorage());

        runner.setValidateExpressionUsage(true);

        runner.setProperty(AzureConstants.ACCOUNT_NAME, getAccountName());
        runner.setProperty(AzureConstants.ACCOUNT_KEY, getAccountKey());
        runner.setProperty(AzureConstants.CONTAINER, TEST_CONTAINER_NAME);
        runner.setProperty(FetchAzureBlobStorage.BLOB, "${azure.blobname}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("azure.primaryUri", "http://" + getAccountName() + ".blob.core.windows.net/" + TEST_CONTAINER_NAME + "/" + TEST_BLOB_NAME);
        attributes.put("azure.blobname", TEST_BLOB_NAME);
        attributes.put("azure.blobtype", AzureConstants.BLOCK);
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchAzureBlobStorage.REL_SUCCESS, 1);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(FetchAzureBlobStorage.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFilesForRelationship) {
            flowFile.assertContentEquals("0123456789".getBytes());
            flowFile.assertAttributeEquals("azure.length", "10");
        }
    }
}
