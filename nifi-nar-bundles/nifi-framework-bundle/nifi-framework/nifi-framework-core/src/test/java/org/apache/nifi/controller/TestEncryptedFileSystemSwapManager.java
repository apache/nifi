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
package org.apache.nifi.controller;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapManagerInitializationContext;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.security.kms.EncryptionException;
import org.apache.nifi.security.kms.StaticKeyProvider;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link EncryptedFileSystemSwapManager}.
 */
public class TestEncryptedFileSystemSwapManager {
    private static final Logger logger = Logger.getLogger(TestEncryptedFileSystemSwapManager.class.getName());

    /**
     * Test a simple swap to disk / swap from disk operation.  Configured to use {@link StaticKeyProvider}.
     */
    @Test
    public void testSwapOutSwapIn() throws GeneralSecurityException, EncryptionException, IOException {
        // use temp folder on filesystem to temporarily hold swap content (clean up after test)
        final File folderRepository = Files.createTempDirectory(getClass().getSimpleName()).toFile();
        logger.info(folderRepository.getPath());
        folderRepository.deleteOnExit();
        new File(folderRepository, "swap").deleteOnExit();

        // configure a nifi properties for encrypted swap file
        final Properties properties = new Properties();
        properties.put(NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS, StaticKeyProvider.class.getName());
        properties.put(NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_ID, NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY);
        properties.put(NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY, StringUtils.repeat("00", 32));
        properties.put(NiFiProperties.FLOWFILE_REPOSITORY_DIRECTORY, folderRepository.getPath());
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);

        // generate some flow file content to swap to disk
        final List<FlowFileRecord> flowFiles = new ArrayList<>();
        for (int i = 0; (i < 100); ++i) {
            flowFiles.add(new StandardFlowFileRecord.Builder().id(i).build());
        }

        // setup for test case
        final FlowFileSwapManager swapManager = createSwapManager(nifiProperties);
        final String queueIdentifier = UUID.randomUUID().toString();
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        when(flowFileQueue.getIdentifier()).thenReturn(queueIdentifier);

        // swap out to disk; pull content back from disk
        final String swapPath = swapManager.swapOut(flowFiles, flowFileQueue, "partition-1");
        final SwapContents swapContents = swapManager.swapIn(swapPath, flowFileQueue);

        // verify recovery of original content
        final List<FlowFileRecord> flowFilesRecovered = swapContents.getFlowFiles();
        Assert.assertEquals(flowFiles.size(), flowFilesRecovered.size());
        Assert.assertTrue(flowFilesRecovered.containsAll(flowFiles));
        Assert.assertTrue(flowFiles.containsAll(flowFilesRecovered));
    }

    /**
     * Borrowed from "nifi-framework-core/src/test/java/org/apache/nifi/controller/TestFileSystemSwapManager.java".
     */
    private FlowFileSwapManager createSwapManager(NiFiProperties nifiProperties)
            throws IOException, GeneralSecurityException, EncryptionException {
        final FlowFileRepository flowFileRepo = Mockito.mock(FlowFileRepository.class);
        when(flowFileRepo.isValidSwapLocationSuffix(any())).thenReturn(true);

        final FileSystemSwapManager swapManager = new EncryptedFileSystemSwapManager(nifiProperties);
        final ResourceClaimManager resourceClaimManager = Mockito.mock(ResourceClaimManager.class);
        final SwapManagerInitializationContext context = Mockito.mock(SwapManagerInitializationContext.class);
        when(context.getResourceClaimManager()).thenReturn(resourceClaimManager);
        when(context.getFlowFileRepository()).thenReturn(flowFileRepo);
        when(context.getEventReporter()).thenReturn(EventReporter.NO_OP);
        swapManager.initialize(context);

        return swapManager;
    }
}
