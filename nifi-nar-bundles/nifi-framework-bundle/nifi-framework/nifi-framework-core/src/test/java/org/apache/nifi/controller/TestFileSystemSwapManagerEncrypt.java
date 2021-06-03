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
import org.apache.nifi.controller.repository.claim.ResourceClaim;
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
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link FileSystemSwapManagerEncrypt}.
 */
public class TestFileSystemSwapManagerEncrypt {
    private static final Logger logger = Logger.getLogger(TestFileSystemSwapManagerEncrypt.class.getName());

    /**
     * Test a simple swap to disk / swap from disk operation.  Configured to use {@link StaticKeyProvider}.
     */
    @Test
    public void testConfig_StaticKeyProvider_SerdeSuccess() throws GeneralSecurityException, EncryptionException, IOException {
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
        for (int i = 0; (i < 10_000); ++i) {
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
        final FileSystemSwapManager swapManager = new FileSystemSwapManagerEncrypt(nifiProperties);
        final ResourceClaimManager resourceClaimManager = new NopResourceClaimManager();

        swapManager.initialize(new SwapManagerInitializationContext() {
            @Override
            public ResourceClaimManager getResourceClaimManager() {
                return resourceClaimManager;
            }

            @Override
            public FlowFileRepository getFlowFileRepository() {
                return flowFileRepo;
            }

            @Override
            public EventReporter getEventReporter() {
                return EventReporter.NO_OP;
            }
        });

        return swapManager;
    }

    /**
     * Borrowed from "nifi-framework-core/src/test/java/org/apache/nifi/controller/TestFileSystemSwapManager.java".
     */
    public static class NopResourceClaimManager implements ResourceClaimManager {
        @Override
        public ResourceClaim newResourceClaim(String container, String section, String id, boolean lossTolerant, boolean writable) {
            return null;
        }

        @Override
        public ResourceClaim getResourceClaim(String container, String section, String id) {
            return null;
        }

        @Override
        public int getClaimantCount(ResourceClaim claim) {
            return 0;
        }

        @Override
        public int decrementClaimantCount(ResourceClaim claim) {
            return 0;
        }

        @Override
        public int incrementClaimantCount(ResourceClaim claim) {
            return 0;
        }

        @Override
        public int incrementClaimantCount(ResourceClaim claim, boolean newClaim) {
            return 0;
        }

        @Override
        public void markDestructable(ResourceClaim claim) {
        }

        @Override
        public void drainDestructableClaims(Collection<ResourceClaim> destination, int maxElements) {
        }

        @Override
        public void drainDestructableClaims(Collection<ResourceClaim> destination, int maxElements, long timeout, TimeUnit unit) {
        }

        @Override
        public void purge() {
        }

        @Override
        public void freeze(ResourceClaim claim) {
        }

        @Override
        public boolean isDestructable(final ResourceClaim claim) {
            return false;
        }
    }
}
