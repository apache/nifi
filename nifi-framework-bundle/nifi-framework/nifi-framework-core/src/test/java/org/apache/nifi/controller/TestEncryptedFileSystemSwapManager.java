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

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapManagerInitializationContext;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link EncryptedFileSystemSwapManager}.
 */
public class TestEncryptedFileSystemSwapManager {
    private static final String KEYSTORE_CREDENTIALS = UUID.randomUUID().toString();

    private static final String KEYSTORE_NAME = "repository.p12";

    private static final String KEY_ID = "primary-key";

    private static final String KEYSTORE_TYPE = "PKCS12";

    private static final int KEY_LENGTH = 32;

    private static final String KEY_ALGORITHM = "AES";

    private static Path keyStorePath;

    @BeforeAll
    public static void setRepositoryKeystore(@TempDir final Path temporaryDirectory) throws GeneralSecurityException, IOException {
        keyStorePath = temporaryDirectory.resolve(KEYSTORE_NAME);

        final SecureRandom secureRandom = new SecureRandom();
        final byte[] key = new byte[KEY_LENGTH];
        secureRandom.nextBytes(key);
        final SecretKeySpec secretKeySpec = new SecretKeySpec(key, KEY_ALGORITHM);

        final KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
        keyStore.load(null);

        final KeyStore.SecretKeyEntry secretKeyEntry = new KeyStore.SecretKeyEntry(secretKeySpec);
        final KeyStore.PasswordProtection passwordProtection = new KeyStore.PasswordProtection(KEYSTORE_CREDENTIALS.toCharArray());
        keyStore.setEntry(KEY_ID, secretKeyEntry, passwordProtection);

        try (final OutputStream outputStream = Files.newOutputStream(keyStorePath)) {
            keyStore.store(outputStream, KEYSTORE_CREDENTIALS.toCharArray());
        }
    }

    /**
     * Test a simple swap to disk / swap from disk operation
     */
    @Test
    public void testSwapOutSwapIn() throws GeneralSecurityException, IOException {
        // use temp folder on filesystem to temporarily hold swap content (clean up after test)
        final File folderRepository = Files.createTempDirectory(getClass().getSimpleName()).toFile();
        folderRepository.deleteOnExit();
        new File(folderRepository, "swap").deleteOnExit();

        // configure a nifi properties for encrypted swap file
        final Map<String, String> properties = getEncryptionProperties();
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
        assertEquals(flowFiles.size(), flowFilesRecovered.size());
        assertTrue(flowFilesRecovered.containsAll(flowFiles));
        assertTrue(flowFiles.containsAll(flowFilesRecovered));
    }

    /**
     * Borrowed from "nifi-framework-core/src/test/java/org/apache/nifi/controller/TestFileSystemSwapManager.java".
     */
    private FlowFileSwapManager createSwapManager(final NiFiProperties nifiProperties) throws GeneralSecurityException {
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

    private Map<String, String> getEncryptionProperties() {
        final Map<String, String> encryptedRepoProperties = new HashMap<>();
        encryptedRepoProperties.put("nifi.repository.encryption.protocol.version", "1");
        encryptedRepoProperties.put("nifi.repository.encryption.key.id", KEY_ID);
        encryptedRepoProperties.put("nifi.repository.encryption.key.provider", "KEYSTORE");
        encryptedRepoProperties.put("nifi.repository.encryption.key.provider.keystore.location", keyStorePath.toString());
        encryptedRepoProperties.put("nifi.repository.encryption.key.provider.keystore.password", KEYSTORE_CREDENTIALS);
        return encryptedRepoProperties;
    }
}
