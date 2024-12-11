/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.tests.system.nar;

import org.apache.nifi.nar.NarState;
import org.apache.nifi.tests.system.ExceptionalBooleanSupplier;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class NarUploadUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(NarUploadUtil.class);

    private final NiFiClient client;

    public NarUploadUtil(final NiFiClient client) {
        this.client = client;
    }

    public NarSummariesEntity verifyNarSummaries(final int expectedCount) throws NiFiClientException, IOException {
        final NarSummariesEntity narSummariesEntity = client.getControllerClient().getNarSummaries();
        assertNotNull(narSummariesEntity);
        assertNotNull(narSummariesEntity.getNarSummaries());
        assertNotNull(narSummariesEntity.getCurrentTime());
        assertEquals(expectedCount, narSummariesEntity.getNarSummaries().size());
        return narSummariesEntity;
    }

    public NarSummaryDTO uploadNar(final File narsLocation, final String narId) throws IOException, NiFiClientException {
        final File narFile = Arrays.stream(Objects.requireNonNull(narsLocation.listFiles(file -> file.getName().startsWith(narId) && file.getName().endsWith(".nar"))))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Unable to find NAR with id " + narId + " in " + narsLocation.getAbsolutePath()));
        assertNotNull(narFile);
        return uploadNar(narFile);
    }

    public NarSummaryDTO uploadNar(final File narFile) throws IOException, NiFiClientException {
        try (final InputStream inputStream = new FileInputStream(narFile)) {
            final NarSummaryEntity narSummaryEntity = client.getControllerClient().uploadNar(narFile.getName(), inputStream);
            assertNotNull(narSummaryEntity);

            final NarSummaryDTO narSummaryDTO = narSummaryEntity.getNarSummary();
            assertNotNull(narSummaryDTO);
            assertNotNull(narSummaryDTO.getIdentifier());
            assertNotNull(narSummaryDTO.getCoordinate());
            assertNotNull(narSummaryDTO.getState());
            return narSummaryEntity.getNarSummary();
        }
    }

    public ExceptionalBooleanSupplier getWaitForNarStateSupplier(final String identifier, final NarState narState) throws InterruptedException {
        return () -> {
            LOGGER.info("Waiting for NAR [{}] to be in state [{}]]", identifier, narState.getValue());
            final NarSummaryEntity narSummaryEntity = client.getControllerClient().getNarSummary(identifier);
            return narSummaryEntity != null && narSummaryEntity.getNarSummary() != null && narState.getValue().equals(narSummaryEntity.getNarSummary().getState());
        };
    }

    public NarSummaryDTO deleteNar(final NarSummaryDTO narSummary) throws NiFiClientException, IOException {
        final NarSummaryEntity deletedNar = client.getControllerClient().deleteNar(narSummary.getIdentifier(), true);
        assertNotNull(deletedNar);
        assertNotNull(deletedNar.getNarSummary());
        assertEquals(narSummary.getIdentifier(), deletedNar.getNarSummary().getIdentifier());
        return deletedNar.getNarSummary();
    }
}
