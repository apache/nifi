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
package org.apache.nifi.integration.lifecycle;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.repository.FileSystemRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.WriteAheadFlowFileRepository;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class ContentCleanupIT extends FrameworkIntegrationTest {

    @Test
    public void testCompletedContentClaimCleanedUpOnCheckpoint() throws InterruptedException, IOException, ExecutionException {
        final AtomicReference<FlowFileRecord> largeFlowFileReference = new AtomicReference<>();
        final AtomicReference<FlowFileRecord> smallFlowFileReference = new AtomicReference<>();

        // Processor to write 1 MB of content to a FlowFile
        final ProcessorNode createLargeProcessor = createGenerateProcessor(1024 * 1024, largeFlowFileReference);
        final ProcessorNode createSmallProcessor = createGenerateProcessor(5, smallFlowFileReference);

        connect(createLargeProcessor, getTerminateProcessor(), REL_SUCCESS);
        connect(createSmallProcessor, getTerminateProcessor(), REL_SUCCESS);

        // Trigger the create processor.
        triggerOnce(createLargeProcessor);
        triggerOnce(createSmallProcessor);

        // Ensure content available and has a claim count of 1.
        final ContentClaim largeContentClaim = largeFlowFileReference.get().getContentClaim();
        final ContentClaim smallContentClaim = smallFlowFileReference.get().getContentClaim();
        assertNotEquals(largeContentClaim.getResourceClaim(), smallContentClaim.getResourceClaim());
        assertEquals(1, getContentRepository().getClaimantCount(largeContentClaim));
        assertEquals(1, getContentRepository().getClaimantCount(smallContentClaim));

        // Ensure that content is still available and considered 'in use'
        final FileSystemRepository fileSystemRepository = (FileSystemRepository) getContentRepository();
        final Path largeClaimPath = fileSystemRepository.getPath(largeContentClaim, false);
        final Path smallClaimPath = fileSystemRepository.getPath(smallContentClaim, false);
        assertTrue(Files.exists(largeClaimPath));
        assertTrue(largeContentClaim.getResourceClaim().isInUse());
        assertTrue(Files.exists(smallClaimPath));
        assertTrue(smallContentClaim.getResourceClaim().isInUse());

        int recordCount = ((WriteAheadFlowFileRepository) getFlowFileRepository()).checkpoint();
        assertEquals(2, recordCount);

        // Trigger the delete Processor.
        triggerOnce(getTerminateProcessor());
        triggerOnce(getTerminateProcessor());

        // Claim count should now be 0 and resource claim should not be in use.
        assertEquals(0, getContentRepository().getClaimantCount(largeContentClaim));
        assertEquals(0, getContentRepository().getClaimantCount(largeContentClaim));

        assertFalse(largeContentClaim.getResourceClaim().isInUse());
        assertTrue(smallContentClaim.getResourceClaim().isInUse());

        // Checkpoint the FlowFile Repo
        recordCount = ((WriteAheadFlowFileRepository) getFlowFileRepository()).checkpoint();
        assertEquals(0, recordCount);

        // Wait for the data to be deleted/archived.
        waitForClaimDestruction(largeContentClaim);

        assertTrue(Files.exists(smallClaimPath));

        assertProvenanceEventCount(ProvenanceEventType.CREATE, 2);
        assertProvenanceEventCount(ProvenanceEventType.DROP, 2);
    }


    @Test
    public void testTransientClaimsNotHeld() throws ExecutionException, InterruptedException, IOException {
        final AtomicReference<ContentClaim> claimReference = new AtomicReference<>();

        final ProcessorNode processor = createProcessorNode((context, session) -> {
            FlowFile flowFile = session.create();

            for (int i=0; i < 1000; i++) {
                final byte[] bytes = String.valueOf(i).getBytes();
                flowFile = session.write(flowFile, out -> out.write(bytes));
            }

            // Write 1 MB to the content claim in order to ensure that the claim is no longer usable.
            final byte[] oneMB = new byte[1024 * 1024];
            flowFile = session.write(flowFile, out -> out.write(oneMB));

            claimReference.set(((FlowFileRecord) flowFile).getContentClaim());
            session.transfer(flowFile, REL_SUCCESS);

        }, REL_SUCCESS);

        connect(processor, getTerminateProcessor(), REL_SUCCESS);
        triggerOnce(processor);

        final int claimCount = getContentRepository().getClaimantCount(claimReference.get());
        assertEquals(1, claimCount);

        int recordCount = ((WriteAheadFlowFileRepository) getFlowFileRepository()).checkpoint();
        assertEquals(1, recordCount);
        assertTrue(claimReference.get().getResourceClaim().isInUse());

        triggerOnce(getTerminateProcessor());
        assertFalse(claimReference.get().getResourceClaim().isInUse());

        recordCount = ((WriteAheadFlowFileRepository) getFlowFileRepository()).checkpoint();
        assertEquals(0, recordCount);

        waitForClaimDestruction(claimReference.get());

        assertProvenanceEventCount(ProvenanceEventType.CREATE, 1);
        assertProvenanceEventCount(ProvenanceEventType.DROP, 1);
    }


    @Test
    public void testCloneIncrementsContentClaim() throws ExecutionException, InterruptedException, IOException {
        final AtomicReference<FlowFileRecord> flowFileReference = new AtomicReference<>();
        final ProcessorNode createProcessor = createGenerateProcessor(1024 * 1024, flowFileReference);

        connect(createProcessor, getTerminateProcessor(), REL_SUCCESS);
        connect(createProcessor, getTerminateAllProcessor(), REL_SUCCESS);

        triggerOnce(createProcessor);

        final ContentClaim contentClaim = flowFileReference.get().getContentClaim();
        assertEquals(2, getContentRepository().getClaimantCount(contentClaim));
        assertTrue(contentClaim.getResourceClaim().isInUse());

        triggerOnce(getTerminateProcessor());
        assertEquals(1, getContentRepository().getClaimantCount(contentClaim));
        assertTrue(contentClaim.getResourceClaim().isInUse());

        triggerOnce(getTerminateAllProcessor());
        assertEquals(0, getContentRepository().getClaimantCount(contentClaim));
        assertFalse(contentClaim.getResourceClaim().isInUse());

        assertEquals(0, ((WriteAheadFlowFileRepository) getFlowFileRepository()).checkpoint());

        waitForClaimDestruction(contentClaim);
    }


    private void waitForClaimDestruction(final ContentClaim contentClaim) {
        final Path path = ((FileSystemRepository) getContentRepository()).getPath(contentClaim, false);

        while (Files.exists(path)) {
            try {
                Thread.sleep(10L);
            } catch (final Exception e) {
            }
        }
    }
}
