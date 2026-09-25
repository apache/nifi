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

package org.apache.nifi.controller.repository;

import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.stateless.repository.ByteArrayContentRepository;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DeferredStatelessContentRepositoryTest {

    @Test
    void testResolvesChangedThreshold() throws IOException {
        final ResourceClaimManager resourceClaimManager = new StandardResourceClaimManager();
        final ContentRepository backingRepository = new ByteArrayContentRepository();
        backingRepository.initialize(new StandardContentRepositoryContext(resourceClaimManager, EventReporter.NO_OP));
        final FlowFileRepository flowFileRepository = mock(FlowFileRepository.class);
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final AtomicLong threshold = new AtomicLong(100L);
        when(processGroup.resolveStatelessContentMaxHeap()).thenAnswer(invocation -> threshold.get());
        final DeferredStatelessContentRepository repository = new DeferredStatelessContentRepository(
            processGroup, backingRepository, flowFileRepository, resourceClaimManager, EventReporter.NO_OP);

        final ContentClaim inMemoryClaim = repository.create(false);
        assertEquals("in-memory", inMemoryClaim.getResourceClaim().getContainer());
        repository.decrementClaimantCount(inMemoryClaim);
        repository.purge();

        final ResourceClaim resourceClaim = inMemoryClaim.getResourceClaim();
        assertNull(resourceClaimManager.getResourceClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId()));
        assertEquals(0, resourceClaimManager.getClaimantCount(resourceClaim));
        assertFalse(resourceClaim.isWritable());

        threshold.set(0L);
        final ContentClaim backingClaim = repository.create(false);
        assertEquals("container", backingClaim.getResourceClaim().getContainer());
    }
}
