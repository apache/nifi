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

package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.stream.io.StreamUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestContentRepositoryFlowFileAccess {

    @Test
    public void testInputStreamFromContentRepo() throws IOException {
        final ContentRepository contentRepo = mock(ContentRepository.class);

        final ResourceClaimManager claimManager = new StandardResourceClaimManager();
        final ResourceClaim resourceClaim = new StandardResourceClaim(claimManager, "container", "section", "id", false);
        final ContentClaim contentClaim = new StandardContentClaim(resourceClaim, 5L);

        final FlowFileRecord flowFile = mock(FlowFileRecord.class);
        when(flowFile.getContentClaim()).thenReturn(contentClaim);
        when(flowFile.getSize()).thenReturn(5L);

        final InputStream inputStream = new ByteArrayInputStream("hello".getBytes());
        when(contentRepo.read(contentClaim)).thenReturn(inputStream);

        final ContentRepositoryFlowFileAccess flowAccess = new ContentRepositoryFlowFileAccess(contentRepo);

        final InputStream repoStream = flowAccess.read(flowFile);
        verify(contentRepo, times(1)).read(contentClaim);

        final byte[] buffer = new byte[5];
        StreamUtils.fillBuffer(repoStream, buffer);
        assertEquals(-1, repoStream.read());
        assertArrayEquals("hello".getBytes(), buffer);
    }


    @Test
    public void testContentNotFoundPropagated() throws IOException {
        final ContentRepository contentRepo = mock(ContentRepository.class);

        final ResourceClaimManager claimManager = new StandardResourceClaimManager();
        final ResourceClaim resourceClaim = new StandardResourceClaim(claimManager, "container", "section", "id", false);
        final ContentClaim contentClaim = new StandardContentClaim(resourceClaim, 5L);

        final FlowFileRecord flowFile = mock(FlowFileRecord.class);
        when(flowFile.getContentClaim()).thenReturn(contentClaim);

        final ContentNotFoundException cnfe = new ContentNotFoundException(contentClaim);
        when(contentRepo.read(contentClaim)).thenThrow(cnfe);

        final ContentRepositoryFlowFileAccess flowAccess = new ContentRepositoryFlowFileAccess(contentRepo);

        try {
            flowAccess.read(flowFile);
            Assert.fail("Expected ContentNotFoundException but it did not happen");
        } catch (final ContentNotFoundException thrown) {
            // expected
            thrown.getFlowFile().orElseThrow(() -> new AssertionError("Expected FlowFile to be provided"));
        }
    }

    @Test
    public void testEOFExceptionIfNotEnoughData() throws IOException {
        final ContentRepository contentRepo = mock(ContentRepository.class);

        final ResourceClaimManager claimManager = new StandardResourceClaimManager();
        final ResourceClaim resourceClaim = new StandardResourceClaim(claimManager, "container", "section", "id", false);
        final ContentClaim contentClaim = new StandardContentClaim(resourceClaim, 5L);

        final FlowFileRecord flowFile = mock(FlowFileRecord.class);
        when(flowFile.getContentClaim()).thenReturn(contentClaim);
        when(flowFile.getSize()).thenReturn(100L);

        final InputStream inputStream = new ByteArrayInputStream("hello".getBytes());
        when(contentRepo.read(contentClaim)).thenReturn(inputStream);

        final ContentRepositoryFlowFileAccess flowAccess = new ContentRepositoryFlowFileAccess(contentRepo);

        final InputStream repoStream = flowAccess.read(flowFile);
        verify(contentRepo, times(1)).read(contentClaim);

        final byte[] buffer = new byte[5];
        StreamUtils.fillBuffer(repoStream, buffer);

        try {
            repoStream.read();
            Assert.fail("Expected EOFException because not enough bytes were in the InputStream for the FlowFile");
        } catch (final EOFException eof) {
            // expected
        }
    }

}
