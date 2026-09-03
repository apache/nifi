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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NonPurgeableContentRepositoryTest {
    private static final long DELEGATE_MAX_APPENDABLE_CLAIM_BYTES = 1_048_576L;

    private static final ContentClaimCreationContext CREATION_CONTEXT = new StandardContentClaimCreationContext("component-1", "connector-1", LossTolerance.LOSS_TOLERANT);

    @Mock
    private ContentRepository delegate;

    @Mock
    private ContentClaim contentClaim;

    @Test
    void testMaxAppendableClaimBytesTakenFromDelegate() {
        when(delegate.getMaxAppendableClaimBytes()).thenReturn(DELEGATE_MAX_APPENDABLE_CLAIM_BYTES);

        final ContentRepository repository = new NonPurgeableContentRepository(delegate);

        assertEquals(DELEGATE_MAX_APPENDABLE_CLAIM_BYTES, repository.getMaxAppendableClaimBytes());
    }

    @Test
    void testCreationContextForwardedToDelegate() throws IOException {
        when(delegate.create(CREATION_CONTEXT)).thenReturn(contentClaim);

        final ContentRepository repository = new NonPurgeableContentRepository(delegate);

        assertSame(contentClaim, repository.create(CREATION_CONTEXT));
    }
}
