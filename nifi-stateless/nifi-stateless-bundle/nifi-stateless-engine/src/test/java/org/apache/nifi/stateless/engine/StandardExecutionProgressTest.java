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

package org.apache.nifi.stateless.engine;

import org.apache.nifi.components.state.StatelessStateManagerProvider;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.stateless.flow.DataflowTriggerContext;
import org.apache.nifi.stateless.flow.FailurePortEncounteredException;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.apache.nifi.stateless.queue.DrainableFlowFileQueue;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.stateless.session.AsynchronousCommitTracker;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StandardExecutionProgressTest {

    @Test
    void testOutputClaimHeldUntilAcknowledged() {
        final TestContext context = createTestContext();

        context.progress().enqueueTriggerResult(() -> { }, failure -> { });
        final TriggerResult result = context.results().remove();

        verify(context.contentRepository(), never()).decrementClaimantCount(context.contentClaim());

        result.acknowledge();

        verify(context.contentRepository()).decrementClaimantCount(context.contentClaim());
        verify(context.contentRepository()).purge();
    }

    @Test
    void testOutputClaimReleasedWhenAborted() {
        final TestContext context = createTestContext();
        context.progress().enqueueTriggerResult(() -> { }, failure -> { });
        final TriggerResult result = context.results().remove();

        result.abort(new IOException("Processing failed"));

        verify(context.contentRepository()).decrementClaimantCount(context.contentClaim());
        verify(context.contentRepository()).purge();
        verify(context.purgeAction()).purge();
    }

    @Test
    void testEachAbortedResultPurgesReleasedClaims() {
        final TestContext context = createTestContext();
        context.progress().enqueueTriggerResult(() -> { }, failure -> { });
        context.progress().enqueueTriggerResult(() -> { }, failure -> { });
        final TriggerResult firstResult = context.results().remove();
        final TriggerResult secondResult = context.results().remove();

        firstResult.abort(new IOException("First processing failure"));
        secondResult.abort(new IOException("Second processing failure"));

        verify(context.contentRepository(), times(2)).decrementClaimantCount(context.contentClaim());
        verify(context.contentRepository(), times(2)).purge();
    }

    @Test
    void testFailurePortReleasesOutputClaimOnce() {
        final TestContext context = createTestContext(Set.of("out"));

        assertThrows(FailurePortEncounteredException.class, () -> context.progress().enqueueTriggerResult(() -> { }, failure -> { }));
        context.progress().notifyExecutionFailed(new IOException("Failure port encountered"));

        verify(context.contentRepository(), times(1)).decrementClaimantCount(context.contentClaim());
        verify(context.purgeAction()).purge();
    }

    private TestContext createTestContext() {
        return createTestContext(Set.of());
    }

    private TestContext createTestContext(final Set<String> failurePortNames) {
        final ContentClaim contentClaim = mock(ContentClaim.class);
        final FlowFileRecord flowFile = mock(FlowFileRecord.class);
        when(flowFile.getContentClaim()).thenReturn(contentClaim);

        final DrainableFlowFileQueue flowFileQueue = mock(DrainableFlowFileQueue.class);
        when(flowFileQueue.size()).thenReturn(new QueueSize(1, 1L));
        doAnswer(invocation -> {
            final List<FlowFileRecord> destination = invocation.getArgument(0);
            destination.add(flowFile);
            return null;
        }).when(flowFileQueue).drainTo(anyList());

        final Connection connection = mock(Connection.class);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        final Port outputPort = mock(Port.class);
        when(outputPort.getName()).thenReturn("out");
        when(outputPort.getIncomingConnections()).thenReturn(List.of(connection));
        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(rootGroup.getOutputPorts()).thenReturn(Set.of(outputPort));

        final ContentRepository contentRepository = mock(ContentRepository.class);
        final RepositoryContextFactory repositoryContextFactory = mock(RepositoryContextFactory.class);
        when(repositoryContextFactory.getContentRepository()).thenReturn(contentRepository);
        final BlockingQueue<TriggerResult> results = new LinkedBlockingQueue<>();
        final FlowPurgeAction purgeAction = mock(FlowPurgeAction.class);
        final StandardExecutionProgress progress = new StandardExecutionProgress(rootGroup, List.of(), results, repositoryContextFactory, failurePortNames,
            mock(AsynchronousCommitTracker.class), mock(StatelessStateManagerProvider.class), mock(DataflowTriggerContext.class), purgeAction);
        return new TestContext(progress, contentRepository, purgeAction, contentClaim, results);
    }

    private record TestContext(StandardExecutionProgress progress, ContentRepository contentRepository, FlowPurgeAction purgeAction, ContentClaim contentClaim,
                               BlockingQueue<TriggerResult> results) {
    }
}
