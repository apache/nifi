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
package org.apache.nifi.processors.snowflake.snowpipe.streaming.channel;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.PutSnowpipeStreaming;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class QueuedSnowpipeStreamingChannelManagerTest {

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(PutSnowpipeStreaming.class);
    }

    @Test
    void testWithoutCreationLimit() throws IOException {
        final ConcurrencyManager concurrencyManager = new UnboundedConcurrencyManager();
        final QueuedSnowpipeStreamingChannelManager manager = createChannelManager(concurrencyManager);
        final ConcurrencyClaim concurrencyClaim = new ConcurrencyClaim();

        final Set<SnowpipeStreamingChannel> channels = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            final Optional<ChannelPromise> optionalPromise = manager.getChannel(new ConcurrencyClaimRequest("a", concurrencyClaim));
            assertTrue(optionalPromise.isPresent());

            // Make sure channel is not reused
            assertTrue(channels.add(optionalPromise.get().getChannel()));
        }

        final SnowpipeStreamingChannel chosen = channels.iterator().next();
        manager.returnChannel(chosen);

        // Ensure that if we get another channel it reuses the same channel index but creates a new channel object
        final Optional<ChannelPromise> promise = manager.getChannel(new ConcurrencyClaimRequest("a", concurrencyClaim));
        assertTrue(promise.isPresent());
        assertNotSame(chosen, promise.get().getChannel());

        // Ensure that the channel index is reused
        assertEquals(chosen.getName(), promise.get().getChannel().getName());
    }

    @Test
    void testLimitsChannelCreation() throws IOException {
        final ConcurrencyManager concurrencyManager = new BoundedConcurrencyManager(2, runner.getLogger());
        final QueuedSnowpipeStreamingChannelManager manager = createChannelManager(concurrencyManager);

        final Optional<ChannelPromise> promise0 = manager.getChannel(new ConcurrencyClaimRequest("a", new ConcurrencyClaim()));
        assertTrue(promise0.isPresent());
        assertEquals("channel.0", promise0.get().getChannel().getName());

        final Optional<ChannelPromise> promise1 = manager.getChannel(new ConcurrencyClaimRequest("a", new ConcurrencyClaim()));
        assertTrue(promise1.isPresent());
        assertEquals("channel.1", promise1.get().getChannel().getName());

        for (int i = 0; i < 5; i++) {
            final Optional<ChannelPromise> promise = manager.getChannel(new ConcurrencyClaimRequest("a", new ConcurrencyClaim()));
            assertTrue(promise.isEmpty());
        }

        manager.returnChannel(promise0.get().getChannel());
        concurrencyManager.releaseClaim(promise0.get().getConcurrencyGroup());

        final Optional<ChannelPromise> availablePromise = manager.getChannel(new ConcurrencyClaimRequest("a", new ConcurrencyClaim()));
        assertTrue(availablePromise.isPresent());
        assertEquals("channel.0", availablePromise.get().getChannel().getName());

        for (int i = 0; i < 5; i++) {
            final Optional<ChannelPromise> promise = manager.getChannel(new ConcurrencyClaimRequest("a", new ConcurrencyClaim()));
            assertTrue(promise.isEmpty());
        }
    }

    @Test
    void testDoesNotLimitIfGroupAcquired() throws IOException {
        final ConcurrencyManager concurrencyManager = new BoundedConcurrencyManager(2, runner.getLogger());
        final QueuedSnowpipeStreamingChannelManager manager = createChannelManager(concurrencyManager);

        // We should be able to get the channel any number of times with the same Concurrency Claim, as once the
        // group is acquired, we have freedom to use it as much as we need.
        final ConcurrencyClaim claim = new ConcurrencyClaim();
        for (int i = 0; i < 100; i++) {
            final Optional<ChannelPromise> promise = manager.getChannel(new ConcurrencyClaimRequest("a", claim));
            assertTrue(promise.isPresent());
        }

        // We have a Max Concurrency of 2, so we should be allowed to obtain a second claim
        final Optional<ChannelPromise> claim2Promise = manager.getChannel(new ConcurrencyClaimRequest("a", new ConcurrencyClaim()));
        assertTrue(claim2Promise.isPresent());

        // Max Concurrency of 2 should limit us now.
        final Optional<ChannelPromise> claim3Promise = manager.getChannel(new ConcurrencyClaimRequest("a", new ConcurrencyClaim()));
        assertTrue(claim3Promise.isEmpty());

        // As soon as one channel is returned and claim released, we should be able to obtain another.
        manager.returnChannel(claim2Promise.get().getChannel());
        concurrencyManager.releaseClaim(claim2Promise.get().getConcurrencyGroup());
        final Optional<ChannelPromise> claim4Promise = manager.getChannel(new ConcurrencyClaimRequest("a", new ConcurrencyClaim()));
        assertTrue(claim4Promise.isPresent());
    }

    private QueuedSnowpipeStreamingChannelManager createChannelManager(final ConcurrencyManager concurrencyManager) {
        final SnowpipeStreamingChannelFactory factory = new SnowpipeStreamingChannelFactory() {
            @Override
            public SnowpipeStreamingChannel openChannel(final String index, final String concurrencyGroup, final SnowpipeStreamingChannelManager manager) {
                final SnowpipeStreamingChannel channel = Mockito.mock(SnowpipeStreamingChannel.class);
                when(channel.getName()).thenReturn("channel." + index);

                final SnowflakeStreamingIngestChannel ingestChannel = Mockito.mock(SnowflakeStreamingIngestChannel.class);
                when(ingestChannel.close()).thenReturn(CompletableFuture.completedFuture(null));
                when(channel.getIngestChannel()).thenReturn(ingestChannel);
                when(channel.getConcurrencyGroup()).thenReturn(concurrencyGroup);

                return channel;
            }

            @Override
            public ChannelCoordinates getCoordinates(final String channelIndex) {
                return new ChannelCoordinates("database", "schema", "table", "prefix", "index");
            }
        };

        return new QueuedSnowpipeStreamingChannelManager(factory, concurrencyManager, runner.getLogger());
    }
}
