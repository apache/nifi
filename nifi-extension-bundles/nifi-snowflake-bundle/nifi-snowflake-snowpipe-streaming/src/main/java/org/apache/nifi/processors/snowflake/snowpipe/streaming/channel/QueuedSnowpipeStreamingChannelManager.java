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

import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Snowpipe Streaming Channel Manager implementation supporting multiple queued Channels
 */
public class QueuedSnowpipeStreamingChannelManager implements SnowpipeStreamingChannelManager {
    private final ComponentLog logger;

    private final AtomicInteger channelCounter = new AtomicInteger(0);

    private final BlockingQueue<String> reusableChannelIndices = new LinkedBlockingQueue<>();
    private final SnowpipeStreamingChannelFactory channelFactory;
    private final ConcurrencyManager concurrencyManager;

    public QueuedSnowpipeStreamingChannelManager(final SnowpipeStreamingChannelFactory channelFactory, final ConcurrencyManager concurrencyManager, final ComponentLog logger) {
        this.channelFactory = channelFactory;
        this.concurrencyManager = concurrencyManager;
        this.logger = logger;
    }

    @Override
    public Optional<ChannelPromise> getChannel(final ConcurrencyClaimRequest claimRequest) {
        final String concurrencyGroup = claimRequest.getGroupRequested();

        final ConcurrencyClaimResult claimResult = concurrencyManager.tryClaim(claimRequest);
        if (claimResult == ConcurrencyClaimResult.REJECTED) {
            return Optional.empty();
        }

        final String channelIndex = getChannelIndex();
        final ChannelCoordinates coordinates = channelFactory.getCoordinates(channelIndex);

        // Only provide the Concurrency Group to the channel if it will own the concurrency group.
        final String channelConcurrencyGroup = claimResult == ConcurrencyClaimResult.OWNED ? concurrencyGroup : null;
        return Optional.of(new Promise(coordinates, channelIndex, channelConcurrencyGroup));
    }


    private String getChannelIndex() {
        // Determine the index of the channel. We will get one from the pool of reusable indices, if one is available.
        // This happens whenever we close a channel due to some failure, and want to re-open the same channel. If we don't
        // have one available, we will create a new index.
        final String channelIndex = reusableChannelIndices.poll();
        if (channelIndex != null) {
            return channelIndex;
        }

        return String.valueOf(channelCounter.getAndIncrement());
    }

    @Override
    public void returnChannel(final SnowpipeStreamingChannel channel) throws IOException {
        try {
            channel.close();
        } catch (final Throwable t) {
            throw new IOException("Failed to close Snowpipe Streaming Channel", t);
        } finally {
            final String channelName = channel.getName();
            final String channelIndex = channelName.substring(channelName.lastIndexOf('.') + 1);
            reusableChannelIndices.add(channelIndex);
            logger.debug("Channel Index {} returned to the pool", channelIndex);
        }
    }

    private class Promise implements ChannelPromise {
        private final ChannelCoordinates coordinates;
        private final String channelIndex;
        private final String concurrencyGroup;
        private SnowpipeStreamingChannel channel;

        public Promise(final ChannelCoordinates coordinates, final String channelIndex, final String concurrencyGroup) {
            this.coordinates = coordinates;
            this.channelIndex = channelIndex;
            this.concurrencyGroup = concurrencyGroup;
        }

        @Override
        public SnowpipeStreamingChannel getChannel() {
            if (channel != null) {
                return channel;
            }

            try {
                channel = channelFactory.openChannel(channelIndex, concurrencyGroup, QueuedSnowpipeStreamingChannelManager.this);
                logger.debug("Opened new Channel with name {} and index {}", channel.getName(), channelIndex);

                return channel;
            } catch (final Exception e) {
                reusableChannelIndices.add(channelIndex);
                throw new RuntimeException("Failed to open a channel with index %s".formatted(channelIndex), e);
            }
        }

        @Override
        public ChannelCoordinates getCoordinates() {
            return coordinates;
        }

        @Override
        public String getConcurrencyGroup() {
            return concurrencyGroup;
        }
    }
}
