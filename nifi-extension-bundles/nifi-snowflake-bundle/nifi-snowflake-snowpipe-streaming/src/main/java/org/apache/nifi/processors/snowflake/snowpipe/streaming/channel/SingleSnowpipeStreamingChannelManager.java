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

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Snowpipe Streaming Channel Manager implementation supporting a single Channel
 */
public class SingleSnowpipeStreamingChannelManager implements SnowpipeStreamingChannelManager {
    private final SnowpipeStreamingChannelFactory channelFactory;
    private final String channelIndex;
    private final BlockingQueue<SnowpipeStreamingChannelFactory> channelSourceQueue = new LinkedBlockingQueue<>(1);

    public SingleSnowpipeStreamingChannelManager(final SnowpipeStreamingChannelFactory channelFactory, final String channelIndex) {
        this.channelFactory = channelFactory;
        this.channelIndex = channelIndex;

        channelSourceQueue.add(channelFactory);
    }

    @Override
    public Optional<ChannelPromise> getChannel(final ConcurrencyClaimRequest claimRequest) {
        // Get the channel source from the queue; if closed throw an exception. This should not occur
        final SnowpipeStreamingChannelFactory channelFactory = channelSourceQueue.poll();
        if (channelFactory == null) {
            return Optional.empty();
        }

        final ChannelPromise promise = new ChannelPromise() {
            @Override
            public SnowpipeStreamingChannel getChannel() {
                return channelFactory.openChannel(channelIndex, claimRequest.getGroupRequested(), SingleSnowpipeStreamingChannelManager.this);
            }

            @Override
            public ChannelCoordinates getCoordinates() {
                return channelFactory.getCoordinates(channelIndex);
            }

            @Override
            public String getConcurrencyGroup() {
                return claimRequest.getGroupRequested();
            }
        };

        return Optional.of(promise);
    }

    @Override
    public void returnChannel(final SnowpipeStreamingChannel channel) throws IOException {
        try {
            try {
                channel.close();
            } finally {
                channelSourceQueue.add(channelFactory);
            }
        } catch (final Throwable t) {
            throw new IOException("Failed to close Channel", t);
        }
    }
}
