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
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class StandardSnowpipeStreamingChannel implements SnowpipeStreamingChannel {
    private final SnowflakeStreamingIngestChannel ingestChannel;
    private final ChannelCoordinates coordinates;
    private final SnowpipeStreamingChannelManager manager;
    private final String destinationName;
    private final String concurrencyGroup;
    private volatile BigInteger localOffset;
    private volatile boolean closed = false;
    private volatile CompletableFuture<Void> closeFuture;

    public StandardSnowpipeStreamingChannel(
            final SnowflakeStreamingIngestChannel ingestChannel,
            final ChannelCoordinates coordinates,
            final SnowpipeStreamingChannelManager manager,
            final String destinationName,
            final String concurrencyGroup
    ) {
        this.ingestChannel = ingestChannel;
        this.coordinates = coordinates;
        this.manager = manager;
        this.destinationName = destinationName;
        this.concurrencyGroup = concurrencyGroup;

        this.localOffset = fetchRemoteOffset();
    }

    @Override
    public String getName() {
        return ingestChannel.getName();
    }

    @Override
    public ChannelCoordinates getCoordinates() {
        return coordinates;
    }

    @Override
    public String getConcurrencyGroup() {
        return concurrencyGroup;
    }

    @Override
    public BigInteger getLocalOffset() {
        return localOffset;
    }

    @Override
    public void updateLocalOffset(final BigInteger offset) {
        this.localOffset = offset;
    }

    @Override
    public BigInteger fetchRemoteOffset() {
        final String channelOffsetToken = ingestChannel.getLatestCommittedOffsetToken();
        if (channelOffsetToken == null) {
            return BigInteger.valueOf(-1L);
        } else {
            try {
                // A channel is a long-lived named entity that is managed by the SDK client
                // It is feasible for a channel to be tampered with outside of this processor, however it is unlikely
                // Since the channel is most likely managed by this processor alone, it is expected that the offset token
                // will represent an incrementing integer value.
                return new BigInteger(channelOffsetToken);
            } catch (final NumberFormatException e) {
                throw new ProcessException("Unexpected channel token format, expected a number but received %s".formatted(channelOffsetToken), e);
            }
        }
    }

    @Override
    public SnowflakeStreamingIngestChannel getIngestChannel() {
        return ingestChannel;
    }

    @Override
    public SnowpipeStreamingChannelManager getManager() {
        return manager;
    }

    @Override
    public String getDestinationName() {
        return destinationName;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (closeFuture != null) {
            return closeFuture;
        }

        closed = true;
        closeFuture = ingestChannel.close();
        return closeFuture;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            try {
                closeFuture.get(1, TimeUnit.MINUTES);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting to close Channel", ie);
            } catch (final Exception e) {
                throw new IOException("Failed to close Channel", e);
            }

            return;
        }

        closed = true;

        try {
            closeAsync().get();
        } catch (final Exception e) {
            throw new IOException("Failed to close Channel", e);
        }
    }
}
