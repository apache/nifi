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

import java.io.Closeable;
import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;

/**
 * Snowflake Snowpipe Streaming Channel abstraction with offset tracking
 */
public interface SnowpipeStreamingChannel extends Closeable {
    String getName();

    ChannelCoordinates getCoordinates();

    String getConcurrencyGroup();

    BigInteger getLocalOffset();

    void updateLocalOffset(BigInteger offset);

    BigInteger fetchRemoteOffset();

    SnowflakeStreamingIngestChannel getIngestChannel();

    SnowpipeStreamingChannelManager getManager();

    String getDestinationName();

    CompletableFuture<Void> closeAsync();
}
