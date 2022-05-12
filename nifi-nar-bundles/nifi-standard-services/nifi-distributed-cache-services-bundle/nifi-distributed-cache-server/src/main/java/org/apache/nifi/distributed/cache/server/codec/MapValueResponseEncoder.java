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
package org.apache.nifi.distributed.cache.server.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.nifi.distributed.cache.server.protocol.MapValueResponse;

/**
 * Message Encoder for Map Value Responses
 */
@ChannelHandler.Sharable
public class MapValueResponseEncoder extends MessageToByteEncoder<MapValueResponse> {
    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final MapValueResponse mapValueResponse, final ByteBuf byteBuf) {
        final Long revision = mapValueResponse.getRevision();
        if (revision != null) {
            byteBuf.writeLong(revision);
        }
        byteBuf.writeInt(mapValueResponse.getLength());

        final byte[] value = mapValueResponse.getValue();
        if (value != null) {
            byteBuf.writeBytes(value);
        }
    }
}
