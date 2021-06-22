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
package org.apache.nifi.event.transport.netty.codec;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.stream.ChunkedStream;
import org.apache.nifi.event.transport.netty.DelimitedInputStream;

import java.io.InputStream;
import java.util.List;

/**
 * Message encoder for an InputStream, which wraps the stream in a ChunkedStream for use with a ChunkedWriter. Can add a delimiter
 * to the end of the output objects if the InputStream is a DelimitedInputStream.
 */
@ChannelHandler.Sharable
public class InputStreamMessageEncoder extends MessageToMessageEncoder<InputStream> {

    @Override
    protected void encode(ChannelHandlerContext context, InputStream messageStream, List<Object> out) throws Exception {
        ChunkedStream chunkedMessage = new ChunkedStream(messageStream);
        out.add(chunkedMessage);

        // If the message being sent requires a delimiter added to the end of the message, provide a DelimitedInputStream
        if (messageStream instanceof DelimitedInputStream) {
            DelimitedInputStream delimStream = (DelimitedInputStream) messageStream;
            out.add(Unpooled.wrappedBuffer(delimStream.getDelimiter()));
        }
    }
}
