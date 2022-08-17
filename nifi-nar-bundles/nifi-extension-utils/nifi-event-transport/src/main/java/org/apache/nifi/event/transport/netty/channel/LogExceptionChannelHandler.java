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
package org.apache.nifi.event.transport.netty.channel;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.nifi.logging.ComponentLog;

import java.net.SocketAddress;

/**
 * Log Exception Channel Handler for logging exceptions in absence of other handlers
 */
@ChannelHandler.Sharable
public class LogExceptionChannelHandler extends ChannelInboundHandlerAdapter {
    private final ComponentLog log;

    public LogExceptionChannelHandler(final ComponentLog log) {
        this.log = log;
    }

    /**
     * Log Exceptions caught during Channel handling
     *
     * @param context Channel Handler Context
     * @param exception Exception
     */
    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable exception) {
        final SocketAddress remoteAddress = context.channel().remoteAddress();
        log.warn("Communication Failed with Remote Address [{}]", remoteAddress, exception);
    }
}
