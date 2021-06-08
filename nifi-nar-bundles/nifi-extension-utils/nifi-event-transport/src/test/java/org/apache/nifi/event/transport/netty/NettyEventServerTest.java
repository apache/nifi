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
package org.apache.nifi.event.transport.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NettyEventServerTest {
    @Mock
    private Channel channel;

    @Mock
    private EventLoopGroup group;

    @Mock
    private ChannelFuture closeFuture;

    @Mock
    private Future<?> shutdownFuture;

    @Test
    public void testShutdown() {
        final NettyEventServer server = new NettyEventServer(group, channel);
        when(channel.isOpen()).thenReturn(true);
        when(channel.close()).thenReturn(closeFuture);
        doReturn(shutdownFuture).when(group).shutdownGracefully();
        server.shutdown();

        verify(channel).close();
        verify(group).shutdownGracefully();
    }
}
