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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class NettyEventSenderTest {
    private static final String LOCALHOST = "127.0.0.1";

    @Mock
    private ChannelPool channelPool;

    @Mock
    private EventLoopGroup group;

    @Mock
    private Future<?> shutdownFuture;

    @Test
    public void testClose() {
        final SocketAddress socketAddress = InetSocketAddress.createUnresolved(LOCALHOST, NetworkUtils.getAvailableTcpPort());
        final NettyEventSender<?> sender = new NettyEventSender<>(group, channelPool, socketAddress, false);
        doReturn(shutdownFuture).when(group).shutdownGracefully();
        sender.close();

        verify(channelPool).close();
        verify(group).shutdownGracefully();
    }
}
