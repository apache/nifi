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

import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import java.util.List;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NettyTransportsTest {
  @Test
  public void nioIsAvailable() {
    assertTrue(NettyTransports.NIO.isAvailable());
    assertTrue(NettyTransports.AVAILABLE_TRANSPORT_NAMES.contains("nio"));
  }

  @Test
  @EnabledOnOs(value = OS.LINUX)
  public void epollIsAvailableOnLinux() {
    Epoll.ensureAvailability();
    assertTrue(Epoll.isAvailable());
    assertTrue(NettyTransports.EPOLL.isAvailable());
    assertEquals(NettyTransports.AVAILABLE_TRANSPORT_NAMES, List.of("epoll", "nio"));
  }

  @Test
  @EnabledOnOs(value = OS.MAC)
  public void kqueueIsAvailableOnMac() {
    KQueue.ensureAvailability();
    assertTrue(KQueue.isAvailable());
    assertTrue(NettyTransports.KQUEUE.isAvailable());
    assertEquals(NettyTransports.AVAILABLE_TRANSPORT_NAMES, List.of("kqueue", "nio"));
  }

  @Test
  public void getDefaultTransport() {
    NettyTransports.NettyTransport transport = NettyTransports.getDefaultNettyTransport();
    assertTrue(transport.isAvailable());
    if (SystemUtils.IS_OS_MAC_OSX) {
      assertEquals(transport.name(), "kqueue");
    } else if (SystemUtils.IS_OS_LINUX) {
      assertEquals(transport.name(), "epoll");
    } else {
      assertEquals(transport.name(), "nio");
    }
  }
}
