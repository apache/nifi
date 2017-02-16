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
package org.apache.nifi.irc.handlers;

import net.engio.mbassy.listener.Handler;
import org.apache.nifi.logging.ComponentLog;
import org.kitteh.irc.client.library.event.client.ClientConnectedEvent;
import org.kitteh.irc.client.library.event.client.ClientConnectionClosedEvent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


public class ServiceEventHandler {
    private final Set<String> requestedChannels;
    private final ComponentLog logger;
    private final AtomicBoolean connectionStatus;

    public ServiceEventHandler(AtomicBoolean connectionStatus, Set<String> requestedChannels, ComponentLog logger) {
        this.requestedChannels = requestedChannels;
        this.logger = logger;
        this.connectionStatus = connectionStatus;
    }

    @Handler
    protected void onConnect(ClientConnectedEvent event) {
        connectionStatus.set(true);
        logger.info("Successfully connected to: " + event.getServerInfo().getAddress().get());
        // If not inside all the desired channel, try to join
        if (!event.getClient().getChannels().contains(requestedChannels) || event.getClient().getChannels().isEmpty()) {
            // chop the already joined channels
            Set<String> pendingChannels = new HashSet<>(requestedChannels);
            pendingChannels.removeAll(event.getClient().getChannels());
            pendingChannels.forEach(pendingChannel -> event.getClient().addChannel(pendingChannel));
        }
    }

    @Handler
    protected void onDisconnect(ClientConnectionClosedEvent event) {
        connectionStatus.set(false);
        // isReconnecting is used to KICL to state if re-connection is being attempted
        // e.g. connection dropped but client is trying to reconnect
        if (event.isReconnecting()) {
            logger.warn("Connection to IRC server dropped! Attempting to reconnect");
        } else {
            // This in theory should only be invoked during shutdown
            logger.info("Successfully disconnected from: " + event.getClient().getServerInfo().getAddress().get());
        }
    }
}
