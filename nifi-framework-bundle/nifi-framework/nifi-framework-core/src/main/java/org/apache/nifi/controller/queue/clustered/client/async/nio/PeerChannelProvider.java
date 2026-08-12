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
package org.apache.nifi.controller.queue.clustered.client.async.nio;

import java.nio.channels.SocketChannel;

/**
 * Provider abstraction for creating a {@link PeerChannel} bound to a connected Socket Channel for load balancing communication.
 */
interface PeerChannelProvider {
    /**
     * Get a Peer Channel for the provided Socket Channel with TLS when supported
     *
     * @param socketChannel Connected Socket Channel for communication with the peer
     * @param peerDescription Description of the peer used for logging
     * @return Peer Channel wrapping the provided Socket Channel
     */
    PeerChannel getPeerChannel(SocketChannel socketChannel, String peerDescription);
}
