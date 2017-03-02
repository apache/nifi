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
package org.apache.nifi.processor.util.listen.dispatcher;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Dispatches handlers for a given channel.
 */
public interface ChannelDispatcher extends Runnable {

    /**
     * Opens the dispatcher listening on the given port and attempts to set the
     * OS socket buffer to maxBufferSize.
     *
     * @param nicAddress the local network interface to listen on, if null will listen on the wildcard address
     *                   which means listening on all local network interfaces
     *
     * @param port the port to listen on
     *
     * @param maxBufferSize the size to set the OS socket buffer to
     *
     * @throws IOException if an error occurred listening on the given port
     */
    void open(InetAddress nicAddress, int port, int maxBufferSize) throws IOException;

    /**
     * @return the port being listened to
     */
    int getPort();

    /**
     * Closes all listeners and stops all handler threads.
     */
    void close();

}
