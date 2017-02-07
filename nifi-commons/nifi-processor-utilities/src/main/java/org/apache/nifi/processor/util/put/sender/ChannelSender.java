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
package org.apache.nifi.processor.util.put.sender;

import org.apache.nifi.logging.ComponentLog;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Base class for sending messages over a channel.
 */
public abstract class ChannelSender {

    protected final int port;
    protected final String host;
    protected final int maxSendBufferSize;
    protected final ComponentLog logger;

    protected volatile int timeout = 10000;
    protected volatile long lastUsed;

    public ChannelSender(final String host, final int port, final int maxSendBufferSize, final ComponentLog logger) {
        this.port = port;
        this.host = host;
        this.maxSendBufferSize = maxSendBufferSize;
        this.logger = logger;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getTimeout() {
        return timeout;
    }

    /**
     * @return the last time data was sent over this channel
     */
    public long getLastUsed() {
        return lastUsed;
    }

    /**
     * Opens the connection to the destination.
     *
     * @throws IOException if an error occurred opening the connection.
     */
    public abstract void open() throws IOException;

    /**
     * Sends the given string over the channel.
     *
     * @param message the message to send over the channel
     * @throws IOException if there was an error communicating over the channel
     */
    public void send(final String message, final Charset charset) throws IOException {
        final byte[] bytes = message.getBytes(charset);
        send(bytes);
    }

    /**
     * Sends the given data over the channel.
     *
     * @param data the data to send over the channel
     * @throws IOException if there was an error communicating over the channel
     */
    public void send(final byte[] data) throws IOException {
        try {
            write(data);
            lastUsed = System.currentTimeMillis();
        } catch (IOException e) {
            // failed to send data over the channel, we close it to force
            // the creation of a new one next time
            close();
            throw e;
        }
    }

    /**
     * Write the given buffer to the underlying channel.
     */
    protected abstract void write(byte[] data) throws IOException;

    /**
     * @return true if the underlying channel is connected
     */
    public abstract boolean isConnected();

    /**
     * Close the underlying channel
     */
    public abstract void close();

}
