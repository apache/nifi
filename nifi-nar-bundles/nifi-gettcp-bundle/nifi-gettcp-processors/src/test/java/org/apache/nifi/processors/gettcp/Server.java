/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.gettcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 *
 */
public class Server extends AbstractSocketHandler {

    public static void main(String[] args) throws Exception {
        InetSocketAddress address = new InetSocketAddress(9999);
        Server server = new Server(address, 4096, (byte) '\r');
        server.start();
        System.in.read();
    }

    /**
     *
     * @param address
     * @param readingBufferSize
     */
    public Server(InetSocketAddress address, int readingBufferSize, byte endOfMessageByte) {
        super(address, readingBufferSize, endOfMessageByte);
    }

    /**
     *
     */
    @Override
    InetSocketAddress connect() throws IOException {
        this.rootChannel = ServerSocketChannel.open();
        ServerSocketChannel channel = (ServerSocketChannel) rootChannel;
        channel.configureBlocking(false);
        channel.socket().bind(this.address);
        channel.register(this.selector, SelectionKey.OP_ACCEPT);
        return this.address;
    }

    /**
     *
     */
    @Override
    void doAccept(SelectionKey selectionKey) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverChannel.accept();
        if (logger.isInfoEnabled()) {
            logger.info("Accepted connection from: " + channel.socket());
        }
        channel.configureBlocking(false);
        channel.register(this.selector, SelectionKey.OP_READ);
    }

    /**
     * Unlike the client side the read on the server will happen using receiving
     * thread.
     */
    @Override
    void processData(SelectionKey selectionKey, ByteBuffer readBuffer) throws IOException {
        logger.info("Server received message of " + readBuffer.limit() + " bytes in size and will delegate to all registered clients.");
        for (SelectionKey key : selector.keys()) {
            if (key.isValid() && key.channel() instanceof SocketChannel && !selectionKey.equals(key)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Distributing incoming message to " + key.channel());
                }
                SocketChannel sch = (SocketChannel) key.channel();
                sch.write(readBuffer);
                readBuffer.rewind();
            }
        }
    }
}