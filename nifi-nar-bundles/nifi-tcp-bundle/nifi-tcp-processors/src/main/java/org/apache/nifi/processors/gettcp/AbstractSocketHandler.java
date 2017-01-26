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
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class to implement async TCP Client/Server components
 *
 */
abstract class AbstractSocketHandler {

    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ByteBuffer readingBuffer;

    private final Runnable listenerTask;

    private volatile ExecutorService listenerTaskExecutor;

    final InetSocketAddress address;

    volatile NetworkChannel rootChannel;

    volatile Selector selector;

    private final AtomicBoolean isRunning;

    protected final byte endOfMessageByte;

    /**
     * This constructor configures the address to bind to, the size of the buffer to use for reading, and
     * the byte pattern to use for demarcating the end of a message.
     *
     * @param address the socket address
     * @param readingBufferSize the buffer size
     * @param endOfMessageByte the byte indicating EOM
     */
    public AbstractSocketHandler(InetSocketAddress address, int readingBufferSize, byte endOfMessageByte) {
        this.address = address;
        this.listenerTask = new ListenerTask();
        this.readingBuffer = ByteBuffer.allocate(readingBufferSize);
        this.isRunning = new AtomicBoolean();
        this.endOfMessageByte = endOfMessageByte;
    }

    /**
     * Once the handler is constructed this should be called to start the handler. Although
     * this method is safe to be called by multiple threads, it should only be called once.
     *
     * @throws IllegalStateException if it fails to start listening on the port that is configured
     *
     */
    public void start() {
        if (this.isRunning.compareAndSet(false, true)) {
            try {
                if (this.selector == null || !this.selector.isOpen()) {
                    this.selector = Selector.open();
                    InetSocketAddress connectedAddress = this.connect();
                    this.listenerTaskExecutor = Executors.newCachedThreadPool();
                    this.listenerTaskExecutor.execute(this.listenerTask);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Started listener for " + AbstractSocketHandler.this.getClass().getSimpleName());
                    }
                    if (logger.isInfoEnabled()) {
                        logger.info("Successfully bound to " + connectedAddress);
                    }
                }
            } catch (Exception e) {
                this.stop();
                throw new IllegalStateException("Failed to start " + this.getClass().getName(), e);
            }
        }
    }

    /**
     * This should be called to stop the handler from listening on the socket. Although it is recommended
     * that this is called once, by a single thread, this method does protect itself from being called by more
     * than one thread and more than one time.
     *
     */
    public void stop() {
        if (this.isRunning.compareAndSet(true, false)) {
            try {
                if (this.selector != null && this.selector.isOpen()) { // since stop must be idempotent, we need to check if selector is open to avoid ClosedSelectorException
                    Set<SelectionKey> selectionKeys = new HashSet<>(this.selector.keys());
                    for (SelectionKey key : selectionKeys) {
                        key.cancel();
                        try {
                            key.channel().close();
                        } catch (IOException e) {
                            logger.warn("Failure while closing channel", e);
                        }
                    }
                    try {
                        this.selector.close();
                    } catch (Exception e) {
                        logger.warn("Failure while closinig selector", e);
                    }
                    logger.info(this.getClass().getSimpleName() + " is stopped listening on " + address);
                }
            } finally {
                if (this.listenerTaskExecutor != null) {
                    this.listenerTaskExecutor.shutdown();
                }
            }
        }
    }

    /**
     * Checks if this component is running.
     */
    public boolean isRunning() {
        return this.isRunning.get();
    }

    /**
     * This must be overridden by an implementing class and should establish the socket connection.
     *
     * @throws Exception if an exception occurs
     */
    abstract InetSocketAddress connect() throws Exception;

    /**
     * Will process the data received from the channel.
     *
     * @param selectionKey key for the channel the data came from
     * @param buffer buffer of received data
     * @throws IOException if there is a problem processing the data
     */
    abstract void processData(SelectionKey selectionKey, ByteBuffer buffer) throws IOException;

    /**
     * This does not perform any work at this time as all current implementations of this class
     * provide the client side of the connection and thus do not accept connections.
     *
     * @param selectionKey the selection key
     * @throws IOException if there is a problem
     */
    void doAccept(SelectionKey selectionKey) throws IOException {
        // noop
    }

    /**
     * Main listener task which will process delegate {@link SelectionKey}
     * selected from the {@link Selector} to the appropriate processing method
     * (e.g., accept, read, write etc.)
     */
    private class ListenerTask implements Runnable {
        @Override
        public void run() {
            try {
                while (AbstractSocketHandler.this.rootChannel != null && AbstractSocketHandler.this.rootChannel.isOpen() && AbstractSocketHandler.this.selector.isOpen()) {
                    if (AbstractSocketHandler.this.selector.isOpen() && AbstractSocketHandler.this.selector.select(10) > 0) {
                        Iterator<SelectionKey> keys = AbstractSocketHandler.this.selector.selectedKeys().iterator();
                        while (keys.hasNext()) {
                            SelectionKey selectionKey = keys.next();
                            keys.remove();
                            if (selectionKey.isValid()) {
                                if (selectionKey.isAcceptable()) {
                                    this.accept(selectionKey);
                                } else if (selectionKey.isReadable()) {
                                    this.read(selectionKey);
                                } else if (selectionKey.isConnectable()) {
                                    this.connect(selectionKey);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Exception in socket listener loop", e);
            }

            logger.debug("Exited Listener loop.");
            AbstractSocketHandler.this.stop();
        }

        /**
         * Accept the selectable channel
         *
         * @throws IOException in the event that something goes wrong accepting it
         */
        private void accept(SelectionKey selectionKey) throws IOException {
            AbstractSocketHandler.this.doAccept(selectionKey);
        }

        /**
         * This will connect the channel; if it is in a pending state then this will finish
         * establishing the connection. Finally the socket handler is registered with this
         * channel.
         *
         * @throws IOException if anything goes wrong during the connection establishment
         * or registering of the handler
         */
        private void connect(SelectionKey selectionKey) throws IOException {
            SocketChannel clientChannel = (SocketChannel) selectionKey.channel();
            if (clientChannel.isConnectionPending()) {
                clientChannel.finishConnect();
            }
            clientChannel.register(AbstractSocketHandler.this.selector, SelectionKey.OP_READ);
        }

        /**
         * The main read loop which reads packets from the channel and sends
         * them to implementations of
         * {@link AbstractSocketHandler#processData(SelectionKey, ByteBuffer)}.
         * So if a given implementation is a Server it is probably going to
         * broadcast received message to all connected sockets (e.g., chat
         * server). If such implementation is the Client, then it is most likely
         * the end of the road where message is processed.
         */
        private void read(SelectionKey selectionKey) throws IOException {
            SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

            int count = -1;
            boolean finished = false;
            while (!finished && (count = socketChannel.read(AbstractSocketHandler.this.readingBuffer)) > 0){
                byte lastByte = AbstractSocketHandler.this.readingBuffer.get(AbstractSocketHandler.this.readingBuffer.position() - 1);
                if (AbstractSocketHandler.this.readingBuffer.remaining() == 0 || lastByte == AbstractSocketHandler.this.endOfMessageByte) {
                    this.processBuffer(selectionKey);
                    if (lastByte == AbstractSocketHandler.this.endOfMessageByte) {
                        finished = true;
                    }
                }
            }

            if (count == -1) {
                if (AbstractSocketHandler.this.readingBuffer.position() > 0) {// flush remainder, since nothing else is coming
                    this.processBuffer(selectionKey);
                }
                selectionKey.cancel();
                socketChannel.close();
                if (logger.isInfoEnabled()) {
                    logger.info("Connection closed by: " + socketChannel.socket());
                }
            }
        }

        private void processBuffer(SelectionKey selectionKey) throws IOException {
            AbstractSocketHandler.this.readingBuffer.flip();
            byte[] message = new byte[AbstractSocketHandler.this.readingBuffer.limit()];
            AbstractSocketHandler.this.readingBuffer.get(message);
            AbstractSocketHandler.this.processData(selectionKey, ByteBuffer.wrap(message));
            AbstractSocketHandler.this.readingBuffer.clear();
        }
    }
}