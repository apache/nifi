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
package org.apache.nifi.processor.util.listen.handler.socket;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.SocketChannelAttachment;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.response.socket.SSLSocketChannelResponder;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Wraps a SocketChannel with an SSLSocketChannel for receiving messages over TLS.
 */
public class SSLSocketChannelHandler<E extends Event<SocketChannel>> extends SocketChannelHandler<E> {

    private final ByteArrayOutputStream currBytes = new ByteArrayOutputStream(4096);

    public SSLSocketChannelHandler(final SelectionKey key,
                                   final AsyncChannelDispatcher dispatcher,
                                   final Charset charset,
                                   final EventFactory<E> eventFactory,
                                   final BlockingQueue<E> events,
                                   final ComponentLog logger) {
        super(key, dispatcher, charset, eventFactory, events, logger);
    }

    @Override
    public void run() {
        boolean eof = false;
        SSLSocketChannel sslSocketChannel = null;
        try {
            int bytesRead;
            final SocketChannel socketChannel = (SocketChannel) key.channel();
            final SocketChannelAttachment attachment = (SocketChannelAttachment) key.attachment();

            // get the SSLSocketChannel from the attachment
            sslSocketChannel = attachment.getSslSocketChannel();

            // SSLSocketChannel deals with byte[] so ByteBuffer isn't used here, but we'll use the size to create a new byte[]
            final ByteBuffer socketBuffer = attachment.getByteBuffer();
            byte[] socketBufferArray = new byte[socketBuffer.limit()];

            // read until no more data
            try {
                while ((bytesRead = sslSocketChannel.read(socketBufferArray)) > 0) {
                    processBuffer(sslSocketChannel, socketChannel, bytesRead, socketBufferArray);
                    logger.debug("bytes read from sslSocketChannel {}", new Object[]{bytesRead});
                }
            } catch (SocketTimeoutException ste) {
                // SSLSocketChannel will throw this exception when 0 bytes are read and the timeout threshold
                // is exceeded, we don't want to close the connection in this case
                bytesRead = 0;
            }

            // Check for closed socket
            if( bytesRead < 0 ){
                eof = true;
                logger.debug("Reached EOF, closing connection");
            } else {
                logger.debug("No more data available, returning for selection");
            }
        } catch (ClosedByInterruptException | InterruptedException e) {
            logger.debug("read loop interrupted, closing connection");
            // Treat same as closed socket
            eof = true;
        } catch (ClosedChannelException e) {
            // ClosedChannelException doesn't have a message so handle it separately from IOException
            logger.error("Error reading from channel due to channel being closed", e);
            // Treat same as closed socket
            eof = true;
        } catch (IOException e) {
            logger.error("Error reading from channel due to {}", new Object[] {e.getMessage()}, e);
            // Treat same as closed socket
            eof = true;
        } finally {
            if(eof == true) {
                IOUtils.closeQuietly(sslSocketChannel);
                dispatcher.completeConnection(key);
            } else {
                dispatcher.addBackForSelection(key);
            }
        }
    }

    /**
     * Process the contents of the buffer. Give sub-classes a chance to override this behavior.
     *
     * @param sslSocketChannel the channel the data was read from
     * @param socketChannel the socket channel being wrapped by sslSocketChannel
     * @param bytesRead the number of bytes read
     * @param buffer the buffer to process
     * @throws InterruptedException thrown if interrupted while queuing events
     */
    protected void processBuffer(final SSLSocketChannel sslSocketChannel, final SocketChannel socketChannel,
                                 final int bytesRead, final byte[] buffer) throws InterruptedException, IOException {
        final InetAddress sender = socketChannel.socket().getInetAddress();

        // go through the buffer looking for the end of each message
        for (int i = 0; i < bytesRead; i++) {
            final byte currByte = buffer[i];

            // check if at end of a message
            if (currByte == getDelimiter()) {
                if (currBytes.size() > 0) {
                    final SSLSocketChannelResponder response = new SSLSocketChannelResponder(socketChannel, sslSocketChannel);
                    final Map<String, String> metadata = EventFactoryUtil.createMapWithSender(sender.toString());
                    final E event = eventFactory.create(currBytes.toByteArray(), metadata, response);
                    events.offer(event);
                    currBytes.reset();
                }
            } else {
                currBytes.write(currByte);
            }
        }
    }

    @Override
    public byte getDelimiter() {
        return TCP_DELIMITER;
    }

}
