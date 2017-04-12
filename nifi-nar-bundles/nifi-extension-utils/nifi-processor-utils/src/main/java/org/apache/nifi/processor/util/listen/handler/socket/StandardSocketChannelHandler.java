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
import org.apache.nifi.processor.util.listen.response.socket.SocketChannelResponder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Reads from the given SocketChannel into the provided buffer. If the given delimiter is found, the data
 * read up to that point is queued for processing.
 */
public class StandardSocketChannelHandler<E extends Event<SocketChannel>> extends SocketChannelHandler<E> {

    private final ByteArrayOutputStream currBytes = new ByteArrayOutputStream(4096);

    public StandardSocketChannelHandler(final SelectionKey key,
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
        SocketChannel socketChannel = null;

        try {
            int bytesRead;
            socketChannel = (SocketChannel) key.channel();

            final SocketChannelAttachment attachment = (SocketChannelAttachment) key.attachment();
            final ByteBuffer socketBuffer = attachment.getByteBuffer();

            // read until the buffer is full
            while ((bytesRead = socketChannel.read(socketBuffer)) > 0) {
                // prepare byte buffer for reading
                socketBuffer.flip();
                // mark the current position as start, in case of partial message read
                socketBuffer.mark();
                // process the contents that have been read into the buffer
                processBuffer(socketChannel, socketBuffer);

                // Preserve bytes in buffer for next call to run
                // NOTE: This code could benefit from the  two ByteBuffer read calls to avoid
                // this compact for higher throughput
                socketBuffer.reset();
                socketBuffer.compact();
                logger.debug("bytes read {}", new Object[]{bytesRead});
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
                IOUtils.closeQuietly(socketChannel);
                dispatcher.completeConnection(key);
            } else {
                dispatcher.addBackForSelection(key);
            }
        }
    }

    /**
     * Process the contents that have been read into the buffer. Allow sub-classes to override this behavior.
     *
     * @param socketChannel the channel the data was read from
     * @param socketBuffer the buffer the data was read into
     * @throws InterruptedException if interrupted when queuing events
     */
    protected void processBuffer(final SocketChannel socketChannel, final ByteBuffer socketBuffer) throws InterruptedException, IOException {
        // get total bytes in buffer
        final int total = socketBuffer.remaining();
        final InetAddress sender = socketChannel.socket().getInetAddress();

        // go through the buffer looking for the end of each message
        currBytes.reset();
        for (int i = 0; i < total; i++) {
            // NOTE: For higher throughput, the looking for \n and copying into the byte stream could be improved
            // Pull data out of buffer and cram into byte array
            byte currByte = socketBuffer.get();

            // check if at end of a message
            if (currByte == getDelimiter()) {
                if (currBytes.size() > 0) {
                    final SocketChannelResponder response = new SocketChannelResponder(socketChannel);
                    final Map<String, String> metadata = EventFactoryUtil.createMapWithSender(sender.toString());
                    final E event = eventFactory.create(currBytes.toByteArray(), metadata, response);
                    events.offer(event);
                    currBytes.reset();

                    // Mark this as the start of the next message
                    socketBuffer.mark();
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
