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
package org.apache.nifi.processors.standard.relp.handler;

import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.handler.socket.StandardSocketChannelHandler;
import org.apache.nifi.processor.util.listen.response.socket.SocketChannelResponder;
import org.apache.nifi.processors.standard.relp.frame.RELPDecoder;
import org.apache.nifi.processors.standard.relp.frame.RELPFrame;
import org.apache.nifi.processors.standard.relp.frame.RELPFrameException;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

/**
 * Extends the StandardSocketChannelHandler to decode bytes into RELP frames.
 */
public class RELPSocketChannelHandler<E extends Event<SocketChannel>> extends StandardSocketChannelHandler<E> {

    private RELPDecoder decoder;
    private RELPFrameHandler<E> frameHandler;

    public RELPSocketChannelHandler(final SelectionKey key,
                                    final AsyncChannelDispatcher dispatcher,
                                    final Charset charset,
                                    final EventFactory<E> eventFactory,
                                    final BlockingQueue<E> events,
                                    final ProcessorLog logger) {
        super(key, dispatcher, charset, eventFactory, events, logger);
        this.decoder = new RELPDecoder(charset);
        this.frameHandler = new RELPFrameHandler<>(key, charset, eventFactory, events, dispatcher);
    }

    @Override
    protected void processBuffer(final SocketChannel socketChannel, final ByteBuffer socketBuffer)
            throws InterruptedException, IOException {

        // get total bytes in buffer
        final int total = socketBuffer.remaining();
        final InetAddress sender = socketChannel.socket().getInetAddress();

        try {
            // go through the buffer parsing the RELP command
            for (int i = 0; i < total; i++) {
                byte currByte = socketBuffer.get();

                // if we found the end of a frame, handle the frame and mark the buffer
                if (decoder.process(currByte)) {
                    final RELPFrame frame = decoder.getFrame();

                    logger.debug("Received RELP frame with transaction {} and command {}",
                            new Object[] {frame.getTxnr(), frame.getCommand()});

                    final SocketChannelResponder responder = new SocketChannelResponder(socketChannel);
                    frameHandler.handle(frame, responder, sender.toString());
                    socketBuffer.mark();
                }
            }

            logger.debug("Done processing buffer");

        } catch (final RELPFrameException rfe) {
            logger.error("Error reading RELP frames due to {}", new Object[] {rfe.getMessage()}, rfe);
            // if an invalid frame or bad data was sent then the decoder will be left in a
            // corrupted state, so lets close the connection and cause the client to re-establish
            dispatcher.completeConnection(key);
        }
    }

    // not used for anything in RELP since the decoder encapsulates the delimiter
    @Override
    public byte getDelimiter() {
        return RELPFrame.DELIMITER;
    }

}
