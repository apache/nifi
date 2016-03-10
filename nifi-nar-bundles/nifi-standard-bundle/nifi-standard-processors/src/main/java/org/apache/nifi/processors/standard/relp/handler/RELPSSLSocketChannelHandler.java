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
import org.apache.nifi.processor.util.listen.handler.socket.SSLSocketChannelHandler;
import org.apache.nifi.processor.util.listen.response.socket.SSLSocketChannelResponder;
import org.apache.nifi.processors.standard.relp.frame.RELPDecoder;
import org.apache.nifi.processors.standard.relp.frame.RELPFrame;
import org.apache.nifi.processors.standard.relp.frame.RELPFrameException;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

/**
 * A RELP implementation of SSLSocketChannelHandler.
 */
public class RELPSSLSocketChannelHandler<E extends Event<SocketChannel>> extends SSLSocketChannelHandler<E> {

    private RELPDecoder decoder;
    private RELPFrameHandler<E> frameHandler;

    public RELPSSLSocketChannelHandler(final SelectionKey key,
                                       final AsyncChannelDispatcher dispatcher,
                                       final Charset charset,
                                       final EventFactory<E> eventFactory,
                                       final BlockingQueue<E> events,
                                       final ProcessorLog logger) {
        super(key, dispatcher, charset, eventFactory, events, logger);
        this.decoder = new RELPDecoder(charset);
        this.frameHandler = new RELPFrameHandler<>(key, charset, eventFactory, events, dispatcher, logger);
    }

    @Override
    protected void processBuffer(final SSLSocketChannel sslSocketChannel, final SocketChannel socketChannel,
                                 final int bytesRead, final byte[] buffer) throws InterruptedException, IOException {

        final InetAddress sender = socketChannel.socket().getInetAddress();
        try {
            // go through the buffer parsing the RELP command
            for (int i = 0; i < bytesRead; i++) {
                byte currByte = buffer[i];

                // if we found the end of a frame, handle the frame and mark the buffer
                if (decoder.process(currByte)) {
                    final RELPFrame frame = decoder.getFrame();

                    logger.debug("Received RELP frame with transaction {} and command {}",
                            new Object[] {frame.getTxnr(), frame.getCommand()});

                    final SSLSocketChannelResponder responder = new SSLSocketChannelResponder(socketChannel, sslSocketChannel);
                    frameHandler.handle(frame, responder, sender.toString());
                }
            }

            logger.debug("Done processing buffer");

        } catch (final RELPFrameException rfe) {
            logger.error("Error reading RELP frames due to {}", new Object[] {rfe.getMessage()} , rfe);
            // if an invalid frame or bad data was sent then the decoder will be left in a
            // corrupted state, so lets close the connection and cause the client to re-establish
            dispatcher.completeConnection(key);
        }
    }

}
