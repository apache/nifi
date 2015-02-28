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
package org.apache.nifi.remote.io.socket;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.apache.nifi.remote.AbstractCommunicationsSession;

public class SocketChannelCommunicationsSession extends AbstractCommunicationsSession {
    private final SocketChannel channel;
    private final SocketChannelInput request;
    private final SocketChannelOutput response;
    private int timeout = 30000;
    
    public SocketChannelCommunicationsSession(final SocketChannel socketChannel, final String uri) throws IOException {
        super(uri);
        request = new SocketChannelInput(socketChannel);
        response = new SocketChannelOutput(socketChannel);
        channel = socketChannel;
        socketChannel.configureBlocking(false);
    }
    
    @Override
    public boolean isClosed() {
        return !channel.isConnected();
    }
    
    @Override
    public SocketChannelInput getInput() {
        return request;
    }

    @Override
    public SocketChannelOutput getOutput() {
        return response;
    }

    @Override
    public void setTimeout(final int millis) throws IOException {
        request.setTimeout(millis);
        response.setTimeout(millis);
        this.timeout = millis;
    }

    @Override
    public int getTimeout() throws IOException {
        return timeout;
    }

    @Override
    public void close() throws IOException {
        IOException suppressed = null;
        
        try {
            request.consume();
        } catch (final IOException ioe) {
            suppressed = ioe;
        }
        
        try {
            channel.close();
        } catch (final IOException ioe) {
            if ( suppressed != null ) {
                ioe.addSuppressed(suppressed);
            }
            
            throw ioe;
        }
        
        if ( suppressed != null ) {
            throw suppressed;
        }
    }
    
    @Override
    public boolean isDataAvailable() {
        return request.isDataAvailable();
    }

    @Override
    public long getBytesWritten() {
        return response.getBytesWritten();
    }

    @Override
    public long getBytesRead() {
        return request.getBytesRead();
    }
    
    @Override
    public void interrupt() {
        request.interrupt();
        response.interrupt();
    }
}
