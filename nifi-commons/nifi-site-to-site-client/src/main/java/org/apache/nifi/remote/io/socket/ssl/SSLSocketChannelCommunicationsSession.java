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
package org.apache.nifi.remote.io.socket.ssl;

import java.io.IOException;

import org.apache.nifi.remote.AbstractCommunicationsSession;

public class SSLSocketChannelCommunicationsSession extends AbstractCommunicationsSession {

    private final SSLSocketChannel channel;
    private final SSLSocketChannelInput request;
    private final SSLSocketChannelOutput response;

    public SSLSocketChannelCommunicationsSession(final SSLSocketChannel channel, final String uri) {
        super(uri);
        request = new SSLSocketChannelInput(channel);
        response = new SSLSocketChannelOutput(channel);
        this.channel = channel;
    }

    @Override
    public SSLSocketChannelInput getInput() {
        return request;
    }

    @Override
    public SSLSocketChannelOutput getOutput() {
        return response;
    }

    @Override
    public void setTimeout(final int millis) throws IOException {
        channel.setTimeout(millis);
    }

    @Override
    public int getTimeout() throws IOException {
        return channel.getTimeout();
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
            if (suppressed != null) {
                ioe.addSuppressed(suppressed);
            }

            throw ioe;
        }

        if (suppressed != null) {
            throw suppressed;
        }
    }

    @Override
    public boolean isClosed() {
        return channel.isClosed();
    }

    @Override
    public boolean isDataAvailable() {
        try {
            return request.isDataAvailable();
        } catch (final Exception e) {
            return false;
        }
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
        channel.interrupt();
    }

    @Override
    public String toString() {
        return super.toString() + "[SSLSocketChannel=" + channel + "]";
    }
}
