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
package org.apache.nifi.remote.io;

import org.apache.nifi.remote.AbstractCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsInput;
import org.apache.nifi.remote.protocol.CommunicationsOutput;

import java.io.IOException;

public class StandardCommunicationsSession extends AbstractCommunicationsSession {

    private final StandardInput request;
    private final StandardOutput response;
    private int timeout = 30000;
    private boolean closed = false;

    public StandardCommunicationsSession(final StandardInput request,
                                         final StandardOutput response) {
        this.request = request;
        this.response = response;
    }

    @Override
    public boolean isClosed() {
        return isUnderlyingResourceClosed() || closed;
    }

    @Override
    public CommunicationsInput getInput() {
        return request;
    }

    @Override
    public CommunicationsOutput getOutput() {
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
    public final void close() throws IOException {
        IOException suppressed = null;

        try {
            try {
                request.consume();
            } catch (final IOException ioe) {
                suppressed = ioe;
            }

            try {
                closeUnderlyingResource();
            } catch (final IOException ioe) {
                if (suppressed != null) {
                    ioe.addSuppressed(suppressed);
                }

                throw ioe;
            }

            if (suppressed != null) {
                throw suppressed;
            }
        } finally {
            closed = true;
        }
    }

    protected void closeUnderlyingResource() throws IOException {

    }

    protected boolean isUnderlyingResourceClosed() {
        return false;
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
