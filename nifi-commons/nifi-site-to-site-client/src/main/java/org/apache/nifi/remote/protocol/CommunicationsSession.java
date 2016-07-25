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
package org.apache.nifi.remote.protocol;

import java.io.Closeable;
import java.io.IOException;

public interface CommunicationsSession extends Closeable {

    public static final byte[] MAGIC_BYTES = {(byte) 'N', (byte) 'i', (byte) 'F', (byte) 'i'};

    CommunicationsInput getInput();

    CommunicationsOutput getOutput();

    void setTimeout(int millis) throws IOException;

    int getTimeout() throws IOException;

    String getUserDn();

    void setUserDn(String dn);

    boolean isDataAvailable();

    long getBytesWritten();

    long getBytesRead();

    /**
     * Asynchronously interrupts this FlowFileCodec. Implementations must ensure
     * that they stop sending and receiving data as soon as possible after this
     * method has been called, even if doing so results in sending only partial
     * data to the peer. This will usually result in the peer throwing a
     * SocketTimeoutException.
     */
    void interrupt();

    /**
     * @return <code>true</code> if the connection is closed, <code>false</code>
     * otherwise
     */
    boolean isClosed();

    /**
     * @param communicantUrl Communicant's url that this session is assigned to.
     * @param sourceFlowFileIdentifier Source Flow-file's uuid.
     * @return A transit uri to be used in a provenance event.
     */
    String createTransitUri(final String communicantUrl, final String sourceFlowFileIdentifier);
}
