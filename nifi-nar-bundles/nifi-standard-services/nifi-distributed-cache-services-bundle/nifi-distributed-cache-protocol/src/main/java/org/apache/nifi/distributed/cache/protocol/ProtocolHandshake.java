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
package org.apache.nifi.distributed.cache.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.nifi.distributed.cache.protocol.exception.HandshakeException;
import org.apache.nifi.remote.VersionNegotiator;

public class ProtocolHandshake {

    public static final byte[] MAGIC_HEADER = new byte[]{'N', 'i', 'F', 'i'};

    public static final int RESOURCE_OK = 20;
    public static final int DIFFERENT_RESOURCE_VERSION = 21;
    public static final int ABORT = 255;

    /**
     * <p>Initiate handshake to ensure client and server can communicate with the same protocol.
     * If the server doesn't support requested protocol version, HandshakeException will be thrown.</p>
     *
     * <p>DistributedMapCache version histories:<ul>
     *     <li>3: Added subMap, keySet, removeAndGet, removeByPatternAndGet methods.</li>
     *     <li>2: Added atomic update operations (fetch and replace) using optimistic lock with revision number.</li>
     *     <li>1: Initial version.</li>
     * </ul></p>
     *
     * <p>DistributedSetCache version histories:<ul>
     *     <li>1: Initial version.</li>
     * </ul></p>
     */
    public static void initiateHandshake(final InputStream in, final OutputStream out, final VersionNegotiator versionNegotiator) throws IOException, HandshakeException {
        final DataInputStream dis = new DataInputStream(in);
        final DataOutputStream dos = new DataOutputStream(out);

        try {
            dos.write(MAGIC_HEADER);

            initiateVersionNegotiation(versionNegotiator, dis, dos);
        } finally {
            dos.flush();
        }
    }

    public static void receiveHandshake(final InputStream in, final OutputStream out, final VersionNegotiator versionNegotiator) throws IOException, HandshakeException {
        final DataInputStream dis = new DataInputStream(in);
        final DataOutputStream dos = new DataOutputStream(out);

        try {
            final byte[] magicHeaderBuffer = new byte[MAGIC_HEADER.length];
            dis.readFully(magicHeaderBuffer);

            receiveVersionNegotiation(versionNegotiator, dis, dos);
        } finally {
            dos.flush();
        }
    }

    private static void initiateVersionNegotiation(final VersionNegotiator negotiator, final DataInputStream dis, final DataOutputStream dos) throws IOException, HandshakeException {
        // Write the classname of the RemoteStreamCodec, followed by its version
        dos.writeInt(negotiator.getVersion());
        dos.flush();

        // wait for response from server.
        final int statusCode = dis.read();
        switch (statusCode) {
            case RESOURCE_OK:   // server accepted our proposal of codec name/version
                return;
            case DIFFERENT_RESOURCE_VERSION:    // server accepted our proposal of codec name but not the version
                // Get server's preferred version
                final int newVersion = dis.readInt();

                // Determine our new preferred version that is no greater than the server's preferred version.
                final Integer newPreference = negotiator.getPreferredVersion(newVersion);
                // If we could not agree with server on a version, fail now.
                if (newPreference == null) {
                    throw new HandshakeException("Could not agree on protocol version");
                }

                negotiator.setVersion(newPreference);

                // Attempt negotiation of resource based on our new preferred version.
                initiateVersionNegotiation(negotiator, dis, dos);
                return;
            case ABORT:
                throw new HandshakeException("Remote destination aborted connection with message: " + dis.readUTF());
            default:
                throw new HandshakeException("Received unexpected response code " + statusCode + " when negotiating version with remote server");
        }
    }

    private static void receiveVersionNegotiation(final VersionNegotiator negotiator, final DataInputStream dis, final DataOutputStream dos) throws IOException, HandshakeException {
        final int version = dis.readInt();
        if (negotiator.isVersionSupported(version)) {
            dos.write(RESOURCE_OK);
            dos.flush();

            negotiator.setVersion(version);
        } else {
            final Integer preferred = negotiator.getPreferredVersion(version);
            if (preferred == null) {
                dos.write(ABORT);
                dos.flush();
                throw new HandshakeException("Unable to negotiate an acceptable version of the Distributed Cache Protocol");
            }
            dos.write(DIFFERENT_RESOURCE_VERSION);
            dos.writeInt(preferred);
            dos.flush();

            receiveVersionNegotiation(negotiator, dis, dos);
        }
    }
}
