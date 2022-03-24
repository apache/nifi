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
package org.apache.nifi.remote;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.protocol.ClientProtocol;
import org.apache.nifi.remote.protocol.ServerProtocol;

public class RemoteResourceFactory extends RemoteResourceInitiator {

    @SuppressWarnings("unchecked")
    public static <T extends FlowFileCodec> T receiveCodecNegotiation(final DataInputStream dis, final DataOutputStream dos) throws IOException, HandshakeException {
        final String codecName = dis.readUTF();
        final int version = dis.readInt();

        final T codec = (T) RemoteResourceManager.createCodec(codecName, version);
        final VersionNegotiator negotiator = codec.getVersionNegotiator();
        if (negotiator.isVersionSupported(version)) {
            dos.write(RESOURCE_OK);
            dos.flush();

            negotiator.setVersion(version);
            return codec;
        } else {
            final Integer preferred = negotiator.getPreferredVersion(version);
            if (preferred == null) {
                dos.write(ABORT);
                dos.flush();
                throw new HandshakeException("Unable to negotiate an acceptable version of the FlowFileCodec " + codecName);
            }
            dos.write(DIFFERENT_RESOURCE_VERSION);
            dos.writeInt(preferred);
            dos.flush();

            return receiveCodecNegotiation(dis, dos);
        }
    }

    public static void rejectCodecNegotiation(final DataInputStream dis, final DataOutputStream dos, final String explanation) throws IOException {
        dis.readUTF();  // read codec name
        dis.readInt();  // read codec version

        dos.write(ABORT);
        dos.writeUTF(explanation);
        dos.flush();
    }

    @SuppressWarnings("unchecked")
    public static <T extends ClientProtocol> T receiveClientProtocolNegotiation(final DataInputStream dis, final DataOutputStream dos) throws IOException, HandshakeException {
        final String protocolName = dis.readUTF();
        final int version = dis.readInt();

        final T protocol = (T) RemoteResourceManager.createClientProtocol(protocolName);
        final VersionNegotiator negotiator = protocol.getVersionNegotiator();
        if (negotiator.isVersionSupported(version)) {
            dos.write(RESOURCE_OK);
            dos.flush();

            negotiator.setVersion(version);
            return protocol;
        } else {
            final Integer preferred = negotiator.getPreferredVersion(version);
            if (preferred == null) {
                dos.write(ABORT);
                dos.flush();
                throw new HandshakeException("Unable to negotiate an acceptable version of the ClientProtocol " + protocolName);
            }
            dos.write(DIFFERENT_RESOURCE_VERSION);
            dos.writeInt(preferred);
            dos.flush();

            return receiveClientProtocolNegotiation(dis, dos);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends ServerProtocol> T receiveServerProtocolNegotiation(final DataInputStream dis, final DataOutputStream dos) throws IOException, HandshakeException {
        final String protocolName = dis.readUTF();
        final int version = dis.readInt();

        final T protocol = (T) RemoteResourceManager.createServerProtocol(protocolName);
        final VersionNegotiator negotiator = protocol.getVersionNegotiator();
        if (negotiator.isVersionSupported(version)) {
            dos.write(RESOURCE_OK);
            dos.flush();

            negotiator.setVersion(version);
            return protocol;
        } else {
            final Integer preferred = negotiator.getPreferredVersion(version);
            if (preferred == null) {
                dos.write(ABORT);
                dos.flush();
                throw new HandshakeException("Unable to negotiate an acceptable version of the ServerProtocol " + protocolName);
            }
            dos.write(DIFFERENT_RESOURCE_VERSION);
            dos.writeInt(preferred);
            dos.flush();

            return receiveServerProtocolNegotiation(dis, dos);
        }
    }

    public static <T extends VersionedRemoteResource> T
            receiveResourceNegotiation(final Class<T> cls, final DataInputStream dis, final DataOutputStream dos, final Class<?>[] constructorArgClasses, final Object[] constructorArgs)
            throws IOException, HandshakeException {
        final String resourceClassName = dis.readUTF();
        final T resource;
        try {
            @SuppressWarnings("unchecked")
            final Class<T> resourceClass = (Class<T>) Class.forName(resourceClassName);
            if (!cls.isAssignableFrom(resourceClass)) {
                throw new HandshakeException("Expected to negotiate a Versioned Resource of type " + cls.getName() + " but received class name of " + resourceClassName);
            }

            final Constructor<T> ctr = resourceClass.getConstructor(constructorArgClasses);
            resource = ctr.newInstance(constructorArgs);
        } catch (final Throwable t) {
            dos.write(ABORT);
            final String errorMsg = "Unable to instantiate Versioned Resource of type " + resourceClassName;
            dos.writeUTF(errorMsg);
            dos.flush();
            throw new HandshakeException(errorMsg);
        }

        final int version = dis.readInt();
        final VersionNegotiator negotiator = resource.getVersionNegotiator();
        if (negotiator.isVersionSupported(version)) {
            dos.write(RESOURCE_OK);
            dos.flush();

            negotiator.setVersion(version);
            return resource;
        } else {
            final Integer preferred = negotiator.getPreferredVersion(version);
            if (preferred == null) {
                dos.write(ABORT);
                dos.flush();
                throw new HandshakeException("Unable to negotiate an acceptable version of the resource " + resourceClassName);
            }
            dos.write(DIFFERENT_RESOURCE_VERSION);
            dos.writeInt(preferred);
            dos.flush();

            return receiveResourceNegotiation(cls, dis, dos, constructorArgClasses, constructorArgs);
        }
    }
}
