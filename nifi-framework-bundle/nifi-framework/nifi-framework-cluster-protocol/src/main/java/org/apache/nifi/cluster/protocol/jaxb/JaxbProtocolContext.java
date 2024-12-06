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
package org.apache.nifi.cluster.protocol.jaxb;

import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolMessageMarshaller;
import org.apache.nifi.cluster.protocol.ProtocolMessageUnmarshaller;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.stream.StandardXMLStreamReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLStreamReaderProvider;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Implements a context for communicating internally amongst the cluster using
 * JAXB.
 *
 * @param <T> The type of protocol message.
 *
 */
public class JaxbProtocolContext<T> implements ProtocolContext<T> {

    private static final int BUF_SIZE = (int) Math.pow(2, 10);  // 1k

    /*
     * A sentinel is used to detect corrupted messages.  Relying on the integrity
     * of the message size can cause memory issues if the value is corrupted
     * and equal to a number larger than the memory size.
     */
    private static final byte MESSAGE_PROTOCOL_START_SENTINEL = 0x5A;

    private final JAXBContext jaxbCtx;

    public JaxbProtocolContext(final JAXBContext jaxbCtx) {
        this.jaxbCtx = jaxbCtx;
    }

    @Override
    public ProtocolMessageMarshaller<T> createMarshaller() {
        return (msg, os) -> {

            try {

                // marshal message to output stream
                final Marshaller marshaller = jaxbCtx.createMarshaller();
                final ByteArrayOutputStream msgBytes = new ByteArrayOutputStream();
                marshaller.marshal(msg, msgBytes);

                final DataOutputStream dos = new DataOutputStream(os);

                // write message protocol sentinel
                dos.write(MESSAGE_PROTOCOL_START_SENTINEL);

                // write message size in bytes
                dos.writeInt(msgBytes.size());

                // write message
                msgBytes.writeTo(dos);

                dos.flush();

            } catch (final JAXBException je) {
                throw new IOException("Failed marshalling protocol message due to: " + je, je);
            }

        };
    }

    @Override
    public ProtocolMessageUnmarshaller<T> createUnmarshaller() {
        return is -> {

            try {

                final DataInputStream dis = new DataInputStream(is);

                // check for the presence of the message protocol sentinel
                final byte sentinel = (byte) dis.read();
                if (sentinel == -1) {
                    throw new EOFException();
                }

                if (MESSAGE_PROTOCOL_START_SENTINEL != sentinel) {
                    throw new IOException("Failed reading protocol message due to malformed header");
                }

                // read the message size
                final int msgBytesSize = dis.readInt();

                // read the message
                final ByteBuffer buffer = ByteBuffer.allocate(msgBytesSize);
                int totalBytesRead = 0;
                do {
                    final int bytesToRead = Math.min((msgBytesSize - totalBytesRead), BUF_SIZE);
                    totalBytesRead += dis.read(buffer.array(), totalBytesRead, bytesToRead);
                } while (totalBytesRead < msgBytesSize);

                // unmarshall message and return
                final Unmarshaller unmarshaller = jaxbCtx.createUnmarshaller();
                final byte[] msg = new byte[totalBytesRead];
                buffer.get(msg);
                final XMLStreamReaderProvider provider = new StandardXMLStreamReaderProvider();
                final XMLStreamReader xsr = provider.getStreamReader(new StreamSource(new ByteArrayInputStream(msg)));
                return (T) unmarshaller.unmarshal(xsr);

            } catch (final JAXBException | ProcessingException e) {
                throw new IOException("Failed unmarshalling protocol message due to: " + e, e);
            }

        };
    }
}
