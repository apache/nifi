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
package org.apache.nifi.distributed.cache.client.adapter;

import org.apache.nifi.distributed.cache.client.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

/**
 * Interface to write service request payloads to a {@link io.netty.channel.Channel}.
 */
public class OutboundAdapter {

    private static final int NO_MINIMUM_VERSION = 0;

    /**
     * Minimum version required for the invocation of the specified API.
     */
    private int minimumVersion;

    /**
     * Container for service request payload.
     */
    private final ByteArrayOutputStream bos;

    /**
     * Wrapper class for crafting service request payload.
     */
    private final DataOutputStream dos;

    /**
     * Constructor.
     */
    public OutboundAdapter() {
        minimumVersion = NO_MINIMUM_VERSION;
        bos = new ByteArrayOutputStream();
        dos = new DataOutputStream(bos);
    }

    public int getMinimumVersion() {
        return minimumVersion;
    }

    public OutboundAdapter minimumVersion(final int minimumVersion) {
        this.minimumVersion = minimumVersion;
        return this;
    }

    /**
     * Write an integer to the service request payload.
     *
     * @param value the value to be written
     * @return this object (allow chaining of calls to assemble payload)
     * @throws IOException on write failure
     */
    public OutboundAdapter write(final int value) throws IOException {
        dos.writeInt(value);
        return this;
    }

    /**
     * Write a long to the service request payload.
     *
     * @param value the value to be written
     * @return this object (allow chaining of calls to assemble payload)
     * @throws IOException on write failure
     */
    public OutboundAdapter write(final long value) throws IOException {
        dos.writeLong(value);
        return this;
    }

    /**
     * Write a string to the service request payload.
     *
     * @param value the value to be written
     * @return this object (allow chaining of calls to assemble payload)
     * @throws IOException on write failure
     */
    public OutboundAdapter write(final String value) throws IOException {
        dos.writeUTF(value);
        return this;
    }

    /**
     * Write bytes to the service request payload.
     *
     * @param value the bytes to be written
     * @return this object (allow chaining of calls to assemble payload)
     * @throws IOException on write failure
     */
    public OutboundAdapter write(final byte[] value) throws IOException {
        dos.writeInt(value.length);
        dos.write(value);
        return this;
    }

    /**
     * Write bytes to the service request payload.
     *
     * @param values the bytes to be written
     * @return this object (allow chaining of calls to assemble payload)
     * @throws IOException on write failure
     */
    public OutboundAdapter write(final Collection<byte[]> values) throws IOException {
        dos.writeInt(values.size());
        for (byte[] value : values) {
            write(value);
        }
        return this;
    }

    /**
     * Write an arbitrary value to the service request payload, using the provided serializer.
     *
     * @param value      the value to be written
     * @param serializer the method by which the value is to be serialized
     * @return this object (allow chaining of calls to assemble payload)
     * @throws IOException on write failure
     */
    public <T> OutboundAdapter write(T value, Serializer<T> serializer) throws IOException {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        serializer.serialize(value, os);
        dos.writeInt(os.size());
        dos.write(os.toByteArray());
        return this;
    }

    /**
     * Assemble the request byte stream to be written to the service {@link io.netty.channel.Channel}.
     *
     * @return the request bytes to be written
     * @throws IOException on write failure
     */
    public byte[] toBytes() throws IOException {
        dos.flush();
        return bos.toByteArray();
    }
}
