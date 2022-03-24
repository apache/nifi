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
package org.apache.nifi.remote.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.nifi.remote.VersionedRemoteResource;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.TransmissionDisabledException;
import org.apache.nifi.remote.protocol.DataPacket;

/**
 * <p>
 * Provides a mechanism for encoding and decoding FlowFiles as streams so that
 * they can be transferred remotely.
 * </p>
 */
public interface FlowFileCodec extends VersionedRemoteResource {

    /**
     * Returns a List of all versions that this codec is able to support, in the
     * order that they are preferred by the codec
     *
     * @return all supported versions
     */
    public List<Integer> getSupportedVersions();

    /**
     * Encodes a DataPacket and its content as a single stream of data and
     * writes that stream to the output.
     *
     * @param dataPacket the data to serialize
     * @param outStream the stream to write the data to
     *
     * @throws IOException if there is a communications issue
     * @throws TransmissionDisabledException if a user terminates the connection
     */
    void encode(DataPacket dataPacket, OutputStream outStream) throws IOException, TransmissionDisabledException;

    /**
     * Decodes the contents of the InputStream, interpreting the data to
     * determine the next DataPacket's attributes and content.
     *
     * @param stream an InputStream containing DataPacket's content and
     * attributes
     *
     * @return the DataPacket that was created, or <code>null</code> if the
     * stream was out of data
     *
     * @throws IOException if unable to read stream
     * @throws ProtocolException if the input is malformed
     * @throws TransmissionDisabledException if a user terminates the connection
     */
    DataPacket decode(InputStream stream) throws IOException, ProtocolException, TransmissionDisabledException;
}
