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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.StreamUtils;

public class StandardFlowFileCodec implements FlowFileCodec {

    public static final int MAX_NUM_ATTRIBUTES = 25000;

    public static final String DEFAULT_FLOWFILE_PATH = "./";

    private final VersionNegotiator versionNegotiator;

    public StandardFlowFileCodec() {
        versionNegotiator = new StandardVersionNegotiator(1);
    }

    @Override
    public void encode(final DataPacket dataPacket, final OutputStream encodedOut) throws IOException {
        final DataOutputStream out = new DataOutputStream(encodedOut);

        final Map<String, String> attributes = dataPacket.getAttributes();
        out.writeInt(attributes.size());
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            writeString(entry.getKey(), out);
            writeString(entry.getValue(), out);
        }

        out.writeLong(dataPacket.getSize());

        final InputStream in = dataPacket.getData();
        StreamUtils.copy(in, encodedOut);
        encodedOut.flush();
    }

    @Override
    public DataPacket decode(final InputStream stream) throws IOException, ProtocolException {
        final DataInputStream in = new DataInputStream(stream);

        final int numAttributes;
        try {
            numAttributes = in.readInt();
        } catch (final EOFException e) {
            // we're out of data.
            return null;
        }

        // This is here because if the stream is not properly formed, we could get up to Integer.MAX_VALUE attributes, which will
        // generally result in an OutOfMemoryError.
        if (numAttributes > MAX_NUM_ATTRIBUTES) {
            throw new ProtocolException("FlowFile exceeds maximum number of attributes with a total of " + numAttributes);
        }

        final Map<String, String> attributes = new HashMap<>(numAttributes);
        for (int i = 0; i < numAttributes; i++) {
            final String attrName = readString(in);
            final String attrValue = readString(in);
            attributes.put(attrName, attrValue);
        }

        final long numBytes = in.readLong();

        return new StandardDataPacket(attributes, stream, numBytes);
    }

    private void writeString(final String val, final DataOutputStream out) throws IOException {
        final byte[] bytes = val.getBytes("UTF-8");
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private String readString(final DataInputStream in) throws IOException {
        final int numBytes = in.readInt();
        final byte[] bytes = new byte[numBytes];
        StreamUtils.fillBuffer(in, bytes, true);
        return new String(bytes, "UTF-8");
    }

    @Override
    public List<Integer> getSupportedVersions() {
        return versionNegotiator.getSupportedVersions();
    }

    @Override
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }

    @Override
    public String toString() {
        return "Standard FlowFile Codec, Version " + versionNegotiator.getVersion();
    }

    @Override
    public String getResourceName() {
        return "StandardFlowFileCodec";
    }
}
