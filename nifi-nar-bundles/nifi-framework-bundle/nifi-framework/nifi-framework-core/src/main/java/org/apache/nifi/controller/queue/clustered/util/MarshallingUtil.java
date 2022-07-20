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
package org.apache.nifi.controller.queue.clustered.util;

import org.apache.nifi.controller.queue.clustered.client.async.nio.PeerChannel;
import org.apache.nifi.controller.queue.clustered.dto.PartitionStatus;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class MarshallingUtil {

    private static byte[] marshallLong(final long number) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(bos);
        dos.writeLong(number);
        dos.flush();
        return bos.toByteArray();
    }

    private static byte[] marshallInt(final int number) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(bos);
        dos.writeInt(number);
        dos.flush();
        return bos.toByteArray();
    }

    private static long unmarshallLong(final byte[] payload) {
        assert(payload.length == Long.BYTES);
        final ByteBuffer bb = ByteBuffer.wrap(payload);
        return bb.getLong();
    }

    private static int unmarshallInt(final byte[] payload) {
        assert(payload.length == Integer.BYTES);
        final ByteBuffer bb = ByteBuffer.wrap(payload);
        return bb.getInt();
    }

    /**
     * Reads the necessary amount of bytes from the {@code PeerChannel} which represents a {@code PartitionStatus} and
     * unmarshalls it. The expected order of the fields in the channel is: totalSizeBytes, objectCount, flowFilesOut.
     *
     * @param channel The channel to read from.
     * @return The unmarshalled {@code PartitionStatus} instance.
     *
     * @throws IOException Thrown in case of error during the unmarshalling or reading.
     */
    public static PartitionStatus readPartitionStatus(final PeerChannel channel) throws IOException {
        final long totalSizeBytes = MarshallingUtil.unmarshallLong(channel.readBytes(Long.BYTES));
        final int objectCount = MarshallingUtil.unmarshallInt(channel.readBytes(Integer.BYTES));
        final int flowFilesOut = MarshallingUtil.unmarshallInt(channel.readBytes(Integer.BYTES));
        return new PartitionStatus(objectCount, totalSizeBytes, flowFilesOut);
    }

    /**
     * Marshalls and writes the values represents a partition's load balancing status into an output stream. The correct
     * order of writing is specified by the load balancing protocol and is: totalSizeBytes, objectCount, flowFilesOut. Readers
     * must expect this order when working with a partition status.
     *
     * @param out Output stream to write in.
     * @param status The status to write in.
     *
     * @throws IOException Thrown in case of error during the marshalling or writing.
     */
    public static void writePartitionStatus(final OutputStream out, final PartitionStatus status) throws IOException {
        out.write(MarshallingUtil.marshallLong(status.getTotalSizeBytes()));
        out.write(MarshallingUtil.marshallInt(status.getObjectCount()));
        out.write(MarshallingUtil.marshallInt(status.getFlowFilesOut()));
        out.flush();
    }
}
