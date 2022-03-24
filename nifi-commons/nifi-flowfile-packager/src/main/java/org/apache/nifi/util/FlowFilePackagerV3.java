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
package org.apache.nifi.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class FlowFilePackagerV3 implements FlowFilePackager {

    public static final byte[] MAGIC_HEADER = {'N', 'i', 'F', 'i', 'F', 'F', '3'};
    private static final int MAX_VALUE_2_BYTES = 65535;
    private final byte[] writeBuffer = new byte[8];

    @Override
    public void packageFlowFile(final InputStream in, final OutputStream out, final Map<String, String> attributes, final long fileSize) throws IOException {
        out.write(MAGIC_HEADER);

        if (attributes == null) {
            writeFieldLength(out, 0);
        } else {
            writeFieldLength(out, attributes.size()); //write out the number of attributes
            for (final Map.Entry<String, String> entry : attributes.entrySet()) { //write out each attribute key/value pair
                writeString(entry.getKey(), out);
                writeString(entry.getValue(), out);
            }
        }

        writeLong(out, fileSize);//write out length of data
        copy(in, out);//write out the actual flow file payload
    }

    private void copy(final InputStream in, final OutputStream out) throws IOException {
        final byte[] buffer = new byte[65536];
        int len;
        while ((len = in.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }
    }

    private void writeString(final String val, final OutputStream out) throws IOException {
        final byte[] bytes = val.getBytes("UTF-8");
        writeFieldLength(out, bytes.length);
        out.write(bytes);
    }

    private void writeFieldLength(final OutputStream out, final int numBytes) throws IOException {
        // If the value is less than the max value that can be fit into 2 bytes, just use the
        // actual value. Otherwise, we will set the first 2 bytes to 255/255 and then use the next
        // 4 bytes to indicate the real length.
        if (numBytes < MAX_VALUE_2_BYTES) {
            writeBuffer[0] = (byte) (numBytes >>> 8);
            writeBuffer[1] = (byte) (numBytes);
            out.write(writeBuffer, 0, 2);
        } else {
            writeBuffer[0] = (byte) 0xff;
            writeBuffer[1] = (byte) 0xff;
            writeBuffer[2] = (byte) (numBytes >>> 24);
            writeBuffer[3] = (byte) (numBytes >>> 16);
            writeBuffer[4] = (byte) (numBytes >>> 8);
            writeBuffer[5] = (byte) (numBytes);
            out.write(writeBuffer, 0, 6);
        }
    }

    private void writeLong(final OutputStream out, final long val) throws IOException {
        writeBuffer[0] = (byte) (val >>> 56);
        writeBuffer[1] = (byte) (val >>> 48);
        writeBuffer[2] = (byte) (val >>> 40);
        writeBuffer[3] = (byte) (val >>> 32);
        writeBuffer[4] = (byte) (val >>> 24);
        writeBuffer[5] = (byte) (val >>> 16);
        writeBuffer[6] = (byte) (val >>> 8);
        writeBuffer[7] = (byte) (val);
        out.write(writeBuffer, 0, 8);
    }

}
