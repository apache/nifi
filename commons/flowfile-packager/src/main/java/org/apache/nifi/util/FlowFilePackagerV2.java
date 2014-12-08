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

/**
 * <p>
 * Packages a FlowFile, including both its content and its attributes into a
 * single file that is stream-friendly. The encoding scheme is as such:
 * </p>
 *
 * <pre>
 * Length Field : indicates the number of Flow File Attributes in the stream
 * 1 to N times (N=number of Flow File Attributes):
 *      String Field : Flow File Attribute key name
 *      String Field : Flow File Attribute value
 * Long : 8 bytes indicating the length of the Flow File content
 * Content : The next M bytes are the content of the Flow File.
 * </pre>
 *
 * <pre>
 * Encoding of String Field is as follows:
 *      Length Field : indicates the length of the String
 *      1 to N bytes (N=String length, determined by previous field, as described above) : The UTF-8 encoded string value.
 * </pre>
 *
 * <pre>
 * Encoding of Length Field is as follows:
 *      First 2 bytes: Indicate length. If both bytes = 255, this is a special value indicating that the length is
 *                     greater than or equal to 65536 bytes; therefore, the next 4 bytes will indicate the actual length.
 * </pre>
 *
 * <p>
 * Note: All byte-order encoding is Network Byte Order (Most Significant Byte
 * first)
 * </p>
 *
 * <p>
 * The following example shows the bytes expected if we were to encode a
 * FlowFile containing the following attributes where the content is the text
 * "Hello World!":
 *
 * <br><br>
 * Attributes:
 * <pre>
 * +-------+-------+
 * | Key   + Value |
 * + --------------+
 * | A     | a     |
 * + --------------+
 * | B     | b     |
 * + --------------+
 * </pre> Content:<br>
 * Hello World!
 * <br><br>
 * Packaged Byte Encoding (In Hexadecimal Form):
 * <p>
 *
 * <pre>
 * 00 02 00 01 41 00 01 61
 * 00 01 42 00 01 62 00 00
 * 00 00 00 00 00 0C 48 65
 * 6C 6C 6F 20 57 6F 72 6C
 * 64 21
 * </pre>
 */
public class FlowFilePackagerV2 implements FlowFilePackager {

    private static final int MAX_VALUE_2_BYTES = 65535;
    private final byte[] writeBuffer = new byte[8];

    @Override
    public void packageFlowFile(final InputStream in, final OutputStream out, final Map<String, String> attributes, final long fileSize) throws IOException {
        writeFieldLength(out, attributes.size()); //write out the number of attributes
        for (final Map.Entry<String, String> entry : attributes.entrySet()) { //write out each attribute key/value pair
            writeString(entry.getKey(), out);
            writeString(entry.getValue(), out);
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
