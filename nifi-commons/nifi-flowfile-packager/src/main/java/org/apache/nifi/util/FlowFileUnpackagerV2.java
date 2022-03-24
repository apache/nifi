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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class FlowFileUnpackagerV2 implements FlowFileUnpackager {

    private final byte readBuffer[] = new byte[8192];
    private Map<String, String> nextAttributes = null;
    private boolean haveReadSomething = false;

    @Override
    public boolean hasMoreData() throws IOException {
        return nextAttributes != null || !haveReadSomething;
    }

    protected Map<String, String> readAttributes(final InputStream in) throws IOException {
        final Map<String, String> attributes = new HashMap<>();
        final Integer numAttributes = readFieldLength(in); //read number of attributes
        if (numAttributes == null) {
            return null;
        }
        if (numAttributes == 0) {
            throw new IOException("flow files cannot have zero attributes");
        }
        for (int i = 0; i < numAttributes; i++) { //read each attribute key/value pair
            final String key = readString(in);
            final String value = readString(in);
            attributes.put(key, value);
        }

        return attributes;
    }

    @Override
    public Map<String, String> unpackageFlowFile(final InputStream in, final OutputStream out) throws IOException {
        final Map<String, String> attributes;
        if (nextAttributes != null) {
            attributes = nextAttributes;
        } else {
            attributes = readAttributes(in);
        }

        final long expectedNumBytes = readLong(in); // read length of payload
        copy(in, out, expectedNumBytes); // read payload

        nextAttributes = readAttributes(in);
        haveReadSomething = true;

        return attributes;
    }

    protected String readString(final InputStream in) throws IOException {
        final Integer numBytes = readFieldLength(in);
        if (numBytes == null) {
            throw new EOFException();
        }
        final byte[] bytes = new byte[numBytes];
        fillBuffer(in, bytes, numBytes);
        return new String(bytes, "UTF-8");
    }

    private void fillBuffer(final InputStream in, final byte[] buffer, final int length) throws IOException {
        int bytesRead;
        int totalBytesRead = 0;
        while ((bytesRead = in.read(buffer, totalBytesRead, length - totalBytesRead)) > 0) {
            totalBytesRead += bytesRead;
        }
        if (totalBytesRead != length) {
            throw new EOFException();
        }
    }

    protected long copy(final InputStream in, final OutputStream out, final long numBytes) throws IOException {
        int bytesRead;
        long totalBytesRead = 0L;
        while ((bytesRead = in.read(readBuffer, 0, (int) Math.min(readBuffer.length, numBytes - totalBytesRead))) > 0) {
            out.write(readBuffer, 0, bytesRead);
            totalBytesRead += bytesRead;
        }

        if (totalBytesRead < numBytes) {
            throw new EOFException("Expected " + numBytes + " but received " + totalBytesRead);
        }

        return totalBytesRead;
    }

    protected long readLong(final InputStream in) throws IOException {
        fillBuffer(in, readBuffer, 8);
        return (((long) readBuffer[0] << 56)
                + ((long) (readBuffer[1] & 255) << 48)
                + ((long) (readBuffer[2] & 255) << 40)
                + ((long) (readBuffer[3] & 255) << 32)
                + ((long) (readBuffer[4] & 255) << 24)
                + ((readBuffer[5] & 255) << 16)
                + ((readBuffer[6] & 255) << 8)
                + ((readBuffer[7] & 255)));
    }

    private Integer readFieldLength(final InputStream in) throws IOException {
        final int firstValue = in.read();
        final int secondValue = in.read();
        if (firstValue < 0) {
            return null;
        }
        if (secondValue < 0) {
            throw new EOFException();
        }
        if (firstValue == 0xff && secondValue == 0xff) {
            int ch1 = in.read();
            int ch2 = in.read();
            int ch3 = in.read();
            int ch4 = in.read();
            if ((ch1 | ch2 | ch3 | ch4) < 0) {
                throw new EOFException();
            }
            return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
        } else {
            return ((firstValue << 8) + (secondValue));
        }
    }
}
