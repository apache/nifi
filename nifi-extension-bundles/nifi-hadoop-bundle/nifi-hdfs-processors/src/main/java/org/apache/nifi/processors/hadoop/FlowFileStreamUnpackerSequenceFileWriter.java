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
package org.apache.nifi.processors.hadoop;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.hadoop.util.InputStreamWritable;
import org.apache.nifi.util.FlowFilePackagerV3;
import org.slf4j.LoggerFactory;

public class FlowFileStreamUnpackerSequenceFileWriter extends SequenceFileWriterImpl {

    static {
        logger = LoggerFactory.getLogger(FlowFileStreamUnpackerSequenceFileWriter.class);
    }

    @Override
    protected void processInputStream(final InputStream stream, final FlowFile flowFileStreamPackedFlowFile, final Writer writer) throws IOException {
        final FlowFileUnpackager unpackager = new FlowFileUnpackager();
        try (final InputStream in = new BufferedInputStream(stream)) {
            while (unpackager.hasMoreData()) {
                unpackager.unpackageFlowFile(stream, writer);
            }
        }
    }

    private static class FlowFileUnpackager {

        private byte[] nextHeader = null;
        private boolean haveReadSomething = false;
        private final byte readBuffer[] = new byte[8192];

        public boolean hasMoreData() {
            return nextHeader != null || !haveReadSomething;
        }

        private byte[] readHeader(final InputStream in) throws IOException {
            final byte[] header = new byte[FlowFilePackagerV3.MAGIC_HEADER.length];
            for (int i = 0; i < header.length; i++) {
                final int next = in.read();
                if (next < 0) {
                    if (i == 0) {
                        return null;
                    }

                    throw new IOException("Not in FlowFile-v3 format");
                }
                header[i] = (byte) (next & 0xFF);
            }

            return header;
        }

        public void unpackageFlowFile(final InputStream in, final Writer writer) throws IOException {
            final byte[] header = (nextHeader == null) ? readHeader(in) : nextHeader;
            if (!Arrays.equals(header, FlowFilePackagerV3.MAGIC_HEADER)) {
                throw new IOException("Not in FlowFile-v3 format");
            }

            final Map<String, String> attributes = readAttributes(in);
            final long expectedNumBytes = readLong(in); // read length of payload
            final InputStreamWritable inStreamWritable = new InputStreamWritable(in, (int) expectedNumBytes);
            String fileName = attributes.get(CoreAttributes.FILENAME.key());
            Text key = new Text(fileName);
            writer.append(key, inStreamWritable);
            nextHeader = readHeader(in);
            haveReadSomething = true;

        }

        protected Map<String, String> readAttributes(final InputStream in) throws IOException {
            final Map<String, String> attributes = new HashMap<>();
            final Integer numAttributes = readFieldLength(in); // read number of attributes
            if (numAttributes == null) {
                return null;
            }
            if (numAttributes == 0) {
                throw new IOException("flow files cannot have zero attributes");
            }
            for (int i = 0; i < numAttributes; i++) { // read each attribute key/value pair
                final String key = readString(in);
                final String value = readString(in);
                attributes.put(key, value);
            }

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
            }
            return ((firstValue << 8) + (secondValue));
        }

    }
}
