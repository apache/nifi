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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

public class FlowFileUnpackagerV1 implements FlowFileUnpackager {

    private int flowFilesRead = 0;

    @Override
    public Map<String, String> unpackageFlowFile(final InputStream in, final OutputStream out) throws IOException {
        flowFilesRead++;
        final TarArchiveInputStream tarIn = new TarArchiveInputStream(in);
        final TarArchiveEntry attribEntry = tarIn.getNextTarEntry();
        if (attribEntry == null) {
            return null;
        }

        final Map<String, String> attributes;
        if (attribEntry.getName().equals(FlowFilePackagerV1.FILENAME_ATTRIBUTES)) {
            attributes = getAttributes(tarIn);
        } else {
            throw new IOException("Expected two tar entries: "
                    + FlowFilePackagerV1.FILENAME_CONTENT + " and "
                    + FlowFilePackagerV1.FILENAME_ATTRIBUTES);
        }

        final TarArchiveEntry contentEntry = tarIn.getNextTarEntry();

        if (contentEntry != null && contentEntry.getName().equals(FlowFilePackagerV1.FILENAME_CONTENT)) {
            final byte[] buffer = new byte[512 << 10];//512KB            
            int bytesRead = 0;
            while ((bytesRead = tarIn.read(buffer)) != -1) { //still more data to read
                if (bytesRead > 0) {
                    out.write(buffer, 0, bytesRead);
                }
            }
            out.flush();
        } else {
            throw new IOException("Expected two tar entries: "
                    + FlowFilePackagerV1.FILENAME_CONTENT + " and "
                    + FlowFilePackagerV1.FILENAME_ATTRIBUTES);
        }

        return attributes;
    }

    protected Map<String, String> getAttributes(final TarArchiveInputStream stream) throws IOException {

        final Properties props = new Properties();
        props.loadFromXML(new NonCloseableInputStream(stream));

        final Map<String, String> result = new HashMap<>();
        for (final Entry<Object, Object> entry : props.entrySet()) {
            final Object keyObject = entry.getKey();
            final Object valueObject = entry.getValue();
            if (!(keyObject instanceof String)) {
                throw new IOException("Flow file attributes object contains key of type "
                        + keyObject.getClass().getCanonicalName()
                        + " but expected java.lang.String");
            } else if (!(keyObject instanceof String)) {
                throw new IOException("Flow file attributes object contains value of type "
                        + keyObject.getClass().getCanonicalName()
                        + " but expected java.lang.String");
            }

            final String key = (String) keyObject;
            final String value = (String) valueObject;
            result.put(key, value);
        }

        return result;
    }

    @Override
    public boolean hasMoreData() throws IOException {
        return flowFilesRead == 0;
    }

    public static final class NonCloseableInputStream extends InputStream {

        final InputStream stream;

        public NonCloseableInputStream(final InputStream stream) {
            this.stream = stream;
        }

        @Override
        public void close() {
        }

        @Override
        public int read() throws IOException {
            return stream.read();
        }

        @Override
        public int available() throws IOException {
            return stream.available();
        }

        @Override
        public synchronized void mark(int readlimit) {
            stream.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            stream.reset();
        }

        @Override
        public boolean markSupported() {
            return stream.markSupported();
        }

        @Override
        public long skip(long n) throws IOException {
            return stream.skip(n);
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            return stream.read(b, off, len);
        }

        @Override
        public int read(byte b[]) throws IOException {
            return stream.read(b);
        }
    }
}
