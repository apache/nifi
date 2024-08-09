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

package org.apache.nifi.python.processor;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class StandardInputFlowFile implements InputFlowFile, Closeable {
    private final ProcessSession session;
    private final FlowFile flowFile;
    private BufferedReader bufferedReader;

    public StandardInputFlowFile(final ProcessSession session, final FlowFile flowFile) {
        this.session = session;
        this.flowFile = flowFile;
    }

    public byte[] getContentsAsBytes() throws IOException {
        if (flowFile.getSize() > Integer.MAX_VALUE) {
            throw new IOException("Cannot read FlowFile contents into a byte array because size is " + flowFile.getSize() +
                ", which exceeds the maximum allowable size of " + Integer.MAX_VALUE + " bytes");
        }

        try (final InputStream in = session.read(flowFile)) {
            final byte[] bytes = new byte[(int) flowFile.getSize()];
            StreamUtils.fillBuffer(in, bytes);
            return bytes;
        }
    }

    public BufferedReader getContentsAsReader() throws IOException {
        close();

        final InputStream in = session.read(flowFile);
        final InputStreamReader reader = new InputStreamReader(in);
        this.bufferedReader = new BufferedReader(reader);
        return bufferedReader;
    }

    @Override
    public void close() throws IOException {
        if (bufferedReader != null) {
            bufferedReader.close();
        }
    }

    public long getSize() {
        return flowFile.getSize();
    }

    public String getAttribute(final String name) {
        return flowFile.getAttribute(name);
    }

    public Map<String, String> getAttributes() {
        return flowFile.getAttributes();
    }

    @Override
    public String toString() {
        return "FlowFile[id=" + getAttribute("uuid") + ", filename=" + getAttribute("filename") + ", size=" + getSize() + "]";
    }
}