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
package org.apache.nifi.processors.hadoop.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Writable;

/**
 * Simple implementation of {@link Writable} that writes data from an
 * InputStream. This class will throw an
 * <tt>UnsupportedOperationException</tt> if {@link #readFields(DataInput)} is
 * called.
 */
public class InputStreamWritable implements Writable {

    private final InputStream inStream;
    private final int size;

    public InputStreamWritable(final InputStream inStream, final int size) {
        this.inStream = inStream;
        this.size = size;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        int numRead, totalRead = 0;
        final byte[] buffer = new byte[8192];
        output.writeInt(size);

        int tempSize = size == -1 ? Integer.MAX_VALUE : size;
        int toRead = Math.min(tempSize, 8192);
        while (totalRead < tempSize && (numRead = inStream.read(buffer, 0, toRead)) != -1) {
            output.write(buffer, 0, numRead);
            totalRead += numRead;
            toRead = Math.min(8192, tempSize - totalRead);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        throw new UnsupportedOperationException("InputStreamWritable does not implement #readFields");
    }
}
