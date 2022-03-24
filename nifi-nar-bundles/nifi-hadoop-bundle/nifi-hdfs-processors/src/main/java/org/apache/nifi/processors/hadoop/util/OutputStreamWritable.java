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

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

/**
 * This class will write to an output stream, rather than an in-memory buffer, the fields being read.
 *
 *
 */
public class OutputStreamWritable implements Writable {

    private static final int BUFFER_SIZE = 16384;
    private final DataOutputStream dos;
    private final boolean writeLength;

    public OutputStreamWritable(OutputStream out, boolean writeLength) {
        this.dos = new DataOutputStream(new BufferedOutputStream(out, BUFFER_SIZE));
        this.writeLength = writeLength;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("OutputStreamWritable does not implement #write");
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        if (in instanceof DataInputBuffer) {
            byte[] bytes = ((DataInputBuffer) in).getData();
            int pos = ((DataInputBuffer) in).getPosition();
            int length = ((DataInputBuffer) in).getLength();
            int bytesRemaining = length - pos;
            if (writeLength) {
                dos.write(bytes, pos, bytesRemaining);
            } else {
                dos.write(bytes, pos + 4, bytesRemaining - 4);
            }
            in.skipBytes(bytesRemaining);
        } else {
            int length = in.readInt();
            if (writeLength) {
                dos.writeInt(length);
            }
            int numRead = 0;
            int toRead = Math.min(BUFFER_SIZE, length);
            byte[] buffer = new byte[BUFFER_SIZE];
            while (toRead > 0) {
                in.readFully(buffer, 0, toRead);
                dos.write(buffer, 0, toRead);
                numRead += toRead;
                toRead = Math.min(BUFFER_SIZE, length - numRead);
            }
        }
        dos.flush();
    }

}
