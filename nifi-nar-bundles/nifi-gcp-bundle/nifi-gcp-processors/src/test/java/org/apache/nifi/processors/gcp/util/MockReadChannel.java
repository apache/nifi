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
package org.apache.nifi.processors.gcp.util;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;

import java.nio.ByteBuffer;

public class MockReadChannel implements ReadChannel {
    private final byte[] toRead;
    private int position = 0;
    private boolean finished;
    private boolean isOpen;

    public MockReadChannel(String textToRead) {
        this.toRead = textToRead.getBytes();
        this.isOpen = true;
        this.finished = false;
    }

    @Override
    public void close() {
        this.isOpen = false;
    }

    @Override
    public void seek(long l) {}

    @Override
    public void setChunkSize(int i) {}

    @Override
    public RestorableState<ReadChannel> capture() {
        return null;
    }

    @Override
    public int read(ByteBuffer dst) {
        if (this.finished) {
            return -1;
        } else {
            if (dst.remaining() > this.toRead.length) {
                this.finished = true;
            }
            int toWrite = Math.min(this.toRead.length - position, dst.remaining());

            dst.put(this.toRead, this.position, toWrite);
            this.position += toWrite;

            return toWrite;
        }
    }

    @Override
    public boolean isOpen() {
        return this.isOpen;
    }
}
