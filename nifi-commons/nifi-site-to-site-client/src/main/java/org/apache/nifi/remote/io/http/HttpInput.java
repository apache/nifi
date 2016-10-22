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
package org.apache.nifi.remote.io.http;

import org.apache.nifi.remote.io.InterruptableInputStream;
import org.apache.nifi.remote.protocol.CommunicationsInput;
import org.apache.nifi.stream.io.ByteCountingInputStream;

import java.io.IOException;
import java.io.InputStream;

public class HttpInput implements CommunicationsInput {

    private ByteCountingInputStream countingIn;
    private InterruptableInputStream interruptableIn;

    @Override
    public InputStream getInputStream() throws IOException {
        return countingIn;
    }

    @Override
    public long getBytesRead() {
        if (countingIn != null) {
            return countingIn.getBytesRead();
        }
        return 0L;
    }

    @Override
    public void consume() throws IOException {
        if (countingIn == null) {
            return;
        }

        final byte[] b = new byte[4096];
        int bytesRead;
        do {
            bytesRead = countingIn.read(b);
        } while (bytesRead > 0);
    }

    public void setInputStream(InputStream inputStream) {
        interruptableIn = new InterruptableInputStream(inputStream);
        this.countingIn = new ByteCountingInputStream(interruptableIn);
    }

    public void interrupt() {
        if (interruptableIn != null) {
            interruptableIn.interrupt();
        }
    }
}
