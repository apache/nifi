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

import org.apache.nifi.remote.io.InterruptableOutputStream;
import org.apache.nifi.remote.protocol.CommunicationsOutput;
import org.apache.nifi.stream.io.ByteCountingOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class HttpOutput implements CommunicationsOutput {

    private ByteCountingOutputStream countingOut;
    private InterruptableOutputStream interruptableOut;

    @Override
    public OutputStream getOutputStream() throws IOException {
        return countingOut;
    }

    @Override
    public long getBytesWritten() {
        if (countingOut != null) {
            return countingOut.getBytesWritten();
        }
        return 0L;
    }

    public void setOutputStream(OutputStream outputStream) {
        interruptableOut = new InterruptableOutputStream(outputStream);
        this.countingOut = new ByteCountingOutputStream(interruptableOut);
    }

    public void interrupt() {
        if (interruptableOut != null) {
            interruptableOut.interrupt();
        }
    }
}
