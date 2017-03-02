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
package org.apache.nifi.processors.standard.relp.frame;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

/**
 * Encodes a RELPFrame into raw bytes using the given charset.
 */
public class RELPEncoder {

    private final Charset charset;

    public RELPEncoder(final Charset charset) {
        this.charset = charset;
    }

    public Charset getCharset() {
        return charset;
    }

    public byte[] encode(final RELPFrame frame) {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        // write transaction number followed by separator
        byte[] txnr = String.format("%s", frame.getTxnr()).getBytes(charset);
        buffer.write(txnr, 0, txnr.length);
        buffer.write(RELPFrame.SEPARATOR);

        // write the command followed by separator
        byte[] command = frame.getCommand().getBytes(charset);
        buffer.write(command, 0, command.length);
        buffer.write(RELPFrame.SEPARATOR);

        // write the data length
        byte[] dataLength = String.format("%s", frame.getDataLength()).getBytes(charset);
        buffer.write(dataLength, 0, dataLength.length);

        // if data to write then put a separator and write the data
        if (frame.getDataLength() > 0) {
            buffer.write(RELPFrame.SEPARATOR);
            buffer.write(frame.getData(), 0, frame.getDataLength());
        }

        // write the end of the frame
        buffer.write(RELPFrame.DELIMITER);

        return buffer.toByteArray();
    }

}
