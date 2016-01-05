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

import org.apache.commons.lang3.StringUtils;

/**
 * A RELP frame received from a channel.
 */
public class RELPFrame {

    public static final byte DELIMITER = 10;
    public static final byte SEPARATOR = 32;

    private final long txnr;
    private final int dataLength;
    private final String command;
    private final byte[] data;

    private RELPFrame(final Builder builder) {
        this.txnr = builder.txnr;
        this.dataLength = builder.dataLength;
        this.command = builder.command;
        this.data = builder.data == null ? new byte[0] : builder.data;

        if (txnr < 0 || dataLength < 0 || command == null || StringUtils.isBlank(command)
                || data == null || dataLength != data.length) {
            throw new RELPFrameException("Invalid Frame");
        }
    }

    public long getTxnr() {
        return txnr;
    }

    public int getDataLength() {
        return dataLength;
    }

    public String getCommand() {
        return command;
    }

    //NOTE: consider making a copy here if we want to be truly be immutable
    public byte[] getData() {
        return data;
    }


    /**
     * Builder for a RELPFrame.
     */
    public static class Builder {

        long txnr;
        int dataLength;
        String command;
        byte[] data;

        public Builder() {
            reset();
        }

        public void reset() {
            txnr = -1;
            dataLength = -1;
            command = null;
            data = null;
        }

        public Builder txnr(final long txnr) {
            this.txnr = txnr;
            return this;
        }

        public Builder dataLength(final int dataLength) {
            this.dataLength = dataLength;
            return this;
        }

        public Builder command(final String command) {
            this.command = command;
            return this;
        }

        public Builder data(final byte[] data) {
            this.data = data;
            return this;
        }

        public RELPFrame build() {
            return new RELPFrame(this);
        }
    }

}
