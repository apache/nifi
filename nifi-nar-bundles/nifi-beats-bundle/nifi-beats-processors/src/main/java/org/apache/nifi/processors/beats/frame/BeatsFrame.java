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
package org.apache.nifi.processors.beats.frame;


/**
 * A frame received from a channel.
 */
public class BeatsFrame {

    public static final byte DELIMITER = 10;

    private final byte version;
    private final byte frameType;
    private final byte[] payload;
    private final long dataSize;
    private final long seqNumber;

    private BeatsFrame(final Builder builder) {
        this.version = builder.version;
        this.frameType = builder.frameType;
        this.payload = builder.payload;
        this.dataSize = builder.dataSize;
        this.seqNumber = builder.seqNumber;

        if (version < 2 ||  payload.length < 0 ) {
            throw new BeatsFrameException("Invalid Frame");
        }
    }

    public long getSeqNumber() {
        return seqNumber;
    }

    public byte getVersion() {
        return version;
    }

    public byte getFrameType() {
        return frameType;
    }

    public byte [] getPayload() {
        return payload;
    }

    /**
     * Builder for a BeatsFrame.
     */
    public static class Builder {

        byte version;
        byte frameType;
        byte [] payload;
        long dataSize;
        int seqNumber;

        public Builder() {
            reset();
        }

        public void reset() {
            version = -1;
            seqNumber = -1;
            frameType = -1;
            payload = null;
        }

        public Builder version(final byte version) {
            this.version = version;
            return this;
        }

        public Builder seqNumber(final int seqNumber) {
            this.seqNumber = seqNumber;
            return this;
        }

        public Builder frameType(final byte frameType) {
            this.frameType = frameType;
            return this;
        }

        public Builder dataSize(final long dataSize) {
            this.dataSize = dataSize;
            return this;
        }

        public Builder payload(final byte [] payload) {
            this.payload = payload;
            return this;
        }


        public BeatsFrame build() {
            return new BeatsFrame(this);
        }

    }

}