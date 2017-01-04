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
package org.apache.nifi.processors.beats.response;

import org.apache.nifi.processors.beats.frame.BeatsFrame;

import java.nio.ByteBuffer;

/**
 'ack' frame type

 SENT FROM READER ONLY
 frame type value: ASCII 'A' aka byte value 0x41

 Payload:
 32bit unsigned sequence number.

 */
public class BeatsResponse {
    private final int seqNumber;
    final private byte version = 0x32; // v2
    final private byte frameType = 0x41; // A or ACK



    public BeatsResponse(final int seqNumber) {
        this.seqNumber = seqNumber;
    }

    /**
     * Creates a BeatsFrame where the data portion will contain this response.
     *
     *
     * @return a BeatsFrame for for this response
     */
    public BeatsFrame toFrame() {

        return new BeatsFrame.Builder()
                .version(version)
                .frameType(frameType)
                .payload(ByteBuffer.allocate(4).putInt(seqNumber).array())
                .build();
    }

    public static BeatsResponse ok(final int seqNumber) {
        return new BeatsResponse(seqNumber);
    }

}
