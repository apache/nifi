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
package org.apache.nifi.processors.lumberjack.response;

import org.apache.nifi.processors.lumberjack.frame.LumberjackFrame;

import java.nio.ByteBuffer;

/**
 'ack' frame type

 SENT FROM READER ONLY
 frame type value: ASCII 'A' aka byte value 0x41

 Payload:
 32bit unsigned sequence number.

 */
public class LumberjackResponse {
    private final long seqNumber;
    final private byte version = 0x31, frameType = 0x41;



    public LumberjackResponse(final long seqNumber) {
        this.seqNumber = seqNumber;
    }

    /**
     * Creates a LumberjackFrame where the data portion will contain this response.
     *
     *
     * @return a LumberjackFrame for for this response
     */
    public LumberjackFrame toFrame() {

        return new LumberjackFrame.Builder()
                .version(version)
                .frameType(frameType)
                .payload(ByteBuffer.allocate(8).putLong(seqNumber).array())
                .build();
    }

    public static LumberjackResponse ok(final long seqNumber) {
        return new LumberjackResponse(seqNumber);
    }

}
