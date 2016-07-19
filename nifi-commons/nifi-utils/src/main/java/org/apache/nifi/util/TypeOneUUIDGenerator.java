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
package org.apache.nifi.util;

import java.util.Random;
import java.util.UUID;

public class TypeOneUUIDGenerator {

    public static final Object lock = new Object();

    private static long lastTime;
    private static long clockSequence = 0;
    private static final Random randomGenerator = new Random();

    /**
     * Will generate unique time based UUID where the next UUID is always
     * greater then the previous.
     */
    public final static UUID generateId() {
        return generateId(System.currentTimeMillis());
    }

    /**
     *
     */
    public final static UUID generateId(long currentTime) {
        return generateId(currentTime, Math.abs(randomGenerator.nextInt()));
    }

    /**
     *
     */
    public final static UUID generateId(long currentTime, int lsbInt) {
        long time;

        synchronized (lock) {
            if (currentTime == lastTime) {
                ++clockSequence;
            } else {
                lastTime = currentTime;
                clockSequence = 0;
            }
        }

        time = currentTime;

        // low Time
        time = currentTime << 32;

        // mid Time
        time |= ((currentTime & 0xFFFF00000000L) >> 16);

        // hi Time
        time |= 0x1000 | ((currentTime >> 48) & 0x0FFF);

        long clockSequenceHi = clockSequence;
        clockSequenceHi <<= 48;
        long lsb = clockSequenceHi | lsbInt;
        return new UUID(time, lsb);
    }
}