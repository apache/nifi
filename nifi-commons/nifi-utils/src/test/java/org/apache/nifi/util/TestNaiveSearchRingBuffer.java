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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestNaiveSearchRingBuffer {

    @Test
    public void testAddAndCompare() {
        final byte[] pattern = new byte[]{
            '\r', '0', 38, 48
        };

        final byte[] search = new byte[]{
            '\r', '0', 38, 58, 58, 83, 78, '\r', '0', 38, 48, 83, 92, 78, 4, 38
        };

        final NaiveSearchRingBuffer circ = new NaiveSearchRingBuffer(pattern);
        int counter = -1;
        for (final byte b : search) {
            counter++;
            final boolean matched = circ.addAndCompare(b);
            if (counter == 10) {
                assertTrue(matched);
            } else {
                assertFalse(matched);
            }
        }
    }

    @Test
    public void testGetOldestByte() {
        final byte[] pattern = new byte[]{
            '\r', '0', 38, 48
        };

        final byte[] search = new byte[]{
            '\r', '0', 38, 58, 58, 83, 78, (byte) 223, (byte) 227, (byte) 250, '\r', '0', 38, 48, 83, 92, 78, 4, 38
        };

        final NaiveSearchRingBuffer circ = new NaiveSearchRingBuffer(pattern);
        int counter = -1;
        for (final byte b : search) {
            counter++;
            final boolean matched = circ.addAndCompare(b);
            if (counter == 13) {
                assertTrue(matched);
            } else {
                assertFalse(matched);
            }
        }
    }

}
