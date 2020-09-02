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
package org.apache.nifi.hazelcast.services.util;

import org.junit.Assert;
import org.junit.Test;

public class LongUtilTest {

    @Test
    public void testLongToBytes() {
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}, LongUtil.toBytesWithPadding(0));
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 1}, LongUtil.toBytesWithPadding(1));
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 2, 0}, LongUtil.toBytesWithPadding(512));
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 2, 0}, LongUtil.toBytesWithPadding(512));
        Assert.assertArrayEquals(new byte[]{127, -1, -1, -1, -1, -1, -1, -1}, LongUtil.toBytesWithPadding(Long.MAX_VALUE));
        Assert.assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1}, LongUtil.toBytesWithPadding(-1));
        Assert.assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -2, 0}, LongUtil.toBytesWithPadding(-512));
        Assert.assertArrayEquals(new byte[]{-128, 0, 0, 0, 0, 0, 0, 0}, LongUtil.toBytesWithPadding(Long.MIN_VALUE));
    }

    @Test
    public void testBytesToLong() {
        Assert.assertEquals(0, LongUtil.fromPaddedBytes(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}));
        Assert.assertEquals(1, LongUtil.fromPaddedBytes(new byte[]{0, 0, 0, 0, 0, 0, 0, 1}));
        Assert.assertEquals(512, LongUtil.fromPaddedBytes(new byte[]{0, 0, 0, 0, 0, 0, 2, 0}));
        Assert.assertEquals(Long.MAX_VALUE, LongUtil.fromPaddedBytes(new byte[]{127, -1, -1, -1, -1, -1, -1, -1}));
        Assert.assertEquals(-1, LongUtil.fromPaddedBytes(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1}));
        Assert.assertEquals(-512, LongUtil.fromPaddedBytes(new byte[]{-1, -1, -1, -1, -1, -1, -2, 0}));
        Assert.assertEquals(Long.MIN_VALUE, LongUtil.fromPaddedBytes(new byte[]{-128, 0, 0, 0, 0, 0, 0, 0}));
    }

    @Test
    public void testConvertFromAndToLong() {
        final long[] values = new long[] {0, 1, 512, Long.MAX_VALUE, -1, -512, Long.MIN_VALUE};

        for (final long value : values) {
            Assert.assertEquals(value, LongUtil.fromPaddedBytes(LongUtil.toBytesWithPadding(value)));
        }
    }

    @Test
    public void testTooLongInput() {
        Assert.assertEquals(512, LongUtil.fromPaddedBytes(new byte[]{0, 0, 0, 0, 0, 0, 2, 0, 1, 2, 3, 4, 5}));
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void tooTooShortInput() {
        LongUtil.fromPaddedBytes(new byte[]{0, 0, 0, 0});
    }
}