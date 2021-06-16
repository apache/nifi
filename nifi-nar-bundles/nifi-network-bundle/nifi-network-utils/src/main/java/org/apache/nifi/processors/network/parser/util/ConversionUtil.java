/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.network.parser.util;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public final class ConversionUtil {
    public static final String toIPV4(final byte[] buffer, final int offset, final int length) {
        try {
            return InetAddress.getByAddress(Arrays.copyOfRange(buffer, offset, offset + length)).getHostAddress();
        } catch (UnknownHostException e) {
            return String.valueOf(toInt(buffer, offset, length));
        }
    }

    public static final String toIPV6(final byte[] buffer, final int offset, final int length) {
        try {
            return InetAddress.getByAddress(Arrays.copyOfRange(buffer, offset, offset + length)).getHostAddress();
        } catch (UnknownHostException e) {
            return String.valueOf(toLong(buffer, offset, length));
        }
    }

    public static final BigInteger toBigInteger(final byte[] buffer, final int offset, final int length) {
        return new BigInteger(Arrays.copyOfRange(buffer, offset, offset + length));
    }

    public static final byte toByte(final byte[] buffer, final int offset) {
        return (byte) (buffer[offset] & 0xff);
    }

    public static final int toInt(final byte[] buffer, final int offset, final int length) {
        int ret = 0;
        final int done = offset + length;
        for (int i = offset; i < done; i++) {
            ret = ((ret << 8) & 0xffffffff) + (buffer[i] & 0xff);
        }
        return ret;
    }

    public static final long toLong(final byte[] buffer, final int offset, final int length) {
        long ret = 0;
        final int done = offset + length;
        for (int i = offset; i < done; i++) {
            ret = ret << 8;
            ret |= (buffer[i] & 0xFF);
        }
        return ret;
    }

    public static final short toShort(final byte[] buffer, final int offset, final int length) {
        short ret = 0;
        final int done = offset + length;
        for (int i = offset; i < done; i++) {
            ret = (short) (((ret << 8) & 0xffff) + (buffer[i] & 0xff));
        }
        return ret;
    }

    public static final String toString(final byte[] buffer, final int offset, final int length) {
        return new String(Arrays.copyOfRange(buffer, offset, offset + length));
    }

    public static byte[] toByteArray(final int i) {
        final byte[] result = new byte[2];
        result[0] = (byte) (i >> 8);
        result[1] = (byte) (i);
        return result;
    }
}
