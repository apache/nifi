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
package org.apache.nifi.processors.network.parser;

import java.util.OptionalInt;
import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toShort;
import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toInt;
import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toLong;
import static org.apache.nifi.processors.network.parser.util.ConversionUtil.toIPV4;

/**
 * Networkv5 is Cisco data export format which contains one header and one or more flow records. This Parser parses the netflowv5 format. More information: @see
 * <a href="https://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html">Netflowv5</a>
 */
public final class Netflowv5Parser {
    private static final int HEADER_SIZE = 24;
    private static final int RECORD_SIZE = 48;

    private static final int SHORT_TYPE = 0;
    private static final int INTEGER_TYPE = 1;
    private static final int LONG_TYPE = 2;
    private static final int IPV4_TYPE = 3;

    private static final String headerField[] = { "version", "count", "sys_uptime", "unix_secs", "unix_nsecs", "flow_sequence", "engine_type", "engine_id", "sampling_interval" };
    private static final String recordField[] = { "srcaddr", "dstaddr", "nexthop", "input", "output", "dPkts", "dOctets", "first", "last", "srcport", "dstport", "pad1", "tcp_flags", "prot", "tos",
            "src_as", "dst_as", "src_mask", "dst_mask", "pad2" };

    private final int portNumber;

    private Object headerData[];
    private Object recordData[][];

    public Netflowv5Parser(final OptionalInt portNumber) {
        this.portNumber = (portNumber.isPresent()) ? portNumber.getAsInt() : 0;
    }

    public final int parse(final byte[] buffer) throws Throwable {
        if( !isValid(buffer.length) ) {
            throw new Exception("Invalid Packet Length");
        }

        final int version = toInt(buffer, 0, 2);
        if( version != 5 ) {
            throw new Exception("Version mismatch");

        }
        final int count = toInt(buffer, 2, 2);

        headerData = new Object[headerField.length];
        headerData[0] = version;
        headerData[1] = count;
        headerData[2] = parseField(buffer, 4, 4, LONG_TYPE);
        headerData[3] = parseField(buffer, 8, 4, LONG_TYPE);
        headerData[4] = parseField(buffer, 12, 4, LONG_TYPE);
        headerData[5] = parseField(buffer, 16, 4, LONG_TYPE);
        headerData[6] = parseField(buffer, 20, 1, SHORT_TYPE);
        headerData[7] = parseField(buffer, 21, 1, SHORT_TYPE);
        headerData[8] = parseField(buffer, 22, 2, INTEGER_TYPE);

        int offset = 0;
        recordData = new Object[count][recordField.length];
        for (int counter = 0; counter < count; counter++) {
            offset = HEADER_SIZE + (counter * RECORD_SIZE);
            recordData[counter][0] = parseField(buffer, offset, 4, IPV4_TYPE);
            recordData[counter][1] = parseField(buffer, offset + 4, 4, IPV4_TYPE);
            recordData[counter][2] = parseField(buffer, offset + 8, 4, IPV4_TYPE);
            recordData[counter][3] = parseField(buffer, offset + 12, 2, INTEGER_TYPE);
            recordData[counter][4] = parseField(buffer, offset + 14, 2, INTEGER_TYPE);
            recordData[counter][5] = parseField(buffer, offset + 16, 4, LONG_TYPE);
            recordData[counter][6] = parseField(buffer, offset + 20, 4, LONG_TYPE);
            recordData[counter][7] = parseField(buffer, offset + 24, 4, LONG_TYPE);
            recordData[counter][8] = parseField(buffer, offset + 28, 4, LONG_TYPE);
            recordData[counter][9] = parseField(buffer, offset + 32, 2, INTEGER_TYPE);
            recordData[counter][10] = parseField(buffer, offset + 34, 2, INTEGER_TYPE);
            recordData[counter][11] = parseField(buffer, offset + 36, 1, SHORT_TYPE);
            recordData[counter][12] = parseField(buffer, offset + 37, 1, SHORT_TYPE);
            recordData[counter][13] = parseField(buffer, offset + 38, 1, SHORT_TYPE);
            recordData[counter][14] = parseField(buffer, offset + 39, 1, SHORT_TYPE);
            recordData[counter][15] = parseField(buffer, offset + 40, 2, INTEGER_TYPE);
            recordData[counter][16] = parseField(buffer, offset + 42, 2, INTEGER_TYPE);
            recordData[counter][17] = parseField(buffer, offset + 44, 1, SHORT_TYPE);
            recordData[counter][18] = parseField(buffer, offset + 45, 1, SHORT_TYPE);
            recordData[counter][19] = parseField(buffer, offset + 46, 2, INTEGER_TYPE);
        }
        return count;
    }

    private final Object parseField(final byte[] buffer, final int startOffset, final int length, final int type) {
        Object value = null;
        switch (type) {
            case SHORT_TYPE:
                value = toShort(buffer, startOffset, length);
                break;
            case INTEGER_TYPE:
                value = toInt(buffer, startOffset, length);
                break;
            case LONG_TYPE:
                value = toLong(buffer, startOffset, length);
                break;
            case IPV4_TYPE:
                value = toIPV4(buffer, startOffset, length);
                break;
            default:
                break;
        }
        return value;
    }

    private boolean isValid(final int length) {
        final int minPacketSize = HEADER_SIZE; // Only HEADER available
        return length >= minPacketSize && length <=  65536;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public static String[] getHeaderFields() {
        return headerField;
    }

    public static String[] getRecordFields() {
        return recordField;
    }

    public Object[] getHeaderData() {
        return headerData;
    }

    public Object[][] getRecordData() {
        return recordData;
    }
}
