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
package org.apache.nifi.processors.network;

import java.util.OptionalInt;

import org.apache.nifi.processors.network.parser.Netflowv5Parser;
import org.junit.Assert;
import org.junit.Test;

public class TestNetflowv5Parser {
    private static final byte sample1[] = {
            // Header
            0, 5, 0, 1, 4, -48, 19, 36, 88, 71, -44, 73, 0, 0, 0, 0, 0, 0, 17, -22, 0, 0, 0, 0,
            // Record 1
            10, 0, 0, 2, 10, 0, 0, 3, 0, 0, 0, 0, 0, 3, 0, 5, 0, 0, 0, 1, 0, 0, 0, 64, 4, -49, 40, -60, 4, -48, 19, 36, 16, -110, 0, 80, 0, 0, 17, 1, 0, 2, 0, 3, 32, 31, 0, 0 };
    private static final int sample1RecordCount = 1;
    private static final byte sample2[] = {
            // Header
            0, 5, 0, 3, 4, -48, 19, 36, 88, 71, -44, 73, 0, 0, 0, 0, 0, 0, 17, -22, 0, 0, 0, 0,
            // Record 1
            10, 0, 0, 2, 10, 0, 0, 3, 0, 0, 0, 0, 0, 3, 0, 5, 0, 0, 0, 1, 0, 0, 0, 64, 4, -49, 40, -60, 4, -48, 19, 36, 16, -110, 0, 80, 0, 0, 17, 1, 0, 2, 0, 3, 32, 31, 0, 0,
            // Record 2
            10, 0, 0, 2, 10, 0, 0, 3, 0, 0, 0, 0, 0, 3, 0, 5, 0, 0, 0, 1, 0, 0, 0, 64, 4, -49, 40, -60, 4, -48, 19, 36, 16, -110, 0, 80, 0, 0, 17, 1, 0, 2, 0, 3, 32, 31, 0, 1,
            // Record 3
            10, 0, 0, 2, 10, 0, 0, 3, 0, 0, 0, 0, 0, 3, 0, 5, 0, 0, 0, 1, 0, 0, 0, 64, 4, -49, 40, -60, 4, -48, 19, 36, 16, -110, 0, 80, 0, 0, 17, 1, 0, 2, 0, 3, 32, 31, 0, 2 };
    private static final int sample2RecordCount = 3;
    private static final byte invalidVersion[] = {
            // Header
            0, 9, 0, 1, 4, -48, 19, 36, 88, 71, -44, 73, 0, 0, 0, 0, 0, 0, 17, -22, 0, 0, 0, 0,
            // Record 1
            10, 0, 0, 2, 10, 0, 0, 3, 0, 0, 0, 0, 0, 3, 0, 5, 0, 0, 0, 1, 0, 0, 0, 64, 4, -49, 40, -60, 4, -48, 19, 36, 16, -110, 0, 80, 0, 0, 17, 1, 0, 2, 0, 3, 32, 31, 0, 0 };

    @Test
    public void testParsingSingleRecord() throws Throwable {
        final Netflowv5Parser parser = new Netflowv5Parser(OptionalInt.empty());
        int parsedRecords = parser.parse(sample1);
        Assert.assertEquals(sample1RecordCount, parsedRecords);
        final Object[][] data = parser.getRecordData();
        Assert.assertEquals(Long.valueOf(1), data[0][5]);
        Assert.assertEquals(Long.valueOf(64), data[0][6]);
    }

    @Test
    public void testParsingMultipleRecords() throws Throwable {
        final Netflowv5Parser parser = new Netflowv5Parser(OptionalInt.empty());
        int parsedRecords = parser.parse(sample2);
        Assert.assertEquals(sample2RecordCount, parsedRecords);
        final Object[][] data = parser.getRecordData();
        for (int i = 0; i < 3; i++) {
            Assert.assertEquals("10.0.0.2", data[i][0]);
            Assert.assertEquals("10.0.0.3", data[i][1]);
            Assert.assertEquals("0.0.0.0", data[i][2]);
            Assert.assertEquals(3, data[i][3]);
            Assert.assertEquals(i, data[i][19]);
        }
    }

    @Test
    public void testInvalidData() {
        final Netflowv5Parser parser = new Netflowv5Parser(OptionalInt.empty());
        try {
            parser.parse("invalid data".getBytes());
        } catch (Throwable e) {
            Assert.assertTrue(e.getMessage().contains("Invalid Packet Length"));
        }
    }

    @Test
    public void testInvalidVersion() {
        final Netflowv5Parser parser = new Netflowv5Parser(OptionalInt.empty());
        try {
            parser.parse(invalidVersion);
        } catch (Throwable e) {
            Assert.assertTrue(e.getMessage().contains("Version mismatch"));
        }
    }
}
