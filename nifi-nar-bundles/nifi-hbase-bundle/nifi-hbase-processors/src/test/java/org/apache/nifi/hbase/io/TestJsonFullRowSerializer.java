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
package org.apache.nifi.hbase.io;

import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.hbase.scan.ResultCell;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TestJsonFullRowSerializer {

    static final String ROW = "row1";

    static final String FAM1 = "colFam1";
    static final String QUAL1 = "colQual1";
    static final String VAL1 = "val1";
    static final long TS1 = 1111111111;

    static final String FAM2 = "colFam2";
    static final String QUAL2 = "colQual2";
    static final String VAL2 = "val2";
    static final long TS2 = 222222222;

    private final byte[] rowKey = ROW.getBytes(StandardCharsets.UTF_8);
    private ResultCell[] cells;

    @Before
    public void setup() {
        final byte[] cell1Fam = FAM1.getBytes(StandardCharsets.UTF_8);
        final byte[] cell1Qual = QUAL1.getBytes(StandardCharsets.UTF_8);
        final byte[] cell1Val = VAL1.getBytes(StandardCharsets.UTF_8);

        final byte[] cell2Fam = FAM2.getBytes(StandardCharsets.UTF_8);
        final byte[] cell2Qual = QUAL2.getBytes(StandardCharsets.UTF_8);
        final byte[] cell2Val = VAL2.getBytes(StandardCharsets.UTF_8);

        final ResultCell cell1 = getResultCell(cell1Fam, cell1Qual, cell1Val, TS1);
        final ResultCell cell2 = getResultCell(cell2Fam, cell2Qual, cell2Val, TS2);

        cells = new ResultCell[] { cell1, cell2 };
    }

    @Test
    public void testSerializeRegular() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final RowSerializer rowSerializer = new JsonFullRowSerializer(StandardCharsets.UTF_8, StandardCharsets.UTF_8);
        rowSerializer.serialize(rowKey, cells, out);

        final String json = out.toString(StandardCharsets.UTF_8.name());
        Assert.assertEquals("{\"row\":\"row1\", \"cells\": [" +
                "{\"fam\":\"" + FAM1 + "\",\"qual\":\"" + QUAL1 + "\",\"val\":\"" + VAL1 + "\",\"ts\":" + TS1 + "}, " +
                "{\"fam\":\"" + FAM2 + "\",\"qual\":\"" + QUAL2 + "\",\"val\":\"" + VAL2 + "\",\"ts\":" + TS2 + "}]}",
                json);
    }

    @Test
    public void testSerializeWithBase64() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final RowSerializer rowSerializer = new JsonFullRowSerializer(StandardCharsets.UTF_8, StandardCharsets.UTF_8, true);
        rowSerializer.serialize(rowKey, cells, out);

        final String rowBase64 = Base64.encodeBase64String(ROW.getBytes(StandardCharsets.UTF_8));

        final String fam1Base64 = Base64.encodeBase64String(FAM1.getBytes(StandardCharsets.UTF_8));
        final String qual1Base64 = Base64.encodeBase64String(QUAL1.getBytes(StandardCharsets.UTF_8));
        final String val1Base64 = Base64.encodeBase64String(VAL1.getBytes(StandardCharsets.UTF_8));

        final String fam2Base64 = Base64.encodeBase64String(FAM2.getBytes(StandardCharsets.UTF_8));
        final String qual2Base64 = Base64.encodeBase64String(QUAL2.getBytes(StandardCharsets.UTF_8));
        final String val2Base64 = Base64.encodeBase64String(VAL2.getBytes(StandardCharsets.UTF_8));

        final String json = out.toString(StandardCharsets.UTF_8.name());
        Assert.assertEquals("{\"row\":\"" + rowBase64 + "\", \"cells\": [" +
                "{\"fam\":\"" + fam1Base64 + "\",\"qual\":\"" + qual1Base64 + "\",\"val\":\"" + val1Base64 + "\",\"ts\":" + TS1 + "}, " +
                "{\"fam\":\"" + fam2Base64 + "\",\"qual\":\"" + qual2Base64 + "\",\"val\":\"" + val2Base64 + "\",\"ts\":" + TS2 + "}]}", json);
    }

    private ResultCell getResultCell(byte[] fam, byte[] qual, byte[] val, long timestamp) {
        final ResultCell cell = new ResultCell();

        cell.setFamilyArray(fam);
        cell.setFamilyOffset(0);
        cell.setFamilyLength((byte)fam.length);

        cell.setQualifierArray(qual);
        cell.setQualifierOffset(0);
        cell.setQualifierLength(qual.length);

        cell.setValueArray(val);
        cell.setValueOffset(0);
        cell.setValueLength(val.length);

        cell.setTimestamp(timestamp);

        return cell;
    }

}
