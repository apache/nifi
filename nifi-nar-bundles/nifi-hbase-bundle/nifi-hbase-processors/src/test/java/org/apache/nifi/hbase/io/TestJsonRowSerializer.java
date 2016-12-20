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

import org.apache.nifi.hbase.scan.ResultCell;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TestJsonRowSerializer {

    private final byte[] rowKey = "row1".getBytes(StandardCharsets.UTF_8);
    private ResultCell[] cells;

    @Before
    public void setup() {
        final byte[] cell1Fam = "colFam1".getBytes(StandardCharsets.UTF_8);
        final byte[] cell1Qual = "colQual1".getBytes(StandardCharsets.UTF_8);
        final byte[] cell1Val = "val1".getBytes(StandardCharsets.UTF_8);

        final byte[] cell2Fam = "colFam2".getBytes(StandardCharsets.UTF_8);
        final byte[] cell2Qual = "colQual2".getBytes(StandardCharsets.UTF_8);
        final byte[] cell2Val = "val2".getBytes(StandardCharsets.UTF_8);

        final ResultCell cell1 = getResultCell(cell1Fam, cell1Qual, cell1Val);
        final ResultCell cell2 = getResultCell(cell2Fam, cell2Qual, cell2Val);

        cells = new ResultCell[] { cell1, cell2 };
    }

    @Test
    public void testSerializeRegular() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final RowSerializer rowSerializer = new JsonRowSerializer(StandardCharsets.UTF_8);
        rowSerializer.serialize(rowKey, cells, out);

        final String json = out.toString(StandardCharsets.UTF_8.name());
        Assert.assertEquals("{\"row\":\"row1\", \"cells\": {\"colFam1:colQual1\":\"val1\", \"colFam2:colQual2\":\"val2\"}}", json);
    }

    private ResultCell getResultCell(byte[] fam, byte[] qual, byte[] val) {
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

        return cell;
    }

}
