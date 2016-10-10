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
package org.apache.nifi.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockHBaseClientService extends AbstractControllerService implements HBaseClientService {

    private Map<String,ResultCell[]> results = new HashMap<>();
    private Map<String, List<PutFlowFile>> flowFilePuts = new HashMap<>();
    private boolean throwException = false;

    @Override
    public void put(String tableName, Collection<PutFlowFile> puts) throws IOException {
        if (throwException) {
            throw new IOException("exception");
        }

        this.flowFilePuts.put(tableName, new ArrayList<>(puts));
    }

    @Override
    public void put(String tableName, byte[] rowId, Collection<PutColumn> columns) throws IOException {
       throw new UnsupportedOperationException();
    }

    @Override
    public void scan(String tableName, Collection<Column> columns, String filterExpression, long minTime, ResultHandler handler) throws IOException {
        if (throwException) {
            throw new IOException("exception");
        }

        // pass all the staged data to the handler
        for (final Map.Entry<String,ResultCell[]> entry : results.entrySet()) {
            handler.handle(entry.getKey().getBytes(StandardCharsets.UTF_8), entry.getValue());
        }
    }

    public void addResult(final String rowKey, final Map<String, String> cells, final long timestamp) {
        final byte[] rowArray = rowKey.getBytes(StandardCharsets.UTF_8);

        final ResultCell[] cellArray = new ResultCell[cells.size()];
        int i = 0;
        for (final Map.Entry<String, String> cellEntry : cells.entrySet()) {
            final ResultCell cell = new ResultCell();
            cell.setRowArray(rowArray);
            cell.setRowOffset(0);
            cell.setRowLength((short) rowArray.length);

            final String cellValue = cellEntry.getValue();
            final byte[] valueArray = cellValue.getBytes(StandardCharsets.UTF_8);
            cell.setValueArray(valueArray);
            cell.setValueOffset(0);
            cell.setValueLength(valueArray.length);

            final byte[] familyArray = "nifi".getBytes(StandardCharsets.UTF_8);
            cell.setFamilyArray(familyArray);
            cell.setFamilyOffset(0);
            cell.setFamilyLength((byte) familyArray.length);

            final String qualifier = cellEntry.getKey();
            final byte[] qualifierArray = qualifier.getBytes(StandardCharsets.UTF_8);
            cell.setQualifierArray(qualifierArray);
            cell.setQualifierOffset(0);
            cell.setQualifierLength(qualifierArray.length);

            cell.setTimestamp(timestamp);
            cellArray[i++] = cell;
        }

        results.put(rowKey, cellArray);
    }

    public Map<String, List<PutFlowFile>> getFlowFilePuts() {
        return flowFilePuts;
    }

    public void setThrowException(boolean throwException) {
        this.throwException = throwException;
    }

    @Override
    public byte[] toBytes(final boolean b) {
        return new byte[] { b ? (byte) -1 : (byte) 0 };
    }

    @Override
    public byte[] toBytes(long l) {
        byte [] b = new byte[8];
        for (int i = 7; i > 0; i--) {
          b[i] = (byte) l;
          l >>>= 8;
        }
        b[0] = (byte) l;
        return b;
    }

    @Override
    public byte[] toBytes(final double d) {
        return toBytes(Double.doubleToRawLongBits(d));
    }

    @Override
    public byte[] toBytes(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] toBytesBinary(String s) {
       return Bytes.toBytesBinary(s);
    }
}
