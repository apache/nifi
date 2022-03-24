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
package org.apache.nifi.cdc.mysql.event;

import org.apache.nifi.cdc.event.BaseRowEventInfo;
import org.apache.nifi.cdc.event.RowEventInfo;
import org.apache.nifi.cdc.event.TableInfo;

import java.util.BitSet;
import java.util.List;

/**
 * A base class to help store information about a row mutation event (UPDATE, DELETE, etc.)
 */
public class BaseBinlogRowEventInfo<RowEventDataType> extends BaseBinlogTableEventInfo implements RowEventInfo<RowEventDataType> {

    private BitSet includedColumns;
    private RowEventInfo<RowEventDataType> delegate;

    public BaseBinlogRowEventInfo(TableInfo tableInfo, String type, Long timestamp, String binlogFilename, Long binlogPosition, BitSet includedColumns, List<RowEventDataType> rows) {
        super(tableInfo, type, timestamp, binlogFilename, binlogPosition);
        this.includedColumns = includedColumns;
        this.delegate = new BaseRowEventInfo<>(tableInfo, type, timestamp, rows);
    }

    public BaseBinlogRowEventInfo(TableInfo tableInfo, String type, Long timestamp, String binlogGtidSet, BitSet includedColumns, List<RowEventDataType> rows) {
        super(tableInfo, type, timestamp, binlogGtidSet);
        this.includedColumns = includedColumns;
        this.delegate = new BaseRowEventInfo<>(tableInfo, type, timestamp, rows);
    }

    public BitSet getIncludedColumns() {
        return includedColumns;
    }

    @Override
    public List<RowEventDataType> getRows() {
        return delegate.getRows();
    }
}
