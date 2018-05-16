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

import org.apache.nifi.cdc.event.BaseTableEventInfo;
import org.apache.nifi.cdc.event.ColumnDefinition;
import org.apache.nifi.cdc.event.TableEventInfo;
import org.apache.nifi.cdc.event.TableInfo;

import java.util.List;

/**
 * A base class to handle data common to binlog table events, such as database name, table name, etc.
 */
public class BaseBinlogTableEventInfo extends BaseBinlogEventInfo implements BinlogTableEventInfo {

    private TableEventInfo delegate;

    public BaseBinlogTableEventInfo(TableInfo tableInfo, String eventType, Long timestamp, String binlogFilename, Long binlogPosition) {
        super(eventType, timestamp, binlogFilename, binlogPosition);
        this.delegate = new BaseTableEventInfo(tableInfo, DDL_EVENT, timestamp);
    }

    @Override
    public String getDatabaseName() {
        return delegate.getDatabaseName();
    }

    @Override
    public String getTableName() {
        return delegate.getTableName();
    }

    @Override
    public Long getTableId() {
        return delegate.getTableId();
    }

    @Override
    public List<ColumnDefinition> getColumns() {
        return delegate.getColumns();
    }

    @Override
    public ColumnDefinition getColumnByIndex(int i) {
        return delegate.getColumnByIndex(i);
    }
}
