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
package org.apache.nifi.cdc.mysql;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class EventUtils {
    private EventUtils() { }
    public static Event buildEvent(final EventHeader header) {
        return buildEvent(header, new EventData() { });
    }

    public static Event buildEvent(final EventHeader header, final EventData data) {
        return new Event(header, data);
    }

    public static EventHeaderV4 buildEventHeaderV4(final EventType eventType, final long nextPosition) {
        final EventHeaderV4 eventHeaderV4 = new EventHeaderV4();
        eventHeaderV4.setTimestamp(System.currentTimeMillis());
        eventHeaderV4.setEventType(eventType);
        eventHeaderV4.setNextPosition(nextPosition);

        return eventHeaderV4;
    }

    public static RotateEventData buildRotateEventData(final String binlogFilename, final long binlogPosition) {
        final RotateEventData rotateEventData = new RotateEventData();
        rotateEventData.setBinlogFilename(binlogFilename);
        rotateEventData.setBinlogPosition(binlogPosition);

        return rotateEventData;
    }

    public static QueryEventData buildQueryEventData(final String database, final String sql) {
        final QueryEventData queryEventData = new QueryEventData();
        queryEventData.setDatabase(database);
        queryEventData.setSql(sql);

        return queryEventData;
    }

    public static TableMapEventData buildTableMapEventData(final long tableId, final String database,
                                                           final String table, final byte[] columnTypes) {
        final TableMapEventData tableMapEventData = new TableMapEventData();
        tableMapEventData.setTableId(tableId);
        tableMapEventData.setDatabase(database);
        tableMapEventData.setTable(table);
        tableMapEventData.setColumnTypes(columnTypes);

        return tableMapEventData;
    }

    public static WriteRowsEventData buildWriteRowsEventData(final long tableId, final BitSet includedColumns, final List<Serializable[]> rows) {
        final WriteRowsEventData writeRowsEventData = new WriteRowsEventData();
        writeRowsEventData.setTableId(tableId);
        writeRowsEventData.setIncludedColumns(includedColumns);
        writeRowsEventData.setRows(rows);

        return writeRowsEventData;
    }

    public static UpdateRowsEventData buildUpdateRowsEventData(final long tableId, final BitSet includedColumnsBeforeUpdate,
                                                               final BitSet includedColumns, final List<Map.Entry<Serializable[], Serializable[]>> rows) {
        final UpdateRowsEventData updateRowsEventData = new UpdateRowsEventData();
        updateRowsEventData.setTableId(tableId);
        updateRowsEventData.setIncludedColumnsBeforeUpdate(includedColumnsBeforeUpdate);
        updateRowsEventData.setIncludedColumns(includedColumns);
        updateRowsEventData.setRows(rows);

        return updateRowsEventData;
    }

    public static DeleteRowsEventData buildDeleteRowsEventData(final long tableId, final BitSet includedColumns, final List<Serializable[]> rows) {
        final DeleteRowsEventData deleteRowsEventData = new DeleteRowsEventData();
        deleteRowsEventData.setTableId(tableId);
        deleteRowsEventData.setIncludedColumns(includedColumns);
        deleteRowsEventData.setRows(rows);

        return deleteRowsEventData;
    }

    @SuppressWarnings("deprecation")
    public static GtidEventData buildGtidEventData(final String sourceId, final String transactionId) {
        final GtidEventData gtidEventData = new GtidEventData();
        gtidEventData.setGtid(buildGtid(sourceId, transactionId));

        return gtidEventData;
    }

    public static String buildGtid(final String sourceId, final String... singleOrTransactionRanges) {
        return sourceId + ":" + String.join(":", singleOrTransactionRanges);
    }
}
