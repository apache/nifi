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

import org.apache.nifi.cdc.event.TableEventInfo;
import org.apache.nifi.cdc.event.TableInfo;


/**
 * An event class corresponding to Data Definition Language (DDL) events, such as schema changes (add/drop column, add/drop table, etc.) and others (truncate table, e.g.)
 */
public class DDLEventInfo extends BaseBinlogTableEventInfo implements TableEventInfo {

    private String query;

    public DDLEventInfo(TableInfo tableInfo, Long timestamp, String binlogFilename, long binlogPosition, String query) {
        super(tableInfo, DDL_EVENT, timestamp, binlogFilename, binlogPosition);
        this.query = query;
    }

    public DDLEventInfo(TableInfo tableInfo, Long timestamp, String binlogGtidSet, String query) {
        super(tableInfo, DDL_EVENT, timestamp, binlogGtidSet);
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}
