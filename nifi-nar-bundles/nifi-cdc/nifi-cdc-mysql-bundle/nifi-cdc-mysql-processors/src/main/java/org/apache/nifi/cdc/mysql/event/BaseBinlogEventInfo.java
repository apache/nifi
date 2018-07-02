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

import org.apache.nifi.cdc.event.BaseEventInfo;

/**
 * A base class for all MYSQL binlog events
 */
public class BaseBinlogEventInfo extends BaseEventInfo implements BinlogEventInfo {

    private String binlogFilename;
    private Long binlogPosition;
    private String binlogGtidSet;

    public BaseBinlogEventInfo(String eventType, Long timestamp, String binlogFilename, Long binlogPosition) {
        super(eventType, timestamp);
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
    }

    public BaseBinlogEventInfo(String eventType, Long timestamp, String binlogGtidSet) {
        super(eventType, timestamp);
        this.binlogGtidSet = binlogGtidSet;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public Long getBinlogPosition() {
        return binlogPosition;
    }

    public String getBinlogGtidSet() {
        return binlogGtidSet;
    }
}
